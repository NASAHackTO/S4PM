#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_split_services.pl - break the initial order up into separate orders based on which service is being requested

The current services configured are:
Service name:
GdMODL1B:	Runs the MODIS Level-1B Subsetter
GdMODOCL2:      Runs the MODIS Ocean's Level-2 subsetter
GdAIRSL1B:	Runs the AIRS Level-1B subsetter

=head1 SYNOPSIS

B<s4pm_split_services.pl>

=head1 DESCRIPTION

The Split Services station generates work orders for the following stations
	1. Request Data Station
     		DO.REQUEST_DATA_ServiceName.orderId_requestPartPnumber.wo
	2. Granule Find Station
     		DO.FIND_ServiceName.rderId_requestPartNumber.wo
	3. Track Request
		DO.TRACK_REQUEST.orderId.wo
	4. Track Data Station
		DO.EXPECT. ordered.wo
	5. PSPEC File
		DO.PSPEC_ServiceName.orderId_requestPartNumber.wo

PDL 

Get ordereid from ODL request 
Read the station specific S4P configuration file.
Read the Allocate Disk configuration file.
Read input work order file  
Set RequestPartNumber to 0;
IF LINE_ITEM_SET Exist 
  FOREACH ITEM_LINE_SET
    SET service name
    write_pspec_wo ( )
    FOREACH LINE_ITEM
      compose_file_group()
      Check if it is DFA
        IF it is DFA
        IF Get replacement granule successful
	Update ODL with replacement granule
	write_request_file_group
	write_moredata_file_group
    ENDFOREACH LINE_ITEM
    write_request__wo
    write_moredata_wo	
  ENDFOREACH  ITEM_LINE_SET
  Increment RequestPartNumber        
ENDIF
IF LINE_ITEM Exist
  FOREACH ITEM_LINE
    SET service name
    write_pspec_wo ( )
    compose_file_group()
    Check if it is DFA
      IF it is DFA
      IF Get replacement granule successful
      Update ODL with replacement granule
      write_request_file_group
      write_moredata_file_group
      write_request__wo
      write_moredata_wo
  ENDFOREACH  ITEM_LINE
ENDIF

write_track_request_wo
write_expect_wo

if the request dbID could not found 

exit 0

=head1 AUTHOR

Yangling Huang

=cut

################################################################################
# s4pm_split_services.pl,v 1.4 2007/11/02 14:47:12 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Getopt::Long;
use Fcntl;
use File::Copy;
use S4P::FileGroup;
use S4P::FileSpec;
use S4P::TimeTools;
use S4PM::Algorithm;
use S4PM;
use S4PM::S4PMOD;
use S4PM::GdSDSRVDbi;
use S4PM::GdDbi;
use S4P::PDR;
use S4P;
use S4P::OdlTree;
use Safe;
use strict;

################################################################################
# Global variables                                                             
################################################################################

use vars qw(
	    $order_id
	    $request_part_number
	    $update_item
	    @bad_granules
	    %expect
	    $dbh
	    $odl_string
	   );

# Get the command line arguments

my $arg = {};
GetOptions( $arg, "f=s");

$arg->{w} = $ARGV[0];

# Parse workorder name to obtain order_ID

$order_id = (split /\./, $arg->{w} )[2] ;
  
# Exit if failed to get order ID and job type.

unless ( defined $order_id  ) {
  S4P::perish( 1, "main: Failed to obtain order ID from work order name $arg->{w}." );
}
  
# Read the station specific S4P configuration file.

my $compartment1 = new Safe 'CFG1';
$compartment1->share('$ur_srvr');
$compartment1->rdo($arg->{'f'}) or 
    S4P::perish(30, "main: Failed to read in configuration file " . $arg->{'f'} . " in safe mode: $!");

# Read the Allocate Disk configuration file.

my $compartment2 = new Safe 'CFG2';
$compartment2->share('%datatype_pool_map', '%datatype_pool', '%proxy_esdt_map');
$compartment2->rdo("../s4pm_allocate_disk.cfg") or 
    S4P::perish(30, "main: Failed to read in configuration file ../s4pm_allocate_disk.cfg in safe mode: $!");

my ( @track, @granules, %expect );

$request_part_number = 0;  # Correspond to each requested LINE_ITEM or LINE_ITEM_SET
$update_item = 0;          # Assume there is no item need to be updated
@bad_granules = ();        # Assum there is no bad granule

$dbh = S4PM::GdDbi::db_connect( );

# Reading an ODL file

my $odl = S4P::OdlTree->new( FILE => $arg->{w} );

if (  my @item_sets = $odl->search( NAME => 'LINE_ITEM_SET' ) ) {
    
    foreach my $set (@item_sets) {

        my $service_name; # Correspond to each service invocation

        $request_part_number++;
  
        foreach ( $set->search( NAME => 'SPECIALIZED_CRITERIA',
                        CRITERIA_NAME => '"SERVICE"' ) )  {
	    $service_name = $_->getAttribute( 'CRITERIA_VALUE' );
	    $service_name =~ s/"//g;

########### Take out spaces and underscores to avoid problems in parsing 
########### specify_data cfg file names

	    $service_name =~ s/[ _]//g;  
            S4P::logger('INFO', "main: Service Name: '$service_name'");

            unless ( $service_name ) {
                S4P::perish(1, "main: There is no service name defined in the $ARGV[0] for request $request_part_number");
            }

        }

        my $pspec_file = 'PSPEC_'.$service_name.'.'. $order_id.'_'.$request_part_number;

        my $request_data_wo = 'REQUEST_DATA.'.$order_id.'_'.$request_part_number.'.wo';

        my $moredata_wo =  'FIND_'.$service_name.'.'.$order_id.'_'.$request_part_number.'.wo';

        my @item_lists = $set->search( NAME => 'LINE_ITEM' );
	
        my ( @request_file_groups, 
            @file_groups, # An array of hash in which key is local granule id, value is its ur
            $bad_item,
	    $start_time, 
     	    $stop_time
        );

        undef $start_time;
        undef $stop_time;
        undef $bad_item;

        push @file_groups, { $pspec_file => "INSERT_UR_HERE" }; 

        foreach my $item ( @item_lists ) {
            my ( $item_ur, $item_granule, $begin_time, $end_time ) = compose_file_group($item);
          
            unless ( $item_ur && $begin_time && $end_time ) {
                $bad_item = $item_granule;
                $update_item = 1;
                last;
            }

            push @file_groups, { $item_granule => $item_ur };

            unless ( $start_time && $stop_time ) {
              $start_time = $begin_time;
              $stop_time = $end_time;
            } 

	    my $request_file_group = write_request_file_group( $item_ur, $item_granule );

            push @request_file_groups, $request_file_group;
         
            push @track, $request_part_number.': '.$item_granule.' '.$item_ur;
	   
   	    if (! $expect{$item_granule} ) {
	        $expect{$item_granule} = 1;            
    	    } else {
	        $expect{$item_granule}++;
            }	     
        }      
      
        if ( $bad_item ) {
      
            S4P::logger ( 'INFO', "main: No $request_part_number request contains bad granule $bad_item. Delete all granules of No $request_part_number request from input work order");

########### Delete bad item set from odl group 
 
            unless ( delete_data_item_set( $set ) ) {
                S4P::perish ( 1, "main: Failed to Delete LINE_ITEM_SET which contain Bad Granule $bad_item From Input");
            }
            next;
        }

        unless ( write_request_wo (\@request_file_groups, $request_data_wo, 1 ) ) { 
            S4P::perish(1, "main: Failed to Create $request_data_wo Work order.");
        }
          
        my ( $moredata_file_groups, $post_off_set, $pre_off_set ) = write_moredata_file_groups(
            \@file_groups, $service_name, $pspec_file,
            $CFG2::datatype_pool{$CFG2::datatype_pool_map{PSPEC}}
        );

        unless ( write_moredata_wo ( $moredata_file_groups, $moredata_wo, $start_time, $stop_time, $post_off_set, $pre_off_set ) ) {
	    S4P::perish(1, "main: Failed to Create  $moredata_wo Work Order.");
        }

        unless ( write_pspec_wo ( $pspec_file, $set, $CFG2::datatype_pool{$CFG2::datatype_pool_map{PSPEC}} ) ) {
            S4P::perish(1, "main: Failed to Create  $pspec_file Work Order.");
        }
    }
} 

if ( my @item_lists = $odl->search( NAME => 'LINE_ITEM' ) ) {
  
    foreach my $item ( @item_lists ) {
         
        my  $service_name ;
      
        foreach ( $item->search( NAME => 'SPECIALIZED_CRITERIA', CRITERIA_NAME => '"SERVICE"' ) ) {
            $service_name = $_->getAttribute( 'CRITERIA_VALUE' );

            # EDG has SERVICE line-wrapped

            ($service_name) = odl_cleanup($service_name);

            # Take out spaces and underscores to avoid problems in parsing 
            # specify_data cfg file names

            $service_name =~ s/[ _]//g;  
            S4P::logger('INFO',  "main: Service Name: '$service_name'");

	    unless ( $service_name ) {
	        $request_part_number++;
                    S4P::perish(1, "main: There is no service name defined in the $ARGV[0] for request $request_part_number");
            }
        }

        if ( $service_name ) {
	
	$request_part_number++;
	
        my $pspec_file = 'PSPEC_'.$service_name.'.'. $order_id.'_'.$request_part_number;

	my $request_data_wo = 'REQUEST_DATA'.'.'.$order_id.'_'.$request_part_number.'.wo';

	my $moredata_wo =  'FIND_'.$service_name.'.'.$order_id.'_'.$request_part_number.'.wo';

	my ( $item_ur, $item_granule, $start_time, $stop_time ) = compose_file_group($item);
        

	unless ( $item_ur && $start_time && $stop_time ) {

          S4P::logger ( 'INFO', "main: No $request_part_number request contains bad granule $item_granule");

          # Delete bad item from odl group
          unless ( delete_data_item( $odl,  $item_granule ) ) {

            S4P::perish ( 1, "main: Failed to Delete Bad Granule $item_granule From Input");
          }
         
          $update_item = 1;
          next;
        }

	my $request_file_group = write_request_file_group( $item_ur, $item_granule );

	my ( $moredata_file_group, $post_offset, $pre_offset ) = write_moredata_file_group( 
	     			  $item_ur, $item_granule, $service_name, $pspec_file, 
				  $CFG2::datatype_pool{$CFG2::datatype_pool_map{PSPEC}} );

	push @track, $request_part_number.': '.$item_granule.' '.$item_ur;
	
	unless ( write_request_wo ($request_file_group, $request_data_wo ) ) { 
	  S4P::perish(1, "main: Failed to Create $request_data_wo work order.");
        }

        unless ( write_moredata_wo ( $moredata_file_group, $moredata_wo, $start_time, $stop_time, 
				     $post_offset, $pre_offset ) ) {
	  S4P::perish(1, "main: Failed to Create  $moredata_wo work order.");
	}

        unless ( write_pspec_wo ( $pspec_file, $item, 
                                  $CFG2::datatype_pool{$CFG2::datatype_pool_map{PSPEC}} ) ) {
	  S4P::perish(1, "main: Failed to Create  $pspec_file Work Order.");
	}

 	if (! $expect{$item_granule} ) {
	    $expect{$item_granule} = 1;
    	} else {
	    $expect{$item_granule}++;
        }
      }
    }
}

# Create failure work order to track request station if any bad granule
if ( @bad_granules ) {
  unless ( write_failure_wo( $arg->{w} ) ) {
    S4P::perish ( 1, "main: Failed to Write Failure Work Order" );
  }
}

# Update input work order with new odl group
if ( $update_item ) {
  unless ( update_input_wo( $odl, $arg->{w} ) ) {
    S4P::perish( 1, "main: Failed to update input work order $arg->{w}" );
  }
}
 
# create track_request work order 
if ( @track ) {
  unless ( write_track_request_wo( $odl, \@track ) ) {
    S4P::perish( 1, "main: Failed to write track request work order" );
  }
} else {
  S4P::perish( 1, "main: There is no granule to be tracked......" ) 
	unless ( @bad_granules );
}

# create expect work order 
if ( %expect ) {
  unless ( write_expect_wo( \%expect ) ) {
    S4P::perish( 1, "main: Failed to write expect work order" );
  }
}

S4P::logger ( 'INFO', "main: Exit From Split Service Station");

exit 0 ;


################################################################################
#    compose_file_group                                                      
################################################################################
sub compose_file_group {

  my $odl_item =  shift;
  
  # Accessing an attribute: <OdlTree object>->getAttribute( <attribute name> )

  my $local_granulID = $odl_item->getAttribute ( 'DATASET_ID' );
  $local_granulID =~ s/"//g;
  
  # EDG puts LOCALGRANULEID in a CRITERIA_FROM_PSA
  # Check that first and replace the bogus one obtained from DATASET_ID
  my $lgid;
  foreach  ( $odl_item->search(NAME => 'SPECIALIZED_CRITERIA', CRITERIA_NAME => '"LOCALGRANULEID"') ) {
    $lgid = odl_cleanup($_->getAttribute('CRITERIA_VALUE'));
    S4P::logger('INFO',  "compose_file_group(): Local Granule ID: $lgid\n");
  }

  $local_granulID = $lgid if ($lgid); 

  my $package_id = $odl_item->getAttribute( 'PACKAGE_ID' );
  $package_id =~ s/"//g;
    
  my $media_type = $odl_item->getAttribute( 'MEDIA_TYPE' );
  $media_type =~ s/"//g;

  if ( ! $media_type ) {

    S4P::logger( 'WARN', "compose_file_group(): The MEDIA_TYPE attribute is missing in LINE_ITEM $local_granulID ...." );
    $media_type = "\"FtpPull\"";

    unless  ( $odl_item->setAttribute ( 'MEDIA_TYPE', $media_type ) ) {
      S4P::logger( 'ERROR', "compose_file_group(): Failed to Update MEDIA_TYPE" );
      return ( undef, $local_granulID, undef, undef );
    }

    $update_item = 1;
    S4P::logger( 'INFO', "compose_file_group(): Set MEDIA_TYPE to FtpPull attribute in LINE_ITEM $local_granulID..." )
  }

  my $media_format = $odl_item->getAttribute( 'MEDIA_FORMAT' );
  $media_format  =~ s/"//g;

  if ( ! $media_format ) {

    S4P::logger( 'ERROR', "compose_file_group(): The MEDIA_FORMAT attribute is missing in LINE_ITEM $local_granulID..." );
    $media_format = "\"FILEFORMAT\"";

    unless  ( $odl_item->setAttribute ( 'MEDIA_FORMAT', $media_format ) ) {
      S4P::logger( 'ERROR', "compose_file_group(): Failed to Update MEDIA_FORMAT" );
      return ( undef, $local_granulID, undef, undef );
    }

    $update_item = 1;
    S4P::logger( 'INFO', "compose_file_group(): Set FILEFORMAT to MEDIA_FORMAT attribute in LINE_ITEM $local_granulID..." );
  }


  my ($short_name, $version_id, $db_id )  = ( $package_id =~ /SC:(.*)\.(\d\d\d):([^:]*)$/) 
                                            ? ( $1, $2, $3 ) : ( undef, undef, undef );

  my $ur = S4PM::dbid2ur ( $CFG1::ur_srvr, $short_name, $version_id, $db_id );

  my $old_dbID_rec = S4PM::GdSDSRVDbi::get_granule ( $dbh, $db_id );
  
  if ( $old_dbID_rec->{LocalGranuleID} ) {

     unless ( $local_granulID eq $old_dbID_rec->{LocalGranuleID} ) {

         S4P::logger ( 'INFO', 
	  "compose_file_group(): Local granulID $local_granulID in ODL is diff from SDSRV databas $old_dbID_rec->{LocalGranuleID}");
         $local_granulID = $old_dbID_rec->{LocalGranuleID};
         # Update odl group with the new local granule ID 
         if ( update_item_with_lgid( $odl_item, $old_dbID_rec ) ) {        
             $update_item = 1;
         }    
     }
   }

  if ( $old_dbID_rec->{DeleteFromArchive} eq "Y" ) {

    S4P::logger ( 'INFO', "compose_file_group(): $local_granulID ( $db_id ) is a DFA");

    my $query_reps =  S4PM::GdSDSRVDbi::get_replacement_queries( $dbh );
    
    my $new_dbID_rec =  S4PM::GdSDSRVDbi::find_replacement_granule( $old_dbID_rec, $query_reps, $dbh);   

    if ( keys %$new_dbID_rec) {

      S4P::logger ( 'INFO', 
                    "compose_file_group(): Found replacement granule $new_dbID_rec->{LocalGranuleID} ( $new_dbID_rec->{dbID} ) "
                    ."for DFA $local_granulID ( $db_id )");
    } else {

      S4P::logger( 'ERROR',  "compose_file_group(): Could not found replacement granule for DFA $local_granulID ( $db_id )");

      # Store bad granule information in an array for later use
      push @bad_granules, $request_part_number.': '.$db_id.' '.$local_granulID.' NO_REPLACE_DFA';

      return ( undef, $local_granulID, undef, undef );
    }
  
    $new_dbID_rec->{ShortName} =~ s/\s+$//;

    # Update odl group with the replacement granule 
    if ( update_data_item( $odl_item, $new_dbID_rec ) ) {        
      $update_item = 1;
    } 

    my $new_ur = S4PM::dbid2ur ($CFG1::ur_srvr, $new_dbID_rec->{ShortName}, $new_dbID_rec->{ VersionID}, 
			   $new_dbID_rec->{dbID} );

    return ( $new_ur, $new_dbID_rec->{LocalGranuleID}, 
             $new_dbID_rec->{BeginningDateTime},$new_dbID_rec->{EndingDateTime});
    
  } elsif ( $old_dbID_rec->{DeleteFromArchive} eq "N" or
            $old_dbID_rec->{DeleteFromArchive} eq "G" ) {

    my $ur = S4PM::dbid2ur ($CFG1::ur_srvr, $short_name, $version_id, $db_id );
    return ( $ur, $local_granulID, $old_dbID_rec->{BeginningDateTime}, $old_dbID_rec->{EndingDateTime});
  } else {

    S4P::logger( 'ERROR',  "compose_file_group(): The Requested Granule $db_id Does Not Exist");
    push @bad_granules, $request_part_number.': '.$db_id.' '.$local_granulID.' NOT_EXIST';
    return ( undef, $local_granulID, undef, undef );
  }

}

################################################################################
#     delete_data_item                                                    
################################################################################
sub delete_data_item {

  my ( $odl_node, $bad_granule ) = @_;

  my $granule = '"'.$bad_granule.'"';

  unless ($odl_node->delete ( NAME =>'LINE_ITEM', DATASET_ID => $granule ) ) {
    S4P::logger ('INFO', "delete_data_item(): Delete Bad Granule $bad_granule From Input Work Order" );
    return 1;
  }

  return 0;
}
################################################################################
#     delete_data_item_set                                                    
################################################################################
sub delete_data_item_set {

  my $set_node = shift;

  my @items = $set_node->search( NAME => 'LINE_ITEM' );

  foreach my $item ( @items ) {
    my $granule = $item->getAttribute ( 'DATASET_ID' );

    unless ($set_node->delete ( NAME =>'LINE_ITEM', DATASET_ID => $granule ) ) {

      S4P::logger ('INFO', "delete_data_item_set(): Delete Granule $granule From Input Work Order" );
    }
  }

  return 1;
}

################################################################################
#   update_item_with_lgid
################################################################################

sub update_item_with_lgid {

  my ( $data_set, $new_rec ) = @_;

  # EDG puts LOCALGRANULEID in a CRITERIA_FROM_PSA
  # Check that first and replace the bogus one obtained from DATASET_ID

  my $n = "\n";
  my $t = "\t";
  foreach  ( $data_set->search(NAME => 'SPECIALIZED_CRITERIA', CRITERIA_NAME => '"LOCALGRANULEID"') ) {

    my $new_granuleID = $new_rec->{LocalGranuleID};
    $new_granuleID = '('.$n.$t.'"'.$new_granuleID.'")';
    unless  ( $data_set->setAttribute ( 'CRITERIA_NAME', $new_granuleID ) ) {
      S4P::logger( 'ERROR', "update_item_with_lgid(): Failed to Update LOCALGRANULEID" );
      return 0;
    }
  }

  return 1;

}

################################################################################
#     update_data_item
################################################################################
sub update_data_item {

  my ( $data_set, $new_rec ) = @_;

  my $new_size = $new_rec->{SizeMBECSDataGranule} * 1000000;
  my $new_granuleID = $new_rec->{LocalGranuleID};
  $new_granuleID = '"'.$new_granuleID.'"';

  unless  ( $data_set->setAttribute ( 'DATASET_ID', $new_granuleID ) ) {
      S4P::logger( 'ERROR', "update_data_item(): Failed to Update DATASET_ID" );
      return 0;
  }

  unless ( $data_set->setAttribute ( 'SIZE', $new_size ) ) {
      S4P::logger( 'ERROR', "update_data_item(): Failed to Update SIZE" );
      return 0;
  }

  my $package =  sprintf ("SC:%s.%03d:%s", $new_rec->{ShortName},
                           $new_rec->{VersionID}, $new_rec->{dbID} );

  $package = '"'.$package.'"';

  unless ( $data_set->setAttribute ( 'PACKAGE_ID', $package )) {
      S4P::logger( 'ERROR', "update_data_item(): Failed to Update PACKAGE_ID" );
      return 0;
  }   
 
  return 1;

}

################################################################################
#      write_request_file_group                                                  
################################################################################
sub write_request_file_group {

  my ($ur, $granuleID ) = @_;

  my ($ur_server, $short_name, $version_id, $dbID) = S4PM::ur2dbid($ur);

  my $file_spec;

  # Start file_group
  my $file_group = new S4P::FileGroup;
  $file_group->ur($ur);
  $file_group->data_type($short_name);
  $file_group->data_version($version_id);

  # Now make a FILE_SPEC
  $file_spec = new S4P::FileSpec;
  $file_spec->directory_id('INSERT_DIRECTORY_HERE');
  $file_spec->file_type('SCIENCE');
  $file_spec->file_id($granuleID);

  $file_group->file_specs( [$file_spec] ); 

  return $file_group;

}


################################################################################
#       write_request_wo                                                 
################################################################################
sub write_request_wo {
  
  my ( $group, $file , $set) = @_;

  my $pdr = new S4P::PDR;

  if ( $set ) {
    $pdr->file_groups($group);
  } else {
    $pdr->file_groups([$group]);
  }

  $pdr->recount;

  if ($pdr->write_pdr($file) ) {
    S4P::logger('ERROR', "write_request_wo(): Failed to write PDR $file");
    return 0;
  }
    
  return 1;
}


################################################################################
#      write_moredata_file_group                                                  
################################################################################
sub write_moredata_file_group {

  my ($ur, $granuleID, $service, $pspec, $pspec_dir ) = @_;

  # Directory where the Select Data config files reside
  my $config_dir = "..";

  # Using service name, formulate the name of the Select Data config file
  my $configfile = "s4pm_select_data_" . "$service" . ".cfg";
  my $oldstyle_configfile = "specify_data_" . "$service" . ".cfg";

# First, try new name for config files. If that fails, try the old name.

  if ( ! -e "$config_dir/$configfile" ) {
      unless ( -e "$config_dir/$oldstyle_configfile" ) {
          S4P::logger("ERROR", "main: No algorithm configuration file seems to exist for service $service. Looking for $config_dir/$configfile or $config_dir/$oldstyle_configfile.");
          return undef;
      }
      $configfile = $oldstyle_configfile;
  }

  my $algorithm = new S4PM::Algorithm("$config_dir/$configfile");
  if ( ! $algorithm ) {
      S4P::perish(30, "main: Could not create new S4PM::Algorithm object from reading $config_dir/$configfile");
  }

  # Get array of PGE inputs

  my @inputs = @{ $algorithm->input_groups };
  
  my @file_groups;

  # Get main parameters
  my $post_processing_offset = $algorithm->post_processing_offset;
  my $pre_processing_offset  = $algorithm->pre_processing_offset;

  foreach my $input ( @inputs ) {
    my $data_type = $input->data_type;
    my $data_version = $input->data_version;
    my $need = $input->need;
    my $lun  = $input->lun;
    my $timer = $input->timer;
    my $currency = $input->currency;
    my $boundary = $input->boundary;

    # Start file_group
    my $file_group = new S4P::FileGroup;
    $file_group->data_type($data_type);
    $file_group->data_version($data_version);

    my ( $data_id, $data_dir);
    if (  $data_type eq "PSPEC" ) {
      $file_group->ur('INSERT_UR_HERE');
      $data_id = $pspec;
      $data_dir = $pspec_dir;
    } else {
      $file_group->ur($ur);
      $data_id = $granuleID;
      $data_dir = 'INSERT_DIRECTORY_HERE';
    }

    $file_group->need($need);
    $file_group->lun($lun);
    $file_group->timer($timer);
    $file_group->currency($currency);
    $file_group->boundary($boundary);
    
    # Now make a FILE_SPEC
    my $file_spec = new S4P::FileSpec;
    $file_spec->directory_id($data_dir);
    $file_spec->file_type('SCIENCE');
    $file_spec->file_id($data_id);

    $file_group->file_specs( [$file_spec] );
    push @file_groups,  $file_group;
  } 

  return ( \@file_groups, $post_processing_offset, $pre_processing_offset );

}


################################################################################
#      write_moredata_file_groups                                                 
################################################################################
sub write_moredata_file_groups {

  my ($ur_items, $service, $pspec, $pspec_dir ) = @_;

  # Directory where the Select Data config files reside
  my $config_dir = "..";

  # Using service name, formulate the name of the Select Data config file
  my $configfile = "s4pm_select_data_" . "$service" . ".cfg";

  # Create a Algorithm object

  unless ( -e "$config_dir/$configfile" ) {
    S4P::logger('ERROR', 
	"write_moredata_file_groups(): No $service configuration file seems to exist. Looking for $config_dir/$configfile.");
    return undef;
  }

  my $algorithm = new S4PM::Algorithm("$config_dir/$configfile");
  if ( ! $algorithm ) {
      S4P::perish(30, "write_moredata_file_groups: Could not create new S4PM::Algorithm object from reading $config_dir/$configfile");
  }

  # Get array of algorithm inputs

  my @inputs = @{ $algorithm->input_groups };
  
  my @file_groups;
 
  # Get main parameters
  my $post_processing_offset = $algorithm->post_processing_offset;
  my $pre_processing_offset  = $algorithm->pre_processing_offset;

  my $pspec = "PSPEC";
  
  for my $hash_item ( @$ur_items ) {
   
    for my $item ( keys %$hash_item ) {

      my $short_name;

      if ( $item =~ /$pspec/ ) {
	$short_name = $pspec;
      } else {
	( $short_name, undef ) = split (/\./, $item, 2);
      }

      foreach my $input ( @inputs ) {

	my $data_type = $input->data_type;
	my $data_type_proxy = S4PM::get_datatype_if_proxy($short_name, \%CFG2::proxy_esdt_map);
	
	next unless ( $data_type eq $data_type_proxy );

	my $data_version = $input->data_version;
	my $need = $input->need;
	my $lun  = $input->lun;
	my $timer = $input->timer;
	my $currency = $input->currency;
	my $boundary = $input->boundary;

	# Start file_group
	my $file_group = new S4P::FileGroup;
	$file_group->data_type($short_name);
        if ( $data_version =~ /^[0-9]+$/ ) {
            $file_group->data_version($data_version);
        } else {
            $file_group->data_version($data_version, "%s");
        }

	my ( $data_id, $data_dir);
	if (  $data_type eq "PSPEC" ) {
	  $file_group->ur('INSERT_UR_HERE');
	  $data_id = $item;
	  $data_dir = $pspec_dir;
	} else {
	  $file_group->ur($hash_item->{$item});
	  $data_id = $item;
	  $data_dir = 'INSERT_DIRECTORY_HERE';
	}

	$file_group->need($need);
	$file_group->lun($lun);
	$file_group->timer($timer);
	$file_group->currency($currency);
	$file_group->boundary($boundary);
    
	# Now make a FILE_SPEC
	my $file_spec = new S4P::FileSpec;
	$file_spec->directory_id($data_dir);
	$file_spec->file_type('SCIENCE');
	$file_spec->file_id($data_id);

	$file_group->file_specs( [$file_spec] );
	push @file_groups,  $file_group;
      }
    }
  } 

  return ( \@file_groups, $post_processing_offset, $pre_processing_offset );

}
  

  
################################################################################
#       write_moredata_wo                                                 
################################################################################
sub write_moredata_wo {
  
  my ( $groups, $file, $begin, $end, $post, $pre) = @_;
 
  #convert a Sybase default date format to our local standard (CCSDSa) format

  $begin = S4P::TimeTools::sybase2CCSDSa($begin);
  $end = S4P::TimeTools::sybase2CCSDSa($end);

  my $pdr = new S4P::PDR;

  $pdr->file_groups($groups);
  
  $pdr->recount;
  $pdr->processing_start($begin);
  $pdr->processing_stop($end);
  $pdr->post_processing_offset($post);
  $pdr->pre_processing_offset($pre);

  if ($pdr->write_pdr($file) ) {
    S4P::logger('ERROR', "write_moredata_wo(): Failed to write PDR to $file");
    return 0;
  }
    
  return 1;
}

################################################################################
#       write_track_request_wo                                                 
################################################################################
sub write_track_request_wo {

  my ($odl_tree, $track_aref )= @_;

  my $track_request_wo = 'TRACK_REQUEST.'.$order_id.'.wo';

  unless ( sysopen( TRACK, $track_request_wo, O_WRONLY | O_TRUNC | O_CREAT ) ) {

      S4P::logger( 'ERROR', "write_track_request_wo(): Failed to Create track work order $track_request_wo" );	
      return 0;
  }

  foreach ( @$track_aref ) {
    print TRACK $_;
    print TRACK "\n";
  }

  if ( ! $odl_string ) {
      $odl_string = $odl->toString (INDENT => '  ' );
  }

  print TRACK "\n\n";
  print TRACK $odl_string;

  close ( TRACK );
  return 1;
}

################################################################################
#       write_failure_wo                                                 
################################################################################
sub write_failure_wo {

  my $input_wo = shift;
  my $failure_wo = 'ORDER_FAILURE.'.$order_id.'.wo';

  unless ( sysopen( FAILURE, $failure_wo, O_WRONLY | O_TRUNC | O_CREAT ) ) {

      S4P::logger( 'ERROR', "write_failure_wo(): Failed to Create Failure Work Order $failure_wo" );	
      return 0;
  }

  unless ( sysopen( INPUT, $input_wo, O_RDONLY ) ) {

      S4P::logger( 'ERROR', "write_failure_wo(): Failed to Read Inout Work Order  $input_wo" );	
      return 0;
  }

  foreach ( @bad_granules ) {

    print FAILURE $_;
    print FAILURE "\n";
  }

  print FAILURE "\n\n";
  my @input_array = <INPUT>;

  print FAILURE @input_array;
  
  close ( FAILURE );
  close ( INPUT );

  return 1;
}

################################################################################
#   write_expect_wo                                                     
################################################################################
sub write_expect_wo {

  my $expect_hash = shift;
  # create EXPECT work order 
  my $expect_wo = 'EXPECT.'.$order_id.'.wo';

  unless ( sysopen( EXPECT, $expect_wo , O_WRONLY | O_TRUNC | O_CREAT ) ) {

	S4P::logger( 'ERROR', "write_expect_wo(): Failed to create expect work order $expect_wo" );
	return 0;
  }

  foreach my $fileID ( keys %$expect_hash ) {
    print  EXPECT 'FileId='.$fileID.' Uses='.$expect_hash->{$fileID};
    print  EXPECT "\n";
  }

  close ( EXPECT );

  return 1;
}

################################################################################
#   update_input_wo                                                    
################################################################################
sub update_input_wo {

    my ( $odl_note, $input_wo ) = @_;

    unless ( sysopen( TREE, $input_wo, O_WRONLY | O_TRUNC | O_CREAT ) ) {
      S4P::logger( 'ERROR', "update_input_wo(): Failed to open input work order $input_wo" );
      return 0;
    }
  
    $odl_string = $odl_note->toString (INDENT => '  ' );

    print TREE $odl_string;

    close (TREE);
    return 1;
}

################################################################################
#      write_pspec_wo                                               
################################################################################
sub write_pspec_wo {

  my ( $pspec_wo , $odl_note, $pspec_dir ) = @_;
 
  #create PSPEC work order 

  unless ( sysopen( PSPEC, "$pspec_dir/$pspec_wo" , O_WRONLY | O_TRUNC | O_CREAT ) ) {

    S4P::logger( 'ERROR', "write_pspec_wo(): Failed to create pspec work order $pspec_wo" );
    return 0;
  }
  
  my $string = $odl_note->toString (INDENT => '  ', MARGIN => '  ' );

  print PSPEC $string;

  close ( PSPEC );

  return 1;
}


sub odl_cleanup {
    use Text::ParseWords;
    my $val = shift;
    $val =~ s/\(\s*\n\s*//s;
    $val =~ s/\s*\)\s*$//s;
    my @v = parse_line(",",0, $val);
    return (wantarray) ? @v : $v[0];
}
