package S4PM::DCLI;

=pod

=head1 NAME

DCLI utility routines.

=head1 SYNOPSIS

use S4PM::DCLI;

=head1 DESCRIPTION

Provides access MSS database and generates distribution list that 
contains list of files needs to be distributed and gl parameter 
in which contains information that DCLI needs to provide to DDIST 
in its call to Submit. example of a glparam list

=head1 AUTHORS

Yangling Huang

=head1 CREATED

Jun 26, 2004

=cut

################################################################################
# DCLI.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Fcntl;
use S4P;
use S4P::MetFile;
use S4P::OdlTree;


#####################################################################################
# get_record
######################################################################################
sub get_record {

  my $user_file = shift;

  my $odl = S4P::OdlTree->new( FILE => $user_file);

  my %record;

  foreach ( $odl->search( NAME => 'CONTACT_ADDRESS' ) ) {

    $record{firstName} = $_->getAttribute( 'FIRST_NAME' );

    $record{lastName} = $_->getAttribute( 'LAST_NAME' );

    ( $record{shipAddrStreet1}, $record{shipAddrStreet2}, undef) =
      split( /,/, $_->getAttribute( 'ADDRESS' ) );

    $record{shipAddrCity} = $_->getAttribute( 'CITY' );

    $record{shipAddrState} = $_->getAttribute( 'STATE' );


    $record{shipAddrZip} =  $_->getAttribute( 'ZIP' );

    $record{shipAddrCountry} = $_->getAttribute( 'COUNTRY' );

    $record{shipAddrPhone} = $_->getAttribute( 'PHONE');

    $record{eMailAddr} = $_->getAttribute( 'EMAIL' );
  }

  foreach ( $odl->search( NAME => 'SHIP_INFO' ) ){
     
    $record{mediaType} = $_->getAttribute( 'MEDIA_TYPE' );
    $record{tapeFormat} = $_->getAttribute( 'MEDIA_FORMAT' );
    $record{destinationDirectory} =  $_->getAttribute( 'FTPPUSH_DEST' );    
    $record{destinationNod} = $_->getAttribute( 'FTP_HOST' );
    $record{ftpAddress} = $_->getAttribute( 'FTP_USER' );
    $record{ftpPassword} = $_->getAttribute( 'FTP_PASSWORD' );
    $record{externalRequestId} = $_->getAttribute( 'USERSTRING');
    
    foreach ($_->search( NAME => 'OUTPUT_INFO'  ) ){
      $record{ESDT_Id} = $_->getAttribute( 'ESDT' );
      $record{numFiles} = $_->getAttribute( 'TOTAL_FILE_COUNT' ); 
      $record{numBytes} = $_->getAttribute( 'TOTAL_FILE_SIZE' );
      $record{orderGranule} = $record{numFiles} / 2;
    }
  }

  foreach ( keys %record) {
    $record{$_} =~ s/^\(|\)$|\"//g;
  }

  return %record;
}
	
####################################################################################
# write_glparam_list 
####################################################################################

sub write_glparam_list {

  my ($record ) = shift;
	
  #create param.in file DCLI input
  my $param_file = "param.list";
  
  unless ( sysopen( PARAM, $param_file, O_WRONLY | O_TRUNC | O_CREAT ) ) {
	
    S4P::logger( 'FATAL', "Failed to create parameter file $param_file" );
    return undef;
  }
    
  if ( $record->{ftpPassword} =~ m/@/g ) {
    $record->{ftpPassword} =~ s/(.*)\@(.*)/$1\@/g;	
  } 

  print PARAM "ECSUSERPROFILE = ECSGuest\n";
  printf (PARAM "USERSTRING = %s\n",  $record->{externalRequestId} ); 
  printf (PARAM "FTPUSER = %s\n",$record->{ftpAddress} );	
  printf (PARAM "FTPPASSWORD = %s\n", $record->{ftpPassword} );
  printf (PARAM "FTPHOST = %s\n", $record->{destinationNod} );
  printf (PARAM "FTPPUSHDEST = %s\n", $record->{destinationDirectory} );
  print PARAM "PRIORITY = NORMAL\n";
  print PARAM "DDISTNOTIFYTYPE = MAIL\n";
  printf (PARAM "MEDIA_FORMAT = %s\n", $record->{tapeFormat}  );
  printf (PARAM "MEDIA_TYPE = %s\n", $record->{mediaType} );
  printf (PARAM "NOTIFY = %s\n", $record->{eMailAddr} );
	
  close ( PARAM );
  return $param_file;
}

####################################################################################
# write_gldist_list 
######################################################################################
sub write_gldist_list {

  my ( $record, $input_wo ) = @_;

  #create dist.in file DCLI input
  my $dist_file = "dist.list";
	
  unless ( sysopen( DIST, $dist_file, O_WRONLY | O_TRUNC | O_CREAT ) ) {
    S4P::logger( 'FATAL', "Failed to create parameter file $dist_file" );
    return ( undef, undef );
  }
    		
  print DIST "<BeginDistList>\n";
  print DIST "<RequestId>\n";
  print DIST "$record->{requestId}\n";
  print DIST "<myNumberofItems>\n";
  print DIST "$record->{numFiles}\n";
  print DIST "<mySize>\n";
  print DIST "$record->{numBytes}\n";
    
  my $odl = S4P::OdlTree->new( FILE => $input_wo );

  my @symbolic_links;  
  foreach ( $odl->search( NAME => 'LINE_ITEM' ) ) {
    
    my $data_size = $_->getAttribute( 'DATASET_SIZE' ) + 
                    $_->getAttribute( 'METADATA_SIZE' );

    my $path = $_->getAttribute( 'OUTPUT_PATH' );
    $path =~ s/^\(|\)$|\"//g; 

    my $dataset =  $_->getAttribute( 'DATASET_ID' );
    $dataset  =~ s/^\(|\)$|\"//g;
   
    chmod 0666, "$path/$dataset" or
      S4P::logger( 'ERROR', "Could not change file $path/$dataset access mode to 0666: $!")
      and return ( undef, undef );
    
    my $metadata = $_->getAttribute( 'METADATA_ID' );
    $metadata =~ s/^\(|\)$|\"//g;

    chmod 0666, "$path/$metadata" or
      S4P::logger( 'ERROR', "Could not change file $path/$metadata access mode to 0666: $!")
      and return ( undef, undef );

    # Get Local Granule ID from datamet file
    my %attrs = S4P::MetFile::get_from_met("$path/$metadata", 'LOCALGRANULEID');

    my $local_granule_id = $attrs{'LOCALGRANULEID'} or 
      S4P::logger ('ERROR',"Could not find LOCALGRANULEID in $metdata\n") 
	  and return ( undef, undef );    
     
    if ( -f "$path/$local_granule_id" ) {
      
      if ( readlink("$path/$local_granule_id") ) {

	S4P::logger ('INFO', 
		     "The symbolic link of \"$path/$local_granule_id\" exist. It should been removed"); 
	unlink ( "$path/$local_granule_id" );
        system("ln -s \"$path/$dataset\" \"$path/$local_granule_id\"");
	push @symbolic_links, "$path/$local_granule_id";
      } else {

	S4P::logger ('INFO', 
		     "Local Granule Id ($local_granule_id ) is the same as the output ($dataset)");
      }
    } else {
    
      system("ln -s \"$path/$dataset\" \"$path/$local_granule_id\"");
      push @symbolic_links, "$path/$local_granule_id";
    }

    my $local_granule_id_met = $local_granule_id.'.met';
    if ( -f "$path/$local_granule_id_met" ) {
      
      if ( readlink("$path/$local_granule_id_met") ) {

	S4P::logger ('INFO', "The symbolic link of \"$path/$local_granule_id_met\" exist, It should been removed");
	unlink ( "$path/$local_granule_id_met" );
        system("ln -s \"$path/$metadata\" \"$path/$local_granule_id_met\"");
	push @symbolic_links, "$path/$local_granule_id_met";

      } else {

	S4P::logger ('INFO', 
		     "$local_granule_id_met is the same as the output metadata ($metadata)");
      }
    } else {
    
      system("ln -s \"$path/$metadata\" \"$path/$local_granule_id_met\"");
      push @symbolic_links, "$path/$local_granule_id_met";
    }

    my $esdt = $_->getAttribute( 'ESDT' );
    $esdt =~ s/^\(|\)$|\"//g;

    print DIST "\n";
    print DIST "<BeginGranule>\n";
    print DIST "<myID>\n";
    print DIST "$local_granule_id\n";
    print DIST "<mySize>\n";
    print DIST "$data_size\n";
    print DIST "<myESDT>\n";
    print DIST "$esdt\n";
    print DIST "myNumItems\n";
    print DIST "2\n";

    print DIST "\n";
    print DIST "<BeginDistFile>\n";
    print DIST "<myUniqueName>\n";
    print DIST "$local_granule_id\n";

    print DIST "<myPath>\n";
    print DIST "$path\n";
    print DIST "<myOriginalName>\n";
    print DIST "$local_granule_id\n";
    print DIST "<mySize>\n";
    printf (DIST "%d\n", $_->getAttribute( 'DATASET_SIZE' ) );
    print DIST "<myCheckSum>\n";
    print DIST "0\n";
    print DIST "<myArchiveId>\n";
    print DIST "-1\n";
    print DIST "<EndDistFile>\n";
	 
    print DIST "\n";
    print DIST "<BeginDistFile>\n";
    print DIST "<myUniqueName>\n";
    print DIST "$local_granule_id_met\n";
    print DIST "<myPath>\n";
    print DIST "$path\n";
    print DIST "<myOriginalName>\n";
    print DIST "$local_granule_id_met\n";
    print DIST "<mySize>\n";
    printf (DIST "%d\n",$_->getAttribute( 'METADATA_SIZE' ) );
    print DIST "<myCheckSum>\n";
    print DIST "0\n";
    print DIST "<myArchiveId>\n";
    print DIST "-1\n";
    print DIST "<EndDistFile>\n";

    print DIST "<EndGranule>\n";
  }
	
  print DIST "\n";
  print DIST "<EndDistList>\n";
	
  close (DIST);

  return ( $dist_file, @symbolic_links ); 
}

1;

