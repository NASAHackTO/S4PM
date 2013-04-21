#!/usr/bin/perl

=pod

=head1 NAME

s4pm_ship_data.pl - Get new Order and Request IDs from ECS and ship data to the user via ECS.

=head1 SYNOPSIS
s4pm_ship_data.pl
B<-f> I<config_file>
I<workorder>

=head1 DESCRIPTION

It is the station script used by the Ship Data station in S4PMOD.

The Ship Data station receives the input work order from the Track Requests 
station. The script obtains a new ECS Order ID and Request ID from the MSS 
database for the output subsetted order and ship data to the user via DCLI.

=head1 ARGUMENTS

=over 4

=item 2. B<-f> configuration file

Station specific configuration file name (ship_data.cfg).

=item 3. work order

The work order is in ODL format.


=head1 ALGORITHM:
   1) Read the work order file received by S4P00.
   2) Create a hash containing user info from ODL file
   3) Create order id string and request id string.  
   4) Insert user info to MSS database EcAcOrder and EcAcRequest table.
   5) Create parameter and dist list files
   6) Submits request to ECS via DCLI to distribute output granules

=head1 EXIT

0 = Successful, 1 = Failed.

=head1 AUTHORS

Yangling Huang, L-3 Government Services

=cut

################################################################################
# s4pm_ship_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Getopt::Long;
use S4P;
use S4PM;
use S4PM::S4PMOD;
use S4PM::GdMSSDbi;
use S4PM::DCLI;
use Fcntl;
use File::Copy;
use Safe;
use strict;

# Get the command line arguments

my $arg = {};
GetOptions( $arg, "f=s", "t=s", "s=s" );

$arg->{w} = $ARGV[0];
my $mode = S4PM::mode_from_path();

my $compartment = new Safe 'CFG';
$compartment->share('$run_dcli');
$compartment->rdo($arg->{'f'}) or
    S4P::perish(30, "main: Failed to read in configuration file " . $arg->{'f'} . " in safe mode: $!");
        
my %record = S4PM::DCLI::get_record($arg->{w} );
       
my $mssDbh = S4PM::GdMSSDbi::mss_db_connect();
					
$record{orderId} = S4PM::GdMSSDbi::get_orderID($mssDbh);
   
if ( $record{orderId} ) {

  S4P::logger( 'INFO', "main: The order Id is $record{orderId}...\n");
} else {

  S4P::perish( 1, "main: Failed to get order ID from MSS database.\n");
}

if ( S4PM::GdMSSDbi::insert_EcAcOrder($mssDbh, %record) ) {
    
  S4P::logger( 'INFO', "main: Insert Order Information To MSS Database EcAcOrder Table\n");
} else {
   S4P::perish ( 1, "main: Failed Insert Order Information to MSS EcAcOrder Table\n" );
}

$record{requestId} = S4PM::GdMSSDbi::get_requestId( $mssDbh);
    
if ( $record{requestId} ) {
    
  S4P::logger( 'INFO', "main: The request Id is $record{requestId}...\n");
} else {
    		
  S4P::perish (1, "main: Failed to get request ID from MSS database.\n");
}  

if (  S4PM::GdMSSDbi::insert_EcAcRequest($mssDbh, %record) ) {

  S4P::logger( 'INFO', "main: Insert Request Information To MSS Database EcAcRequest Table\n");
} else {

  S4P::perish ( 1, "main: Failed Insert Request Information to MSS EcAcRequest Table" );
}
  
my  $param_list =  S4PM::DCLI::write_glparam_list ( \%record );

my  ( $dist_list, @links )  =  S4PM::DCLI::write_gldist_list ( \%record, $arg->{w} );			            

unless ( -f $param_list && -f  $dist_list) {
  
  remove_symbolic_link (@links);  
  S4P::perish( 1, "main: Failed to Create $param_list or $dist_list");
} 

my @common = ( $CFG::run_dcli,
               $mode,
               $record{orderId},
               $record{requestId},
               $dist_list,
               $param_list        
	     );

my $status = system(@common);

if ( $status ) {

  remove_symbolic_link (@links);
  S4P::perish ( 1, "main: Failed to execute $CFG::run_dcli for distributing subsetted granules");
} 

unless ( write_update_wo ($arg->{w}) ) {
  S4P::perish( 1, "main: Cannot Create UPDATE work order" );
}

my $track_request_wo = $arg->{w};

$track_request_wo =~ s/SHIP/CLOSE/;
$track_request_wo =~ s/^DO\.//;
$track_request_wo .= ".wo";

unless ( copy($arg->{w}, $track_request_wo) ) {
  S4P::logger('WARN', "main: Failed to copy $arg->{w} file to $track_request_wo");
}

unless ( remove_symbolic_link (@links) ) {
  S4P::perish ( 1, "main: Failed to remove symbolic links" )
}

# Remove the dist list file.

S4P::perish ( 1, "main: Failed to remove $dist_list" )
    unless unlink "$dist_list";

# Remove the dist list file.

S4P::perish ( 1, "main: Failed to remove $param_list" )
    unless unlink "$param_list";

# Remove the local copy of the dist list file.

if ( -f "$record{requestId}.DistList" ) {
  S4P::perish ( 1, "main: Failed to remove $record{requestId}.DistList (local copy)" )
      unless unlink "$record{requestId}.DistList";
}

exit 0;
      
############################################################################
# write_update_wo
# Creates the UPDATE work order, decrementing the Uses for each output file.
############################################################################   

sub write_update_wo {

  my $input_file = shift;
  my $update_wo = $input_file;

  $update_wo =~ s/SHIP/UPDATE/;
  $update_wo =~ s/^DO\.//;
  $update_wo .= ".wo";

  unless ( sysopen( UPDATE, $update_wo, O_WRONLY | O_TRUNC | O_CREAT ) ) {
    S4P::logger( 'ERROR', "write_update_wo(): Can not open $update_wo: $!" );
    return 0;
  }

  my $odl = S4P::OdlTree->new( FILE => $input_file);
     
  foreach ( $odl->search( NAME => 'LINE_ITEM' ) ) {
       
    my $path = $_->getAttribute( 'OUTPUT_PATH' );
    $path =~ s/^\(|\)$|\"//g; 

    my $dataset =  $_->getAttribute( 'DATASET_ID' );
    $dataset  =~ s/^\(|\)$|\"//g;

    my $metadata = $_->getAttribute( 'METADATA_ID' );
    $metadata =~ s/^\(|\)$|\"//g;   
    printf UPDATE "FileId=%s/%s Uses=-1\n", $path, $dataset;
    printf UPDATE "FileId=%s/%s Uses=-1\n", $path, $metadata;
  }

  close (UPDATE);
  return 1;

}
   
############################################################################
# remove_symbolic_link 
# 
############################################################################ 

sub remove_symbolic_link {

  my @symbolic_links = @_;

  # Remove the symbolic link   
  foreach my $link ( @symbolic_links ) {

    if ( readlink( $link ) ) {
      # unlink returns the number of filenames sucessfully
      unless ( unlink $link ) {
        S4P::logger ( 'ERROR', "remove_symbolic_link(): Failed to remove symbolic link $link: $!" );
	return 0;
      }
    } else {
      S4P::logger ( 'WARN', "remove_symbolic_link(): The symbolic link $link does not exist" );
      return 0;
    }
  }
  
  return 1;
}
