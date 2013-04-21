#!/usr/bin/perl

=head1 NAME

s4pm_insert_datapool.pl - script to insert only non-ECS products into datapool

=head1 SYNOPSIS

s4pm_insert_datapool.pl
B<-d> I<staging_dir>
B<-p> I<pdr_dir>
I<FIND_workorder>

=head1 DESCRIPTION

This script inserts non-ECS data into the ECS datapool.

=head1 AUTHOR

Yangling Huang, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_insert_datapool.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use S4P::PDR;
use strict;
use Getopt::Std;
use Net::FTP;
use Fcntl;
use File::Copy;
use S4P::FileGroup;
use S4PM;
use vars qw($opt_d $opt_p);

getopts('d:p:');

my $mode = S4PM::mode_from_path();

unless ( $opt_d ) {
    S4P::perish(30, "No staging directory was specified with the -d argument.");
}
unless ( $opt_p ) {
    S4P::perish(31, "No PDR directory was specified with the -p argument.");
}

# Get filename to process (input pdr file)
my $pdr_file = $ARGV[0];

S4P::logger("INFO", "*** s4pm_insert_datapool.pl starting for work order: $pdr_file ");

# Copy input PDR into PDR directory specified with the -p flag. First, 
# remove the DO. in front.
my $new_pdr_file = $pdr_file;
$new_pdr_file =~ s/^DO\.//;
copy($pdr_file, "$opt_p/$new_pdr_file");

my $pdr = S4P::PDR::read_pdr($pdr_file);
my $file_count =  $pdr->recount / 2;

my $is_failed = 0;
my $xml_count = 0;

my  ( @TMP_PAN, $status );

foreach my $file_group(@{ $pdr->file_groups }) {
  
  ## Obtain ftp host and file will be pooled location from file group
  my $ftp_host = $file_group->node_name;
  $ftp_host =~ s/^(.*?)\.(.*)$/$1/;
  $status = $file_group->ftp_get(  $ftp_host, $opt_d ); 
    
  unless ( $status ) {
    S4P::perish(60, "Failed to ftp files from $ftp_host");
  } 

  my ( $granule_id,$met_file_id );
 
  foreach my $file_spec ( @{ $file_group->file_specs } ) {
    
    my $data_type = $file_spec->file_type;
    my $ftp_path = $file_spec->directory_id;

    if ( $data_type eq "Science" || $data_type eq "SCIENCE" ) {
      $granule_id = $file_spec->file_id;
    } else {
      $met_file_id = $file_spec->file_id;
    }
       
    if ( $met_file_id ) {

      $xml_count++;
      
      my $insert_file = 'BatchInsert.'.$xml_count;

      # Get Local Granule ID from datamet file  
      my ( $data_file, $met_file ) = get_local_granule_id ( $opt_d, $granule_id, $met_file_id );  

      unless ( sysopen( INSERT, $insert_file, O_WRONLY | O_TRUNC | O_CREAT ) ) {

	S4P::perish(70, "Failed to Populate run time file $insert_file ");
      }

      if ( $data_file && $met_file &&
           -e "$opt_d/$data_file" && -e "$opt_d/$met_file" ) {
      	print INSERT "$opt_d/$met_file";
      } else {
      	print INSERT "$opt_d/$met_file_id";
      }
      
      close ( INSERT );
      
      my @command = (
		     "/tools/gdaac/$mode/bin/DPL/GdDlBatchInsert.pl",
		     $mode,
		     "-nonecs",
		     "-file",
		     $insert_file 
		    );
    
     $status = S4P::exec_system( @command );

      if ( $status ) {	
	S4P::logger ( 'ERROR', "Failed to excuted @command\n\n" );      
      } else {
	S4P::logger ( 'INFO', "Successfully excuted @command\n\n" );
      }

      unlink $insert_file;

      my $time_stamp = get_time_stamp();

      if ( $status ) {

	$is_failed = 1;
	push @TMP_PAN, "FILE_DIRECTORY = $ftp_path;";
	push @TMP_PAN, "FILE_NAME = $granule_id;";
	push @TMP_PAN, "DISPOSITION = \"FAILURE TO INSERT FILE TO DPL\";";
	push @TMP_PAN, "TIME_STAMP = $time_stamp;";

      } else {
	push @TMP_PAN, "FILE_DIRECTORY = $ftp_path;";
	push @TMP_PAN, "FILE_NAME = $granule_id;";
	push @TMP_PAN, "DISPOSITION = \"SUCCESSFUL\";";
	push @TMP_PAN, "TIME_STAMP = $time_stamp;";

      }

      if ( -f $insert_file ) {
	S4P::perish ( 72, "Failed to remove $insert_file" )
	  unless unlink $insert_file;
      }

    }
    undef  $met_file_id unless ( ! $met_file_id );
  }
}

my @PAN;

if ( ! $is_failed ) {

  my $time_stamp = get_time_stamp();

  push @PAN, "MESSAGE_TYPE = SHORTPAN;";
  push @PAN, "DISPOSITION = SUCCESSFUL;";
  push @PAN, "TIME_STAMP = $time_stamp;";
} else {
  
  push @PAN, "MESSAGE_TYPE = LONGPAN;";
  push @PAN, "NO_OF_FILES = $file_count;";
  push (@PAN, @TMP_PAN );
}

my $pan_string = join("\n",@PAN);

my $pan_file = $ARGV[0];

$pan_file =~ s/PDR/PAN/;
$pan_file =~ s/^DO\.//;

$status = S4P::write_file($pan_file, $pan_string );

S4P::perish(110, "Problem writing PAN to $pan_file \n")
  unless  ( $status );

if ( -f "failed.list" ) {
  S4P::perish ( 75, "Failed to remove failed.list " )
    unless unlink "failed.list";
}

# We manually move the PAN file along with its log file to the Receive PAN
# station since stationmaster has a hard time with this.

my $log_file = $ARGV[0] . ".log";
$log_file =~ s/^DO\.//;
my $new_log_file = $log_file;
$new_log_file =~ s/PDR/PAN/;

move($new_log_file, "../../receive_pan");
S4P::logger("INFO", "Move $new_log_file to ../../receive_pan directory...");

move($pan_file, "../../receive_pan");
S4P::logger("INFO", "Move $pan_file to ../../receive_pan directory...");

S4P::logger("INFO", "***s4pm_insert_datapool.pl  successfully completed for workorder: $ARGV[0]");

exit 0;


#######################################################################
#    get_time_stamp
#######################################################################
sub get_time_stamp {
   
  my $datetime = S4P::format_gmtime(time);

  my @datetime = split(/ /,$datetime);
  my $date = $datetime[0];
  my $time = $datetime[1];
  my $timestamp = $date . "T" . $time . "Z";

  return $timestamp;
}


#######################################################################
#    get_local_granule_id
#######################################################################

sub get_local_granule_id {

  my ( $stage, $data_file, $met_file ) = @_;

  my $met_string = `cat $stage/$met_file`;

  my ($lgid) = $met_string =~ /.+<LocalGranuleID>\ *(.+)\ *<\/LocalGranuleID>.+/is;

  my ($file ) = $met_string =~ /.+<File type="Science">\ *(.+)\ *<\/File>.+/is;

  $met_string  =~ s/$file/$lgid/is unless ( $lgid eq $file );

  my $lgid_met = $lgid.'.xml';

  if (  $lgid eq $data_file ) {

    S4P::logger ( 'INFO', "Local granule id $lgid is the same as data file $data_file");
    return ( undef, undef );

  } else {

    S4P::logger ( 'INFO', "Local granule id $stage/$lgid is not the same as data file $stage/$data_file");

    rename ( "$stage/$data_file", "$stage/$lgid" ) and 
      S4P::logger ( 'INFO', "Successfully rename $stage/$data_file to $stage/$lgid" ) || 
      S4P::perish ( 80, "Cannot rename $stage/$data_file to $stage/$lgid: $!");

    S4P::perish( 70, "Failed to open $stage/$lgid_met......")
      unless ( sysopen( MET, "$stage/$lgid_met", O_WRONLY | O_TRUNC | O_CREAT ) );

    print MET $met_string;
    close ( MET );

    if ( unlink "$stage/$met_file" ) {
      S4P::logger ( 'INFO', "Remove $stage/$met_file......" );
    } else {
		 
      S4P::perish ( 80, "Failed to remove $stage/$met_file....." );
    }

  }
  
  return ($lgid, $lgid_met );  
}
  
