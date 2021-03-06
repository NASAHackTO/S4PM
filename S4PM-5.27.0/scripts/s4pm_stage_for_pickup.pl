#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_stage_for_pickup.pl - pull data from S4P Export station and send back PAN

=head1 SYNOPSIS

s4pm_stage_for_pickup.pl
B<-d> I<staging_dir>
[B<-r> I<return_pan_dir>]
[B<-p> I<pdr_dir>]
I<MOREDATAworkorder>

=head1 DESCRIPTION

This script imports data that was exported via the PDR mechanism, and sends
back a PAN.  It renames the data and metadata files to match the 
local_granule_id.

At this point it copies the PAN back to a specified NFS-mounted directory.

N.B. This code is a fork of s4p_import.pl which copies rather than FTP. 
Some of the comments, variable names, and messages haven't been updated for 
this change.

=head1 AUTHOR

Yangling Huang and Christopher Lynnes, NASA/GSFC, Code 610.2
Eunice Eng, NASA/GSFC

=cut

################################################################################
# s4pm_stage_for_pickup.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use Net::FTP;
use File::Basename;
use File::Copy;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4P::MetFile;
use S4P::TimeTools;
use vars qw($opt_d $opt_p $opt_r);

getopts('d:p:r:');

S4P::perish(1, "No staging directory was specified with the -d argument.")
    unless ( $opt_d );

# Get filename to process (input pdr file)
my $pdr_file = $ARGV[0];
  
S4P::logger("INFO", "*** s4pm_stage_for_pickup.pl starting for work order: $pdr_file ");

# Copy input PDR into PDR directory specified with the -p flag. First, 
# remove the DO. in front.

my $pdr_dir = $opt_p || "../../PDR";
my $new_pdr_file = $pdr_file;
$new_pdr_file =~ s/^DO\.//;
S4P::logger("INFO", "main: Copying $pdr_file to $pdr_dir/$new_pdr_file ...");
copy($pdr_file, "$pdr_dir/$new_pdr_file") or S4P::perish(20, "main: Failed to copy $pdr_file to $pdr_dir/$new_pdr_file: $!");

my $pdr = S4P::PDR::read_pdr($pdr_file);
my $file_count =  $pdr->recount / 2;

my $is_failed = 0;
my $xml_count = 0;

my  ( @TMP_PAN, $status );

foreach my $file_group(@{ $pdr->file_groups }) {
  
    ## Obtain ftp host and file will be pooled location from file group

    my $ftp_host = $file_group->node_name;
    $ftp_host =~ s/^(.*?)\.(.*)$/$1/;
    
    my $status= file_get($file_group,$ftp_host, $opt_d ); 
    unless ( $status ) {
        S4P::perish(40, "main: Failed to copy data from $ftp_host into directory $opt_d");
    }
    
    # Use the first science file as "key" for granule
    my @data_files = $file_group->science_files;
    my $met_file  = $file_group->met_file;
    my ($granule_id, $file_dir) = fileparse($data_files[0]);
 
    my $time_stamp = S4P::TimeTools::CCSDSa_Now();

    push @TMP_PAN, "FILE_DIRECTORY = $file_dir;";
    push @TMP_PAN, "FILE_NAME = $granule_id;";

    # Replace remote directories with local one ($opt_d)
    ($met_file, @data_files) = 
        map {"$opt_d/" . basename($_)} ($met_file, @data_files);

    if (! $status ) {
	$is_failed = 1;
	push @TMP_PAN, 'DISPOSITION = "FTP/KFTP FAILURE";';
        push @TMP_PAN, 'TIME_STAMP =                     ;';
    } 
    else {
        my ($new_met, @new_data) = rename_files($met_file, @data_files);
        push @TMP_PAN, 'DISPOSITION = "SUCCESSFUL";';
        push @TMP_PAN, "TIME_STAMP = $time_stamp;";
    }
}

my @PAN;

if ( ! $is_failed ) {
    my $time_stamp = S4P::TimeTools::CCSDSa_Now();
    push @PAN, "MESSAGE_TYPE = SHORTPAN;";
    push @PAN, 'DISPOSITION = "SUCCESSFUL";';
    push @PAN, "TIME_STAMP = $time_stamp;";
} 
else {
    push @PAN, "MESSAGE_TYPE = LONGPAN;";
    push @PAN, "NO_OF_FILES = $file_count;";
    push (@PAN, @TMP_PAN );
}

my $pan_string = join("\n",@PAN, '');

my $pan_file = $ARGV[0];

$pan_file =~ s/(\.PDR)*$/.PAN/;
$pan_file =~ s/^DO\.//;

$status = S4P::write_file($pan_file, $pan_string );

S4P::perish(110, "Problem writing PAN to $pan_file \n")
  unless  ( $status );

# We manually move the PAN file along with its log file to the Receive PAN
# station since stationmaster has a hard time with this.

my $log_file = $ARGV[0] . ".log";
$log_file =~ s/^DO\.//;
my $new_log_file = $log_file;
$new_log_file =~ s/PDR/PAN/;
rename($log_file, $new_log_file) unless ($log_file eq $new_log_file);

if ($opt_r) {
    foreach ($new_log_file, $pan_file) {
        if (move($_, $opt_r)) {
            S4P::logger("INFO", "Moved $_ to $opt_r directory");
        }
        else {
            S4P::perish(40, "Could not move $new_log_file to $opt_r: $!");
        }
    }
}

S4P::logger("INFO", "***s4pm_stage_for_pickup.pl  successfully completed for workorder: $ARGV[0]");

exit 0;


sub rename_files {
    my ($met_file, @data_files) = @_;
    my %met = S4P::MetFile::get_from_met($met_file, 'LOCALGRANULEID');
    my $lgid = $met{'LOCALGRANULEID'};
    if (! $lgid) {
        S4P::logger("WARN", "Cannot find LOCALGRANULEID in metadata, will skip renaming");
        return ($met_file, @data_files);
    }

    my ($base, $dir, $suffix) = fileparse($met_file, '.met','.xml');

    # Rename metadata file first
    my $new_met = "$dir/$lgid$suffix";
    rename($met_file, $new_met) or 
        S4P::perish(40, "Failed to rename $met_file to $new_met");
    S4P::logger("INFO", "Renamed $met_file to $new_met");

    my $n_data = scalar(@data_files);
    my ($i, @new_data);
    if ($n_data > 1) {
        # Multi-file granule case: add sequence number
        for ($i = 0; $i < $n_data; $i++) {
            push( @new_data, sprintf("%s/%s%d", $dir, $lgid, $i) );
        }
    }
    else {
        push( @new_data, sprintf("%s/%s", $dir, $lgid) );
    }
    for ($i = 0; $i < $n_data; $i++) {
        rename($data_files[$i], $new_data[$i]) or 
           S4P::perish(40, "Failed to rename $data_files[$i] to $new_data[$i]");
        S4P::logger("INFO", "Renamed $data_files[$i] to $new_data[$i]");
    }

    return ($new_met, @new_data);
}

# Copy file from remote host to local dir

sub file_get {

# Original code based on getting files via FTP. This code doesn't use FTP,
# but the variable names haven't been changed because of that.

  my $file_group = shift;       # S4P::FileGroup object to be transferred.
  my $remote_host = shift || $file_group->node_name();      # Remote host name.
  my $local_dir = shift || '.'; # Local directory.
  my $ftp = shift || undef;     # Net::FTP object for persistent connection.
  my $max_attempt = shift || 1; # Max attempts on failure to get a connection.
  my $snooze = shift || 60;     # Snooze time in s for multiple attempts to
                                # get a connection.

  foreach my $file_spec ( @{ $file_group->file_specs } ) {

    my $ftp_path = $file_spec->directory_id;

    # Quit on failure to change directory.

    my $data_file_id = $file_spec->file_id;
    my $desired_file = "$ftp_path".'/'."$data_file_id";
    my $local_file =  $local_dir .'/'. $data_file_id;
    
    # downddload file to local dir
    $status = copy( $desired_file, $local_file);

    if ( $status ) {
      S4P::logger( 'INFO',
                   "successfully downloaded $data_file_id to destination"
                   . " $local_dir" );
    } else {
      S4P::logger( 'FATAL',
                   "failed to download $data_file_id to destination $local_dir("
                   . $! . ")" );
      return undef;
    }
  }

  return 1;
}
