#!/usr/bin/perl 

=head1 NAME

s4pm_receive_dn.pl - create new or repair work order from ECS DN

=head1 PROJECT

GSFC DAAC

=head1 SYNOPSIS

s4pm_receive_dn.pl 
B<-i> I<email_DN_dir>   
B<-e> I<email_DN_filename>
B<-w> I<workorder_dir> 
B<-o> I<repair_workorder_dir>
B<-d> I<dest_dir>   
B<-l> I<host:dir>   
B<-t>
B<-s>
I<S4PM_DN_file>

=head1 DESCRIPTION

I<s4pm_receive_dn.pl> detects Distribution Notice from either a procmail 
filter setup or from a stationmaster.  With procmail, a filter must be
setup to create the DN file.  Depending on the email address and the
correctness of the data, an output work order will be created for either
the Register Data station. However if there is a DN failure, a repair work 
orders will be created for the repair_dn station.  The repair work order will 
either be DO.FIND_REPAIR.<jobid>.wo or DO.DATA_REPAIR.<jobid>.wo.  Using 
Stationmaster, the same work order will be created.  Since the Stationmaster 
requires an input file, make sure the file is in the station directory.
I<Note: Make sure the -s option is used when using Stationmaster.>

During these processes, all errors and all transactions are logged.
=head1 OPTIONS

=over 4

=item B<-i> I<email_DN_dir>

Location of the email DN directory.

=item B<-e> I<email_DN_filename>

Name of DN email file name.

=item B<-w> I<workorder_dir>

Output directory for the work order.

=item B<-o> I<repair_workorder_dir>

Output directory for the repair work order.

=item B<-t> 

Trigger data output.

=item B<-s> 

If set, use stationmaster, else procmail.

S4PM file name is used if -s is set

=item B<-l> and B<-d>

In some cases, the data actually reside on a disk that is locally accessible
and can simply be symlinked (as the Data Pool).  In this case,
you can specify a destination directory for the symlink (e.g. the DATA/INPUT
directory's fullpath).  You must also specify host:root for the source of the
link.  The host is the hostname in the Distribution Notice that indicates that
it can be locally symlinked.  If the DN does not match this value, the script 
will assume the data have already been pushed to the INPUT directory and are 
not to be symlinked.  The root is the source root to prepend onto the 
directory given in the DN, e.g. /datapool/OPS/user.

Example:  s4pm_receive_dn.pl -d /data1/OPS/s4pm/on_demand/stations/neutral/DATA/INPUT
-l g0dps01u:/datapool/OPS/user

=back

=head1 AUTHOR

Long B. Pham

=head1 ADDRESS

NASA/GSFC, Code 610.2, Greenbelt, MD  20771

=head1 CREATED

08/11/2000

=head1 Y2K

Certified Correct by Long Pham on DATE UNKNOWN

=cut

################################################################################
# s4pm_receive_dn.pl,v 1.4 2007/03/14 18:59:16 mtheobal Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Getopt::Std; 
use strict;
use FileHandle;
use S4P::PDR;
use S4P;
use Time::Local;
use Sys::Hostname;

use vars qw($opt_d $opt_l $opt_i $opt_e $opt_w $opt_o $opt_t $opt_s);

getopts('d:l:i:e:w:o:ts');

# Default directory if input email dir is not passed in
$opt_i ||= '.';
my $efile_dir = $opt_i;

my ($link_root, $link_host, $use_symlinks);
if ($opt_l) {
    ($link_host, $link_root) = split(':', $opt_l);
}

# Retrieve DN file from either email or stationmaster
if ($opt_s) {
    if (! -e $ARGV[0]){
        S4P::perish(4,"Can't read work order filename.\n");
    }
    else {
        $opt_e = $ARGV[0];
    }
}
else {
    $opt_e ||= "tmpEmail.txt";
}
my $dn_file = $opt_e;

# Default directory if work order dir is not passed in
$opt_w ||= '.';
my $wo_dir = $opt_w;

# Default directory if repair work order dir is not passed in
$opt_o ||= '.';
my $repair_dir = $opt_o;

my $trigger = $opt_t;
my $file_type;
my $ur_cnt;
my $file_cnt;
my $tot_file_cnt;
my @ftp_host;
my @data_type;
my @data_version;
my @ur;
my @directory_id;
my @file_id;
my @file_type;
my @file_size;
my @req_id;
my $error_flag;
my $pdr;
my @file_out;
my $i;
my $WORK_ORDER;

# Create new work order for import
#print "Preparing to process new work order...\n";

my $input_file =  $efile_dir."/".$dn_file;
open (DN_FD, "$input_file")
     || S4P::perish(1,"DN Error: Couldn't open $input_file file for reading.\n");

# Read the whole file
my @dn_lines = <DN_FD>;

# Remove EOL from each line
chomp @dn_lines;

# Remove all leading & trailing spaces
foreach (@dn_lines) {s/^\s+//; s/\s+$//;}

# Check if DN FAILED
if (check_DN_failure(\@dn_lines) == -1) {
    # Create new re-work order
    repair_wo(\@dn_lines);

    # Log DN failure distribution notice
    S4P::perish(2,"DN Failure.  Exiting!!!\n");
}
 
# Extract data from DN 
# Get FTP host and strip domain name (easier to compare)
foreach (@ftp_host = grep(m"FTPHOST: ", @dn_lines)) {s/FTPHOST: //g; s/\..*//g};
# Check to see if host matches the one we're looking for 
my $use_symlinks = ($link_root && $link_host eq $ftp_host[0]);
foreach (@req_id = grep(m"REQUESTID: ", @dn_lines)) {s/REQUESTID: //g};
foreach (@directory_id = grep(m"FTPDIR: ", @dn_lines)) {s/FTPDIR: //g};
my $dir_id = $directory_id[0];
if ($use_symlinks) {
    $dir_id = "$link_root/$dir_id";
}
# Use GRANULE as tag, not UR -- S4PA doesn't use UR.
foreach (@ur = grep(m"GRANULE:", @dn_lines)) {s/GRANULE: UR:/GRANULE:/g};
foreach (@data_type = grep(m"ESDT: ", @dn_lines)) {s/ESDT: //g;}
foreach (@data_version = grep(m"ESDT: ", @dn_lines)) {s/ESDT: //g};
foreach (@file_id = grep(m"FILENAME: ", @dn_lines)) {s/FILENAME: //g};

# Calculate total files in the Distribution Notice
$tot_file_cnt = scalar(@file_id);
foreach (@file_type = grep(m"FTPDIR: ", @dn_lines)) {s/FTPDIR: //g};
foreach (@file_size = grep(m"FILESIZE: ", @dn_lines)) {s/FILESIZE: //g};

# Calculate total file names/granule
my @granule;
my $granule_index = 0;
my $tmp_line;
foreach $tmp_line (@dn_lines) {
    if ($tmp_line =~ m/GRANULE:/){
      $granule_index++;
    }
    if ($tmp_line =~ m/FILENAME:/){
      $granule[$granule_index]++;
    }
}

# Check to see if data file exists, EXIT if not
if (($error_flag = check_file(\@file_id, $dir_id, $tot_file_cnt)) == -1){
    S4P::logger("ERROR","ERROR: Non-existent file(s) (see logfile)... exiting!!!\n");
}

# Check to see if data file size is correct, EXIT if not
if (($error_flag = check_file_size(\@file_id, \@file_size, $dir_id ,$tot_file_cnt)) == -1){
    S4P::logger("ERROR","ERROR: Incorrect file size(s) (see logfile)... exiting!!!\n");
}

# File error encountered exiting
if ($error_flag == -1){
    S4P::perish(3,"ERROR(S) encountered... exiting!!! \n");
}

# If we're using Symlinks to link to an FTP Pull request,
# it's time to create the symlinks in the destination directory
if ($use_symlinks) {
    foreach my $f(@file_id) {
        my $local = "$opt_d/$f";
        my $remote = "$dir_id/$f";
        S4P::logger("INFO", "Creating symlink $local -> $remote");
        symlink($remote, $local) or S4P::perish(4, "Error creating symlink: $!");
    }
    # New directory is now the destination directory
    $dir_id = $opt_d;
}
# Extract data type
foreach (@data_type) {s/\.\d+//;}

# Extract data version
foreach (@data_version) {s/\w+\.//;}

# Add file path to each file
foreach (@file_id) {s/^/$dir_id\//;}

# Add unique ID # to REQUEST_ID from DN filename
my @tmp_str = split(m"\.+",$dn_file);
$req_id[0] = pop @tmp_str;
$req_id[1] = pop @tmp_str;

# Create work order file name and directory
if ($trigger) {
    if ($opt_s) {
        $WORK_ORDER = $wo_dir."/REGISTER.".$req_id[1].".".$req_id[0].".wo";
    }
    else {
        $WORK_ORDER = $wo_dir."/DO.REGISTER.".$req_id[1].".".$req_id[0].".wo";
    }
}
else {
    if ($opt_s) {
       $WORK_ORDER = $wo_dir."/CATCH_DATA.".$req_id[1].".".$req_id[0].".wo";
    }
    else {
       $WORK_ORDER = $wo_dir."/DO.CATCH_DATA.".$req_id[1].".".$req_id[0].".wo";
    }
}

# Start a new S4P::PDR for output
$pdr = S4P::PDR::start_pdr();

my @file_tmp = @file_id;

# Create PDR output
for ($ur_cnt = 0;$ur_cnt < scalar(@ur);$ur_cnt++) {
    # Clear the files
    undef(@file_out);

    # Extract files for each granule 
    for ($i = 0;$i < $granule[$ur_cnt+1];$i++) {
        push(@file_out, shift(@file_tmp));
    }

    # Add granule info to pdr
    $pdr->add_granule('ur'=>$ur[$ur_cnt], 'data_type'=>$data_type[$ur_cnt],
                     'data_version'=>$data_version[$ur_cnt],
                     'files'=>\@file_out);
}

# Output new S4P::PDR to file
$pdr->write_pdr($WORK_ORDER);

# Change the workorder permission
system ("chmod 664 $WORK_ORDER");

# Log distribution notice
S4P::logger("INFO","Finished processing.\n");

#print "Finished processing!!!\n";

# Check to see if DN failure has been sent
sub check_DN_failure
{
    my ($e_lines) = (@_);
    my @failed_line;

    # Retrieve failure information
    foreach (@failed_line = grep(m"FAILURE", @$e_lines)) {};
   
    if (shift(@failed_line) =~ m/FAILURE/) {
        return (-1);
    }
    else {
        return (0);
    }
}   

# Create new work order for DN failure
sub repair_wo
{
    my ($e_lines) = (@_);
    my @ur_tmp;
    my @reqid_tmp;
    my $repair_file;
    my $i = 0;

    #print "Creating new repair work order...\n";

    foreach (@reqid_tmp = grep(m"REQUESTID: ", @$e_lines)) {s/REQUESTID: //g};
    foreach (@ur_tmp = grep(m"GRANULE:", @$e_lines)) { s/GRANULE:\s*//g ; s/UR:\s*//g ; }

    # Add unique ID # to REQUEST_ID from DN filename
    my @tmp_str = split(m"\.+",$dn_file);
    $reqid_tmp[0] = pop @tmp_str;
    $reqid_tmp[1] = pop @tmp_str;
 
    # Create repair file
    if ($trigger) {
        $repair_file = "DO.DATA_REPAIR.".$reqid_tmp[1].".".$reqid_tmp[0].".wo";
    }
    else {
        $repair_file = "DO.DATA_REPAIR.".$reqid_tmp[1].".".$reqid_tmp[0].".wo";
    }
    open (REPAIR_FD,"> $repair_dir/$repair_file") || S4P::logger("WARNING","DN warning: unable to create repair work order file, $repair_dir/$repair_file. $!\n");
    foreach (@ur_tmp) {print REPAIR_FD "$ur_tmp[$i++]\n"};
    close (REPAIR_FD);
}

# Check to see if file exists
sub check_file
{
    my $i;
    my $error_flag = 0 ;
    my ($file_name,$f_dir,$file_cnt) = (@_);

    for ($i; $i < $file_cnt;$i++) {
        if ((-e "$f_dir/$file_name->[$i]") == 0) {
            $error_flag = -1;
        }
    }  # End of for
   return($error_flag);
}  # End of sub

# Check to see if file size is correct
sub check_file_size
{
    my $i;
    my $error_flag = 0;
    my ($file_name, $file_size, $f_dir, $file_cnt) = @_;

    for ($i; $i < $file_cnt;$i++) {
        if ($file_size->[$i] != (-s "$f_dir/$file_name->[$i]")) {
            S4P::logger('ERROR', "Incorrect file_size $file_size->[$i] for $f_dir/$file_name->[$i]");
            $error_flag = -1;
        }
    }  # End of for
   return($error_flag);
}  # End of sub
