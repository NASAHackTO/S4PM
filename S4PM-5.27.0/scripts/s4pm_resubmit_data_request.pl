#!/usr/bin/perl 

=head1 NAME

s4pm_resubmit_data_request.pl - resubmit data request to ECS after failed DN

=head1 SYNOPSIS

s4pm_resubmit_data_request.pl 
B<[-L]> 
B<[-h]> 

=head1 DESCRIPTION

B<s4pm_resubmit_data_request.pl> will extract URs from a DATA_REPAIR work
order created by the Receive DN station when a distribution failure occurs 
within the ECS and the DN provided by ECS indicates a distribtion failure
along with the URs that failed in the request, and resubmit the request to the 
ECS Science Data Server via the SCLI.  Data has been requested by the 
Request Data station already and appropriate allocations have been made by 
data type and are outstanding.  Stub files may also exist in the REQUESTS 
directory in the Request Data station. 

The SCLI parameter file and acquire script must be available to
B<s4pm_resubmit_data_request.pl> script in the Receive DN station, nominally 
via symbolic links to these files in the Request Data station.

=head1 ARGUMENTS

=item B<-L>

Directory where log file from job execution end up

=item B<-h>

If the SCLI is on a remote machine, this should be set to the remote machine
name on which SCLI is installed.

=head1 AUTHOR

Bruce Vollmer, NASA/GSFC, Code 610.2, Greenbelt, MD 20771 

=cut

################################################################################
# s4pm_resubmit_data_request.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_L
            $opt_h
            $do
	    $jobtype
            $jobid
            $filename
	    $pid
            $wo
            $mode
            $log_dir
           );
use Cwd;
use Safe;
use S4P;
use S4P::PDR;
use S4PM;
use File::Basename;
use Getopt::Std;

# Get log directory from station.cfg

  getopts('L:h:');
  $log_dir = $opt_L;

# Get remote SCLI host, if any

my $SCLI_host = $opt_h;

# Glob for the input workorder (DO.DATA_REPAIR*)

my @files = <DO.DATA_REPAIR*>;

my $num_files = @files;

if ($num_files == 1) {

    $filename = $files[0];

#   parse the input workorder filename

    ($do, $jobtype, $jobid, $pid, $wo) = split(/\./, $filename, 5);

# redirect log to log directory

my $redirect = "$log_dir" . "RESUBMIT_REQUEST." . "$jobid." . "$pid.log"; 
S4P::redirect_log($redirect);

    S4P::logger("INFO", "*** s4pm_resubmit_data_request.pl starting for work order: $filename");

} else {

    S4P::perish(1,"Number of DATA_REPAIR work orders is $num_files \n");

}


#check jobtype - should be DATA_REPAIR 

unless ($jobtype eq "DATA_REPAIR") {
    S4P::perish(1,"Input jobtype not DATA_REPAIR; jobtype: $jobtype");
}

#initialize array for list of URs to order from ECS

my @URlist = ();

# open DATA_REPAIR workorder to extract URs to re-order from ECS
# and put URs into array URlist

 open(WORKORDER,"$filename") || die "Could not open $filename\n";

while (<WORKORDER>) {

    chomp;

#   strip GSF reference from UR and adjust byte count from 13 to 10

    $_ =~ s/13:\[GSF:DSSDSRV\]/10:[:DSSDSRV]/;

    push(@URlist, $_);
}

# check size of URlist, if 0 then no new URs to order
# from ECS, log msg and exit

my $num_new_URs = @URlist;

if ( $num_new_URs == 0) {
S4P::logger("INFO","*** ATTENTION: Number of new URs to process is: $num_new_URs, No new URs to process in workorder $filename, exiting program *** \n");

exit 0;
}

#construct filename for list of URs that will be used when SCLI is invoked

my $URfile = join(".","URfile" , $jobid);
S4P::logger("DEBUG", "URfile name: $URfile \n");

# load UR(s) into a string for writing to URfile

my $URstring = join("\n",@URlist);

# write URs to URfile

my $rc = S4P::write_file($URfile,"$URstring\n");

if ($rc) {
    S4P::logger("INFO","URfile successfully written, filename: $URfile \n");
} else {
    S4P::perish($rc,"Error writing URfile, filename: $URfile \n");
}

# set correct ECS mode
$mode = S4PM::mode_from_path();
$mode = "TS2" if ($mode eq "DEV");

# contruct unique tag for SCLI submission
# tag = epochal time appended to process id
my $tag = (time) . $$;

# set up commandline for invoking SCLI to order granules from ECS
# If SCLI_host is not specified in cfg file then we're making a local call to
# SCLI, else make an ssh connection to another host that has SCLI:

my $command = "../acquire $mode -p ../ACQParmfile -f $URfile -t $tag";
if ($SCLI_host) {
    S4P::logger("INFO","main: SCLI_host specified, SCLI invoked on $SCLI_host. \n");
    my $cwd = getcwd();
    $command = "ssh -x $SCLI_host 'cd $cwd && $command'";
}

(my $err_string, my $err_code) = S4P::exec_system($command);

# check return code from SCLI execution
# if execution is successful, delete URfile 
# if execution is unsuccessful, get out now, leave debris, trap and
# report errors to log file
#
 
if($err_code == 0) {

    S4P::logger("INFO","SCLI successfully invoked for workorder $filename \n"); 

#   write URfile to log then get rid of URfile

    S4P::logger("INFO","URs re-orderered: \n $URstring \n");

    unlink($URfile) || S4P::perish(1,"Error unlinking $URfile \n");

}  else {

    S4P::logger("INFO","Call to SCLI FAILED for workorder $filename \n");


    S4P::perish(1,"Error invoking SCLI for workorder $filename,
                 error_code: $err_code, error message: $err_string \n");
}

S4P::logger("INFO", "*** s4pm_resubmit_data_request.pl successfully completed for work order: $filename");

exit 0;
