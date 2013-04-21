#!/tools/gdaac/COTS/perl-5.8.5/bin/perl 

=head1 NAME

s4pm_request_data.pl - script to request files from ECS SDSRV for input into data reprocessing.

=head1 SYNOPSIS

s4pm_request_data.pl.pl 
B<[-a]> 
B<[-t]> 
B<[-f]> 
B<[-p]> 
B<[-r]> 
B<[-s]> 
B<[-h]> 
B<[-l]> 
I<input_workorder>

=head1 DESCRIPTION

B<s4pm_request_data.pl> processes input work order in which specification is 
made of data file(s) to order from ECS via SDSRV Command Line Interface (SCLI).
Stub files are generated for files that have been ordered from ECS and
placed in a "requests" sub-directory under the Request Data station
directory. As each workorder is processed, the existence of stub files
is checked to avoid ordering essentially the same file twice.
Additionally, requests for duplicate files (same data time coverage
with different insert/production time) are checked (nominal production only,
on demand processing checks fully specified filename for duplicates by use
of command line option [-a]).

Prior to ordering files from ECS space is allocated within a disk pool 
for the incoming files. Filesize is obtained from the FILE_SPEC within 
each FILE_GROUP in the incoming workorder.

The available pool disk space is checked. If there is enough disk
space, the amount of space available in the pool is updated. Otherwise
we wait.

If call to SCLI fails, then the space allocated per datatype will be
reallocated.

=head1 Invoking the SCLI

elements needed for SCLI invocation:

acquire - static script resident in station directory that invokes SCLI binary 
resident within ECS 

Mode - resident within s4pm_request_data.pl script specifed at installation 
by build script

Parameter file - static file resident in request_data station directory 
passed to the s4pm_request_data.pl script via station.cfg file

UR file - dynamic file containing list of URs to order from ECS resident 
in the request_data station run directory. 

Tag - job specifc tag to track failures in log file. Jobid is used.

=head1 ARGUMENTS

=item B<-a>

Command line option used to skip pattern matching when checking for duplicate 
files (same data time coverage with different insert/production time) in the 
request. All fields in the stubfile name are used when checking for the 
existence of stub files (fully specified stub file). Used only for On Demand 
processing. Not invoked for nominal processing.

=item B<-t>

Command line option used to rename input work order as an output workorder for 
tracking requests. Output work order is sent to Track Request station via 
Stationmaster.  Used primarily for On Demand processing. Not invoked for 
nominal processing.

=item B<-f>

Request Data configuration file.

=item B<-p>

Parameter file used by SCLI that contains information necessary to acquire
files from ECS (username, push directory, etc.), located in station directory.

=item B<-r>

Directory specification where stub files for requested data files are placed.

=item B<-s>

Acquire script that invokes SCLI, located in request_data station directory.

=item B<-h>

Command line option for specifying a host to connect to via ssh to invoke the
SCLI. Host specified in this option must have the SCLI available (Sun or SGI).
Used when running on a platform where the SCLI is not available (i.e., Linux).

=item B<-l>

This causes the allocation of disk pool space to be skipped for all data types.
It assumed that all inputs are symbolic links only and therefore, no disk
allocation is necessary.

=head1 CONFIGURATION FILE

The s4pm_request_data.pl script uses the Allocate Disk configuration file for 
configuration items. It contains four hashes. The first hash relates the 
product data type (key) to the maximum data product size (value). The second 
hash maps the file data type (key) to the actual pool (value).  The third hash 
describes the relationship between the pool (key) and disk pool location 
(value).  And the last one maps so-called "proxy" ESDTs to the list of ESDTs 
they proxy for.

datatype_maxsize = (
     'MOD000' => 6500000000,
);

%datatype_pool_map = (
     'MOD000' => 'INPUT',
);

%datatype_pool = (
   'MODIS' => "/usr/daac/dev/prgsrc/s4pm/testbed.unit/mock_data/output/"
);

%proxy_esdt_map = (
    'MODOCL23' => ['MO04MW', 'MO04MD', 'MO04RD'],
    'MODOSS1'  => ['MOD021KM', 'MOD02HKM', 'MOD02QKM'],
);

=head1 AUTHOR

Bruce Vollmer, Bob Mack and Chris Lynnes, NASA/GSFC, Code 610.2, Greenbelt, MD 20771 

=cut

################################################################################
# s4pm_request_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_a $opt_t $opt_f $opt_p $opt_r $opt_s $opt_h $opt_l
            $pdr
            $do
            $jobtype
            $jobid
	    $input_pdr
            $PUSHDIR
            $FILENAME
            $mode
            $cwd
            $command
          );

use Getopt::Std;
use Safe;
use Cwd;
use S4P;
use S4P::PDR;
use S4PM;
use File::Basename;
require 5.6.0;
use File::Copy;
use S4P::ResPool;

# Initialize disk allcation items

my ($pool, $space);
my $pool_db = "../s4pm_allocate_disk.db";
my $wait_for_disk = 300;
my $total_wait = 86400 * 365;  # Got a problem if wait is longer than 1 year!

# Get run time tokens 

getopts('atf:p:r:s:h:l');

# Get tokens for SCLI parameter file, subdirectory for stub files
# and SCLI acquire script from cfg file

my $SCLI_parmfile = $opt_p;
my $REQUESTS_DIR = $opt_r;
my $SCLI_acquire = $opt_s; 
my $SCLI_host = $opt_h;

# Read the configuration file containing pool database configuration
# and mappings.

my $compartment = new Safe 'CFG';
$compartment->share('%datatype_maxsize', '%datatype_pool_map', '%datatype_pool', '%proxy_esdt_map');
$compartment->rdo($opt_f) or
    S4P::perish(30, "main: Failed to read configuration file $opt_f in safe mode: $!");

# Get filename to process (input workorder name)

my $filename = $ARGV[0];

if (! -r $filename) {
    S4P::perish(2,"main: Cannot read work order from $filename");
}


S4P::logger("INFO", "*** s4pm_request_data.pl starting for work order: $filename");

# Parse the input workorder filename

($do, $jobtype, $jobid) = split(/\./, $filename, 4);

# Read input work order

$pdr = S4P::PDR::read_pdr($filename);

# If the -t option is set in the script invocation, then the input work 
# order will be copied and sent to the Track Requests station.

if ($opt_t ) {

### Create output filename without DO prefix

    my $track_filename = $jobtype . ".$jobid" . ".wo";

### Rename input work order to output work order

    copy ($filename, $track_filename);

}

# Check total file count in PDR, quit if <= 0

my $total_file_count = $pdr->total_file_count;
unless ($total_file_count > 0) {
    S4P::perish(1,"main: File count in input PDR is $total_file_count");
}

# Initialize hash for paired URs and stubfiles that are candidates
# for ordering from ECS

my %URpool = ();

# Initialize array for list of URs to order from ECS

my @URlist = ();

# Initialize array for list of datatypes to order from ECS

my @datatypes_ordered = ();

# Set aside an array for all the file groups from the input work order

my @input_file_groups = @{$pdr->file_groups};

# Initialize string, counter and array for duplicate files

my $dup_string = "";
my $dup_count = 0;
my @dup_files = ();

# Loop through the file groups in the input PDR

foreach my $fg (@input_file_groups) {

### Get UR and data_type from file group

    my $UR = $fg->ur;
    my $datatype = S4PM::get_datatype_if_proxy($fg->data_type, \%CFG::proxy_esdt_map);

    unless ($UR) {
        s4p::perish(1,"main: Problem with UR: $UR in workorder $filename");
    }

### Set aside an array for all the file_specs in this file group

    my @file_specs = @{$fg->file_specs};

    if (! @file_specs) {
        S4P::logger('WARN',"main: No file_specs in file_group for $datatype");
        next;
    }

### Strip 3 character DAAC reference from UR and adjust byte count from 13 to 10

    $UR =~ s/13:\[[A-Z]{3}:DSSDSRV\]/10:[:DSSDSRV]/;

### Get science file from file group - should be one per group

    my @science_files = $fg->science_files;
    my $num_science_files = @science_files;

    S4P::logger("DEBUG", "main: Number of science files in file group: $num_science_files \n");

    ($FILENAME,$PUSHDIR) = fileparse($science_files[0]);

# Prepend the *original* data type (i.e. not the proxy data type) to the file 
# name so that it can be searched for more easily by Sweep Data (when it comes 
# time to delete the stub file)

    $FILENAME = $fg->data_type . "." . $FILENAME;

### Create full path name for stubfile

    my $STUBFILE = "$REQUESTS_DIR/$FILENAME.req";

    S4P::logger("DEBUG","main: Stub filename: $STUBFILE \n");

### If stub file exists then it has already been ordered from ECS, don't 
### process. if stub file does not exist then write stub file to "REQUESTS" 
### directory add corresponding UR to the list of URs to order from ECS.

### Add data type to list of data types to order from ECS and allocate space 
### for this datatype in this file group.

    unless( -e $STUBFILE) {

####### Unless opt_a is invoked, pattern matching will be used to check for 
####### duplicate files (same datatype and datatime). 

####### If opt_a is invoked, then only the full filename of the stubfile will
####### be checked for duplicate files. No pattern matching is used to check
####### for duplicates.

        unless($opt_a) {

########### Check for duplicate input files in the system (same data type 
########### and data time, different production time). If duplicates are found, 
########### increment dup_count, log pertinent info on the duplicate.

########### Parse file name and reconstruct all but production time as pattern
########### to check for duplicate files in the system. If a duplicate is 
########### found, increment the counter and add store the corresponding stub 
########### file.

            (my $dtype, my $yearday, my $datatime, my $version_id, my $prodtime) = split(/\./,$FILENAME,5);

            my $pattern = "$REQUESTS_DIR/" . join(".",$dtype,$yearday,$datatime,$version_id) . '*';

            my @filelist = glob($pattern);
            push(@dup_files,@filelist);

            if (@dup_files) {
                $dup_count++;
                next;
            }
    
        }

        $URpool{$STUBFILE} = $UR;

        push(@datatypes_ordered, $datatype);

        unless ( $opt_l ) {

########### Find the maximum file size for this datatype.

            my $total_req_size = $CFG::datatype_maxsize{$datatype};

########### Map the datatype to the pool name and find the pool directory

            my $data_pool;
            if ( exists $CFG::datatype_pool_map{$datatype} ) {
                $data_pool = $CFG::datatype_pool_map{$datatype};
            } else {
                S4P::perish(30, "main: No data pool has been configured for data type: $datatype ACTION: Check the Allocate Disk configuration file.");
            }

            if ( exists $CFG::datatype_pool{$data_pool} ) {
                $pool = $CFG::datatype_pool{$data_pool};
            } else {
                S4P::perish(30, "main: No data pool directory has been configured for data pool: $data_pool ACTION: Check the Allocate Disk configuration file.");
            }

########### Update the pool amount. If there is not enough space, sleep until
########### space becomes available.

            unless ( -e $pool_db ) {
                S4P::perish(30, "main: Disk pool database $pool_db doesn't seem to exist!");
            }

            unless ( defined S4P::ResPool::read_from_pool($pool, $pool_db) ) {
                S4P::perish(30, "main: No space for ". basename($pool) . " defined in $pool_db!");
            }

            $space = S4P::await($total_wait, $wait_for_disk, "Disk space is low for $datatype which requires: $total_req_size from $pool. Currently, " . S4P::ResPool::read_from_pool($pool, $pool_db) . " is available", \&S4P::ResPool::update_pool, $pool, -1*$total_req_size, $pool_db);

            if (! $space) {
                S4P::perish(2,"main: Could not obtain $total_req_size for $datatype after $total_wait secs");
            }

            S4P::logger("INFO", "main: $total_req_size bytes allocated for $datatype.");
        } else {
           
            S4P::logger("INFO", "main: Link option is specified (-l). Therefore, no disk pool allocation is performed.");

        }

    } else {

        S4P::logger("INFO", "main: STUBFILE: $STUBFILE exists \n");
        S4P::logger("INFO", "main: NOTE: Although file is in the request, it will NOT be ordered from SDSRV because a stubfile exists \n");

    }

}

# Values in URpool hash contain URs that are to be ordered from ECS 
# Put these URs into an array that will be used to fill the UR list 
# that is used when SCLI is invoked to order from ECS 

 @URlist = values(%URpool);

# Check size of URlist, if 0 then no new URs to order from ECS, log msg and 
# exit

my $num_new_URs = @URlist;

if ( $num_new_URs == 0) {

    S4P::logger("INFO","main: *** ATTENTION: Number of new URs to process is: $num_new_URs, Stub files exist for these files, \n No new URs to process in workorder $filename. *** \n");

### Check for duplicate input files in the request; if yes, log and exit 
### with error. In this case all files in the request have duplicates in 
### the database.

    if ($dup_count) {
        $dup_string = join("\n",@dup_files);
        S4P::logger("INFO","main: *** ATTENTION: Duplicate input files found in request: $filename \n");
        S4P::logger("INFO","main: *** Operator action required (check input files in database); DFA where necessary \n");
        S4P::logger("INFO","main: *** Number of duplicates = $dup_count \n");
        S4P::logger("INFO","main: *** Duplicate files: \n$dup_string \n"); 
	S4P::perish(3,"main: Job failed because duplicate input files were detected in $filename \n");
    }

    exit 0;
}

# Construct filename for list of URs that will be used when SCLI is invoked

my $URfile = join(".","URfile" , $jobid);
S4P::logger("DEBUG", "main: URfile name: $URfile \n");

# Load UR(s) into a string for writing to URfile

my $URstring = join("\n",@URlist);

# Write URs to URfile

my $rc = S4P::write_file($URfile,"$URstring\n");

if ($rc) {
    S4P::logger("INFO","main: URfile successfully written, filename: $URfile \n");
} else {
    S4P::perish($rc,"main: Error writing URfile, filename: $URfile \n");
}

# Set correct ECS mode

$mode = S4PM::mode_from_path();
$mode = "TS2" if ($mode eq "DEV");

# Set up commandline for invoking SCLI to order files from ECS

my $tag = substr($jobid, 0, 11) . $$;

# If SCLI_host is not specified in cfg file then we're making a local call to 
# SCLI, else make an ssh connection to another host that has SCLI:

if (! $SCLI_host) {
    $command = "$SCLI_acquire $mode -p $SCLI_parmfile -f $URfile -t $tag";
} else {
    S4P::logger("INFO","main: SCLI_host specified, SCLI invoked on $SCLI_host. \n");
    $cwd = getcwd();
    $command = "ssh -x $SCLI_host 'cd $cwd && $SCLI_acquire $mode -f $URfile -p $SCLI_parmfile -t $tag'";
}

(my $err_string, my $err_code) = S4P::exec_system($command);

# Check return code from SCLI execution. If execution is successful, delete 
# URfile and create stub files for files ordered from ECS. If execution is 
# unsuccessful, get out now, leave debris, trap and  report errors to log file.
 
if($err_code == 0) {

    S4P::logger("INFO","main: SCLI successfully invoked for workorder $filename \n"); 

### Write URfile to log then get rid of URfile

    S4P::logger("INFO","main: URs orderered: \n $URstring \n");

    unlink($URfile) || S4P::perish(1,"main: Error unlinking $URfile \n");

### Write stub files to requests directory

    while ((my $stubfile, my $ur) = each(%URpool)) {
        my $rcode = S4P::write_file($stubfile,"$ur \n");
        unless($rcode) {
        S4P::perish($rcode,"main: Error writing stubfile: $stubfile \n");
        }
    }

}  else {

    S4P::logger("INFO","main: Call to SCLI FAILED for workorder $filename \n");

### Reallocate space for files that will not be coming to s4pm
### due to failed SCLI call (unless these are only links)

    unless ( $opt_l ) {

        foreach my $dt (@datatypes_ordered) {

########### Find the maximum file size for this datatype

            my $total_req_size = $CFG::datatype_maxsize{$dt};

########### Map the datatype to the pool name and find the pool directory

            my $data_pool = $CFG::datatype_pool_map{$dt};
            $pool = $CFG::datatype_pool{$data_pool};

########### Update the pool amount

            $space = S4P::ResPool::update_pool($pool, $total_req_size, $pool_db);

            if($space) {
                S4P::logger("INFO", "main: $total_req_size bytes re-allocated for $dt. \n");
            }
    
        }
    } else {
        S4P::logger("INFO", "main: Skipping reallocation since no allocations were made.");
    }

    S4P::perish(1,"main: Error invoking SCLI for workorder $filename, error_code: $err_code, error message: $err_string \n");
}

# Check for duplicate input files in the request; if yes, log and exit with 
# error. In this case the request contained a mix of valid input files and 
# duplicate input files.

# If duplicate input files were found, log them and error on exit.

if ($dup_count) {

    $dup_string = join("\n",@dup_files);
    S4P::logger("INFO","main: *** ATTENTION: Duplicate input files found in request: $filename \n");
    S4P::logger("INFO","main: *** Operator action required (check input files in database); DFA where necessary \n");
    S4P::logger("INFO","main: *** Number of duplicates = $dup_count \n");
    S4P::logger("INFO","main: *** Duplicate files:\n $dup_string \n"); 
    S4P::perish(3,"main: Job failed because duplicate input files were detected in $filename \n");
}

S4P::logger("INFO", "*** s4pm_request_data.pl successfully completed for workorder: $filename");

exit 0;
