#!/usr/bin/perl

########################################################################

=head1 NAME

s4pm_allocate_disk.pl - assign a disk pool for output and allocate disk space

=head1 SYNOPSIS

s4pm_allocate_disk.pl 
B<-f> I<allocate_disk_cfg> 
B<-d> I<allocate_disk_db>
I<input_workorder>

=head1 DESCRIPTION

B<s4pm_allocate_disk.pl> reads the input ALLOCATE work order (which is
a Process Control File) and searches for records with the key words, 
INSERT_DIRECTORY_HERE. When it finds these records, it determines the data
type from the file name field in the record. The data type is used to 
determine the maximum size of the product and hence, the space to allocate.

The available pool disk space is checked. If there is enough disk 
space, the amount of space available in the pool is updated. Otherwise,
B<s4pm_allocate_disk.pl> exits with an exit code of 1. If run with 
B<s4p_repeat_work_order.pl>, this particular exit code is interpreted as a
"failed to get disk space" error and the work order is recycled so that
it can be attempted again later. Non-zero exit codes great than 1 are
interpreted as true failures and B<s4p_repeat_work_order.pl> will not recycle
such work orders.

If all requested disk space is available, the keywords INSERT_DIRECTORY_HERE
in the input work order (PCF) records are replaced with the actual allocated 
directory pool locations. An output RUN work order (also a PCF) is generated 
for the downstream Run Algorithm station. 

=head1 AUTHOR

Bob Mack, NASA/GSFC, Code 610.2
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_allocate_disk.pl,v 1.8 2008/04/22 18:13:09 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use S4P::ResPool;
use S4PM;
use Getopt::Std;
use vars qw($opt_f $opt_d);

# Initialize

my (%datatype_pool, %datatype_maxsize, %datatype_pool_map, %proxy_esdt_map);
my ($pool, $space);
my $pcf_string = '';
my $out_work_order = '';
my $wait_for_disk = 300;
my $sleep_msg;
my $total_req_size;
my $data_pool;

# Array to keep track of individual disk allocations in case one fails and we
# have to roll back.

my @undo_allocate = ();	

## Read the configuration file containing pool database configuration
## and mappings.

getopts('f:d:');

unless ( $opt_f ) {
    S4P::perish(30, "main: No Allocate Disk configuration file specified with the -f argument.");
}

unless ( $opt_d ) {
    S4P::perish(30, "main: No Allocate Disk database file specified with the -d argument.");
}

my $pool_db = $opt_d;
unless ( -R $pool_db ) {
    S4P::logger("FATAL", "main: Database file $pool_db is unreadable: ACTION: Adjust permissions so that $pool_db is readable by the user running S4PM.");
    S4P::raise_anomaly('DATABASE_UNREADABLE', '..', 'FATAL', "Database file $pool_db is unreadable", 0);
}

my $string = S4P::read_file($opt_f);
exit 8 if ! $string;
eval ($string);
S4P::perish(10, "eval: $@\n"), if $@;

## Read input PCF work order.

my $wo_string = "";
my $work_order = $ARGV[0];
$wo_string = S4P::read_file($work_order);
if (! $wo_string) {
    S4P::perish (20, "main: Reading of the input work order file $work_order failed!\n");
}

## Search the records of the PCF work order for the key phrase
## INSERT_DIRECTORY_HERE. When it's found, replace it with the 
## disk pool directory location.

my @pcf_lines = split(/\n/, $wo_string);  # Separate string into records.

# First we collect up disk pool allocations that need to be made and later
# we'll actually do the allocation. In this way, for multiple files of the
# same data type, only one summed request will be made rather than a bunch of
# smaller individual ones.

my %allocations = ();
foreach (@pcf_lines) {

### Skip comments; can cause havoc if INSERT_DIRECTORY_HERE is in a comment

    next if (/^#/);  
    if ( /INSERT_DIRECTORY_HERE/ or /INSERT_MULTIDIRECTORY_HERE/ ) {
        my @fields = split (/\|/, $_);   # Split the record into fields.

####### Parse the S4PM file name for the datatype. Find the maximum file size 
####### for the data type. Map the datatype to the pool name. Find the pool 
####### directory.

        my ($datatype, undef, undef, undef, undef) =
            S4PM::parse_patterned_filename($fields[1]);

####### Check to see if the data type is present in the datatype_maxsize
####### and datatype_pool_maxsize hashes found in the configuration files.
####### If it isn't in both hashes, fail with an error message.

        my $found_dt_in_maxsize = 0;
        foreach my $key ( keys(%datatype_maxsize) ) {
            if ($datatype eq $key) {
                $found_dt_in_maxsize = 1;
                last;
            }
        }

        my $found_dt_in_poolmap = 0;
        foreach my $key ( keys(%datatype_pool_map) ) {
            if ($datatype eq $key) {
                $found_dt_in_poolmap = 1;
                last;
            }
        }

        if ($found_dt_in_poolmap && $found_dt_in_maxsize) {
            $total_req_size = $datatype_maxsize{$datatype};
            $data_pool = $datatype_pool_map{$datatype};
            $pool = $datatype_pool{$data_pool};
        } else {
            if (!$found_dt_in_poolmap && !$found_dt_in_maxsize) {
                S4P::perish (30, "Data type [$datatype] could not be found in $opt_f: datatype_pool_map and datatype_maxsize hash!\n");
            } elsif (!$found_dt_in_poolmap) {
                S4P::perish (31, "Data type [$datatype] could not be found in $opt_f: datatype_pool_map hash!\n");
            } elsif (!$found_dt_in_maxsize) {
                S4P::perish (32, "Data type [$datatype] could not be found in $opt_f: datatype_maxsize hash!\n");
            } else {
                S4P::perish (33, "UNKNOWN error with $datatype in file: $opt_f!\n");
            }
        }

####### Update the pool amount. If there is not enough space, we perish (exit) 
####### with an exit code of 1. This particular exit code will cause 
####### s4p_repeat_work_order.pl to recycle the input work order (effectively, 
####### sending the work order to the back of the queue to try again later.

        S4P::logger("INFO", "main: pool: [$pool], pool_db: [$pool_db]");
        if ( exists $allocations{$pool} ) {
            $allocations{$pool} += $total_req_size;
        } else {
            $allocations{$pool} = $total_req_size;
        }

####### Substitute key phrase with output file pool location

        if ( /INSERT_DIRECTORY_HERE/ ) {
            $_ =~ s/INSERT_DIRECTORY_HERE/$pool/;
        } elsif (  /INSERT_MULTIDIRECTORY_HERE/ ) {
            my $actual_fn = $fields[1];
            $actual_fn =~ s/:[0-9]+//;
            my $actual_dir = $pool . "/" . $actual_fn;
            $_ =~ s/INSERT_MULTIDIRECTORY_HERE/$actual_dir/;
        }
   }
}

# Now, actually make the disk pool allocations

foreach my $pool ( keys %allocations ) {
    my $total = $allocations{$pool};
    $space = S4P::ResPool::update_pool ($pool, -1*$total, $pool_db);
    if ( $space == 0 ) {
        S4P::logger("ERROR", "main: Insufficient disk space in pool $pool which requires: $total.");
    
####### Undo any allocations already completed to get us back where 
####### we started

        foreach my $allocation (@undo_allocate) {
            my ($pool_allocated, $size_allocated) = split(/\|/, $allocation);
            S4P::logger("DEBUG", "main: Undoing allocation of $size_allocated bytes for $pool_allocated.");
            my $undo_space = S4P::ResPool::update_pool($pool_allocated, $size_allocated, $pool_db);
            S4P::logger("DEBUG", "main: Successfully released $undo_space bytes.");
        }
        S4P::perish(1, "main: Exiting with a 1 to trigger a recycling of the input work order.");
    } else {
        S4P::logger ("INFO", "main: $total bytes allocated in $pool.");
        push(@undo_allocate, "$pool|$total");
    }
}

# Save the records back into a string to be written to the output work order.

foreach (@pcf_lines) {
    $pcf_string = ($pcf_string . "$_\n");
}

##  Create RUN_* PCF work order 

my @wofields = split (/\./,$work_order);
my @pge = split (/_/, $wofields[1], 2);
$out_work_order = "RUN_$pge[1].$wofields[2].pcf";
my $iret = S4P::write_file($out_work_order,$pcf_string);

exit 0;
