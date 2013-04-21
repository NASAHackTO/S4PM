#!/usr/bin/perl

=head1 NAME

s4pm_delete_expired_data.pl - delete data files that have expired

=head1 SYNOPSIS

s4pm_delete_expired_data.pl
B<-c> I<config_file>
B<-a> I<allocate_disk_cfg>

=head1 DESCRIPTION

This script, meant to run periodically like a cron job, deletes data files 
that have been resident on the S4PM file system beyond a configurable time.
The configuration file specified with the -c argument contains the age 
limits (in hours) for each data type beyond which this script will add the 
particular file to an UPDATE work order so that it will be deleted. The 
UPDATE work orders are sent to Gran Central which, after seeing the uses 
knocked down to a large negative number (-10000), will send a SWEEP work 
order to the Sweep Data station. It is here that the files are actually deleted 
from the file system.

Since the B<s4pm_delete_expired_data.pl> also needs to know the location of 
files of a particular data type, it uses the Allocate Disk configuration file, 
specified with the -a argument. Both these files are assumed to be in the 
station directory (or at least, the links to them).

=head1 AUTHOR

Stephen Berrick

=head1 CREATED

02/07/2001

=cut

################################################################################
# s4pm_delete_expired_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use S4P;
use Safe;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw(
            %DataLocator
            $DeleteFlag
            $opt_c
            $opt_a
           );

################################################################################

# Parse command line arugments

getopts('c:a:');

unless ( $opt_c ) {
    S4P::perish(30, "No configuration file specified with the -c argument.");
}
unless ( $opt_a ) {
    S4P::perish(30, "No Allocate Disk configuration file specified with the -a argument.");
}

my $CleanCfg    = $opt_c;
my $AllocDiskCfg = $opt_a;

$DeleteFlag = 0;

S4P::logger("INFO", "********** s4pm_delete_expired_data.pl starting **********");

# Read in configuration files

my $compartment1 = new Safe 'SWEEP';
$compartment1->share('%AgeLimits');
$compartment1->rdo($CleanCfg) or
    S4P::perish(30, "main: Failed to read in configuration file $CleanCfg in safe mode: $!");
my $compartment2 = new Safe 'ALLOC';
$compartment2->share('%datatype_pool_map', '%datatype_pool');
$compartment2->rdo($AllocDiskCfg) or
    S4P::perish(30, "main: Failed to read in configuration file $AllocDiskCfg in safe mode: $!");


# Create an output UPDATE work order that will contain files that need to be
# deleted

my $OutputWorkorder = "UPDATE.EXPIRE_" . "$$.wo";

open(OUT, ">$OutputWorkorder") or 
    S4P::perish(110, "main: Cannot open output work order: $OutputWorkorder: $!");

foreach my $datatype ( keys %ALLOC::datatype_pool_map ) {

    my $pool = $ALLOC::datatype_pool_map{$datatype};
    my $dir = $ALLOC::datatype_pool{$pool};

### Verify that an age limit has been specified for this data type. If not, 
### print out an error message and then skip it.

    unless ( exists $SWEEP::AgeLimits{$datatype} ) {
        S4P::logger("ERROR", "main: Data type $datatype does not exist in \%AgeLimits hash. Skipping.");
        next;	# Move on to next data type
    } else {    # Continue on
        S4P::logger("INFO", "main: Age limit on data type: $datatype is configured as " . $SWEEP::AgeLimits{$datatype} . " hours.");
    }

    opendir(DATA, $dir) or 
        S4P::perish(120, "main: Cannot opendir: $dir:$!");
    while ( defined( my $file = readdir(DATA) ) ) {
        next if ( $file eq "." or $file eq "..");
        next if ( $file =~ /\.ur$/ or $file =~ /\.met$/ );	# Skip .ur and .met files
        next if ( $file !~ /^$datatype/ );	# Skip if it's not the datatype
						# we're working on right now

        my $fullpath = "$dir/$file";

####### lstat function returns 13 elements, but we only care about the 10th
####### one, mtime. So, we ignore the rest by setting them to 'undef'
####### We use lstat rather than stat since in Datapool polling configuration,
####### we have links to files rather than files themselves and stat won't work.

        my (undef, undef, undef, undef, undef, undef, undef, undef, undef, $mtime,
            undef, undef, undef) = lstat($fullpath);
        my $DiffInSeconds = time() - $mtime;
        S4P::logger("DEBUG", "main: mtime of file: $fullpath is $mtime");
        S4P::logger("DEBUG", "main: This means file is $DiffInSeconds seconds old.");

        if ( $mtime eq undef ) {
            S4P::logger("ERROR", "main: mtime for file: $fullpath was undefined. Skipping...");
            next;
        }
        if ( $DiffInSeconds < 0 ) {
            S4P::logger("ERROR", "main: mtime is somehow LATER than current time for file: $fullpath. Skipping...");
            next;
        }
        
        if ( $DiffInSeconds > ($SWEEP::AgeLimits{$datatype} * 3600) ) {
            S4P::logger("INFO", "main: $fullpath is older than " . $SWEEP::AgeLimits{$datatype} . " hours. Adding it to UPDATE work order for deletion.");
            print OUT "FileId=" . "$fullpath Uses=-10000\n";
            $DeleteFlag = 1;
        } else {
            S4P::logger("DEBUG", "main: $fullpath is younger than " . $SWEEP::AgeLimits{$datatype} . " hours. It will NOT be deleted.");
        }
    }
    closedir DATA or
        S4P::perish(121, "main: Cannot closedir: $!");
}

close OUT or 
    S4P::perish(112, "main: Cannot close output work order: $OutputWorkorder: $!");

unless ( $DeleteFlag ) {
    unlink($OutputWorkorder) or S4P::perish(70, "main: Cannot delete empty UPDATE work order: $OutputWorkorder: $!");
}

S4P::logger("INFO", "********** s4pm_delete_expired_data.pl completed successfully! **********");
