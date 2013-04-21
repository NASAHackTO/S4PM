#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_find_data.pl - Find or wait for requested data for a particular algorithm

=head1 SYNOPSIS

s4pm_find_data.pl 
B<-a[lloc]> I<allocate_disk_cfg>
B<-p[ge]> I<algorithm>
[B<-c[onfig]> I<find_data_cfg>]
[B<-l[og]>]
[B<-p[olling]> I<polling_in_secs>
[B<-r[ecycle]> I<timer>]
[B<-s[mart]>]
I<FIND_workorder>

=head1 DESCRIPTION

The main function of the B<Find Data> station is to locate files
requested in input FIND work orders and create an output PREPARE
work orders containing actual file names and directory locations.
B<Find Data> searches the local filesystem to determine what
files exist.

The filled in output work orders are of type PREPARE and are sent to the 
B<Prepare Run> station.

B<s4pm_find_data.pl> works by first examining the file groups corresponding 
to the required inputs only. If any one of these cannot be found within the 
time specified by the timers, then B<s4pm_find_data.pl> will fail.

Once all required input has been located, the optional file groups are 
examined next. Optional file groups are first grouped by LUN or the logical
unit number specified and used within the process control file when the 
associated algorithm is run. For each LUN, B<s4pm_find_data.pl> will look for 
first choice files (those where the need is set to OPT1). If not found within
the time specified by the timer for that file group, the second choice
file (if any) is looked for. This continues until a choice has been found 
for that LUN or the timers have expired. Then, the next LUN and its 
file groups are searched for, etc. If any or all optional files are not found, 
B<s4pm_find_data.pl> will continue without them and produce a viable output
work order for the B<Prepare Run> station.

In checking for optional input files, B<s4pm_find_data.pl> will continue to look
for more desired options (those with a smaller option number) and if found, 
these files will supersede the less desired options. For example, if while 
looking for a OPT3 file the OPT1 shows up, the OPT1 file will be seen 
and used.

For optional inputs only, the station will also release the job if a 
file named 'IGNORE_OPTIONAL' is detected in the job directory. This 
effectively causes the station to expire all optional timers and look 
for all optional files one final timE. This can be useful if the job 
is waiting on optional Inputs that we know will never arrive. This only 
works for optional inputs, not required.

For both optional and required inputs, if a file named 
'EXPIRE_CURRENT_TIMERS' is detected in the job directory, only the current 
timer on the input will be expired. The search will continue with the next 
alternalte (for required) or optional input. If there are no other alternate
or optional inputs, the job will be released.

For required inputs only, if a file named 'IGNORE_REQUIRED' is detected in 
the job directory, the job is released unconditionally after checking
one last time for all required inputs. The algorithm is assumed to be able to
handle this situation.

The B<Find Data> station uses an optional configuration file to set the 
File system polling frequency. A polling frequency supplied on the command
line with the argument B<-p> overrides any found in a configuration file.
The polling frequency defaults to 30 seconds if not specified in either
place. If it exists, the configuration file is assumed to be named
I<s4pm_find_data.cfg> and located in the station directory (or, at least a 
link to it). This can be overridden with the B<-config> option in which 
the full path to the configuration file is specified.

The B<-log> causes B<Find Data> to write a single line for each run in
the log file specified after the option. This line is in a parameter=value
format and contains: algorithm name, data time, production time, and the 
currency for each input, both required and optional, for the data found by 
B<Find Data>.

=head1 ARGUMENTS

=over 4

=item B<-a[lloc]> I<allocate_disk_cfg>

This is the full pathname of the Allocate Disk configuation file.

=item B<-p[ge]> I<algorithm>

This is the name of the algorithm.

=item B<-c[onfig]> I<find_data_cfg>

This is the full pathname of the Find Data configuration file. This argument
is optional. See CONFIGURATION FILE.

=item B<-l[og]> 

If this option is set, all transactions by Find Data are logged in a log file
named find_data.log.

=item B<-p[olling]> I<polling_in_secs>

This is the polling interval in seconds. It may also be set in the Find
Data configuration file with the B<-c[onfig]> option. The default is 30
seconds.

=item B<-r[ecycle]> I<timer>

This option needs to be used with s4p_repeat_work_order.pl. When invoked,
the job will ignore the wait timers specified in the algorithm configuration
files and instead wait only the number of seconds specified with I<timer>.
After that time is up, the job will exit with a 1 and it is assumed that the 
s4p_repeat_work_order.pl will recycle the job.

=item B<-s[mart]>

This option invokes "smart" polling of the file system. This allows polling
of the file system to be frequent in the beginning and then slow up as the
time since the last data file was found grows longer. The idea behind this
is to reduce thrashing the disk when, evidently, data are not arriving as
planned. Regardless of the file system polling frequency, the polling for
signals such as 'Ignore Optional' are carried out on a fixed frequency
set via the -polling option, via a configuration file, or the default of
30 seconds. 

Note that invoking any of the manual overrides such as 'Ignore Optional'
resets the clock: its treated as if a data file was found.

To use smart polling, two parameters may be included in the Find Data 
configuration file: @cfg_intervals and @cfg_freqs. The first specifies the 
intervals in seconds when a new polling frequency is to be read from the 
@cfg_freqs array and applied. If @cfg_intervals and @cfg_freqs are not set, 
these default values are used:

@cfg_intervals = (300, 3600, 21600, 86400, 432000);
@cfg_freqs     = (30, 300, 1800, 7200, 43200, 86400);

The intepretation of these default settings is as follows: if the time (in
seconds) since a data file was found is less than 300, the polling frequency
of the file system will be every 30 seconds; if the time since the last
data file was found is greater than 300 seconds, but less than 3600 seconds,
the polling frequency will be every 300 seconds; etc. Note that there needs
to be one more element in the @cfg_freqs array than in the @cfg_intervals.
This is the last frequency to apply. In this case, if the time since the 
last data file was found exceeds 5 days (432000 seconds), the frequency of
polling will be once every day. The polling won't slow down any further.

=back

=head1 CONFIGURATION FILE

A number of parameters can be specified in a configuration file which is
passed to Find Data via the B<-c[onfig]> I<config_file> option. For details
on these options, refer to the s4pm_find_data.cfg man page.

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_find_data.pl,v 1.13 2006/12/06 14:41:39 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use File::Basename;
use S4P::PDR;
use S4P;
use S4PM;
use S4PM::Handles;
use S4P::TimeTools;
use S4P::FileSpec;
use Getopt::Long;
use Safe;
require 5.6.0;
use strict;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw(
            $INPUTWORKORDER
            @OUTPUTFILEGROUPS
            %OPTIONALGROUPSHASH
            %REQUIREDGROUPSHASH
            $INPUTPDR
            $CONFIGFILE
            $LOG
            $LOGSTR
            $POLLING
            $LAST_HIT_TIME
            $CLOCK
            $PSPEC
            $ALLOCCFG
            $RECYCLE
            $SMART
            $IGNORE_OPTIONAL
            $PGE
            %datatype_pool
            %datatype_pool_map
            %datatype_maxsize
            %proxy_esdt_map
           );

# PSPEC is set if the trigger data type is a PSPEC file which
# means we're dealing with on-demand processing.

$PSPEC = undef;

# $CONFIGFILE is set only if -config is invoked on the command line

$CONFIGFILE = undef;

# $LOG is an option that allows logging of s4pm_find_data activity

$LOG = undef;

# $POLLING is the default polling frequency in seconds if a value is not
# provided in the Find Data configuration and not passed via the -polling 
# option.

$POLLING = undef;

$ALLOCCFG = undef;

$CLOCK = time();

# $LAST_HIT_TIME is the elapsed time in seconds since the last optional or
# required input file was found.

$LAST_HIT_TIME = $CLOCK;

$RECYCLE = undef;

# $SMART flags whether or not smart polling has been invoked on the command line

$SMART= undef;

# $IGNORE_OPTIONAL flags whether or not a IGNORE_OPTIONAL signal file has been
# dropped into the running job directory.

$IGNORE_OPTIONAL = 0;

# $PGE is the name of the algorithm

$PGE = undef;

################################################################################

my $OutputWorkorder;	# File name of the output work order

# Polling frequency in seconds if no configuration file is specified AND if
# a Find Data configuration file does not exist AND polling was not set as a 
# command line argument.

my $polling_default = 30;

GetOptions( "config=s"  => \$CONFIGFILE,
            "alloc=s"   => \$ALLOCCFG,
            "log=s"     => \$LOG,
            "polling=s" => \$POLLING,
            "recycle=s" => \$RECYCLE,
            "smart"     => \$SMART,
            "pge=s"     => \$PGE,
          );

unless ( $PGE ) {
    S4P::perish(30, "main: No algorithm specified with the -pge argument.");
}

if ( $LOG ) {
    if ( -e $LOG ) {
        S4P::logger("INFO", "main: Opening log file: $LOG");
        open(LOG, ">>$LOG" ) or S4P::perish(110, "main: Failed to open log file: $LOG: $!");
    } else {
        S4P::logger("INFO", "main: Opening new log file: $LOG");
        open(LOG, ">$LOG" ) or S4P::perish(110, "main: Failed to open log file: $LOG: $!");
    }
}

# Read in the Allocate Disk configuration file

if ( $ALLOCCFG ) {
    my $alloc_cpt = new Safe 'ALLOC';
    $alloc_cpt->share('$cfg_polling_interval', '@cfg_intervals', '@cfg_freqs', '%cfg_custom_find');
    $alloc_cpt->rdo($ALLOCCFG) or S4P::perish(102, "main: Failed to import allocation configuration file parameters: $ALLOCCFG: $!");
}
else {
    S4P::perish(30, "main: No Allocate Disk configuration file was set with the -alloc argument.");
}

if ( $POLLING ) {
    $main::cfg_polling_interval = $POLLING;
    if ( $main::cfg_polling_interval < 0 or $main::cfg_polling_interval > 1000 ) {
        S4P::perish(33, "main: Invalid polling frequency: $main::cfg_polling_interval. Values must be > 0 and less than 1000 seconds.");
    } else {
        S4P::logger("INFO", "main: Polling frequency for signal files was set by command line option to $main::cfg_polling_interval seconds.");
    }
} elsif ( $CONFIGFILE ) {
    my @def_intervals = (300, 3600, 21600, 86400, 432000);
    my @def_freqs     = (30, 300, 1800, 7200, 43200, 86400);

    my $compartment = new Safe 'CFG';
    $compartment->share('$cfg_polling_interval', '@cfg_intervals', '@cfg_freqs', '%cfg_custom_find');
    $compartment->rdo($CONFIGFILE) or S4P::perish(102, "main: Failed to import configuration file parameters: $CONFIGFILE: $!");
    unless ( scalar(@main::cfg_intervals) > 0 and scalar(@main::cfg_freqs) > 0 ) {
        @main::cfg_intervals = @def_intervals;
        @main::cfg_freqs = @def_freqs;
    }
    if ( $RECYCLE ) { 
        S4P::logger("INFO", "main: Starting s4pm_find_data.pl in RECYCLE mode with a timer of $RECYCLE seconds and with this configuration file: $CONFIGFILE");
    } else {
        S4P::logger("INFO", "main: Starting s4pm_find_data.pl with this configuration file: $CONFIGFILE");
    }
    S4P::logger("INFO", "main: Polling frequency for signal files was set by configuration file to $main::cfg_polling_interval seconds.");
} else {
    $main::cfg_polling_interval = $polling_default;
    S4P::logger("INFO", "main: Polling frequency for signal files was set to default of $main::cfg_polling_interval seconds.");
}

# Verify that an input work order (last command-line argument) has been
# specified and that it exists. Note that GetOptions (above) will remove
# from ARGV only those arguments it recognizes and leave the remaining in 
# place. Since the input work order is not "recognized", it is guaranteed 
# to be the only remaining argument and will thus be the first: $ARGV[0]
 
if ( !defined $ARGV[0] ) {
    S4P::perish(20, "main: No input work order specified! Must be first argument and file name of the input FIND work order");
} else {
    $INPUTWORKORDER = $ARGV[0];
    unless ( -e $INPUTWORKORDER ) {
        S4P::perish(20, "main: Work order $INPUTWORKORDER doesn't seem to exist!");
    }
    if ( $INPUTWORKORDER =~ /^DO\.(.+)_$PGE\./ ) {
        $OutputWorkorder = $INPUTWORKORDER;
        $OutputWorkorder =~ s/$1/PREPARE/;
        $OutputWorkorder =~ s/^DO\.//;
        $OutputWorkorder .= ".wo";
    } else {
        S4P::perish(20, "main: Input work order name: [$INPUTWORKORDER] could not be parsed!");
    }
}

S4P::logger("INFO", "********** s4pm_find_data.pl starting up for algorithm $PGE **********");

################################################################################

@OUTPUTFILEGROUPS = ();   # Array to contain filled-out file groups for the
			  # output work order

# Read in the input work order

my $INPUTPDR = new S4P::PDR('text' => S4P::read_file($INPUTWORKORDER));

# First, round the processing start and stop times from the input PDR to a
# boundary commensurate with the temporal coverage of the trigger data type.
# This is done because there may have been a leap second correction applied
# to the start and stop times in the input PDR (e.g. for AIRS), and we don't
# want these to interfer with how we construct file name search patterns for 
# the remaining input needed.

my $rounded_process_start = S4P::TimeTools::CCSDSa_DateRound($INPUTPDR->processing_start, 60);
my $rounded_process_stop  = S4P::TimeTools::CCSDSa_DateRound($INPUTPDR->processing_stop, 60);

if ( $LOG ) {
    my $ts = S4P::timestamp();
    $ts =~ s/\s/T/;
    $LOGSTR = "PGE=$PGE DATADATE=$rounded_process_start PRODDATE=$ts ";
}

# Set aside an array for all the file groups from the input work order

my @input_file_groups = @{$INPUTPDR->file_groups};

# Do some basic sanity checking on the input work order file groups (TBD)

validate_pdr(@input_file_groups);

$PSPEC = is_on_demand(@input_file_groups);
if ( $PSPEC ) {
    S4P::logger("INFO", "main: PSPEC file detected. I'm running in an on-demand processing scenario.");
} else {
    S4P::logger("INFO", "main: No PSPEC file detected. I'm running in a standard processing scenario.");
}

# If in DEBUG mode, print out all of the input file groups found

S4P::logger("DEBUG", "main: ORIGINAL INPUT FILE GROUPS FOUND:\n" . S4P::PDR::show_file_groups(@input_file_groups) );

# Separate into arrays the required file groups from the optional file groups
# and if in DEBUG mode, print them out. Bear in mind that there may be no
# optional file groups.

my @requiredGroups = get_required_groups(@input_file_groups);
S4P::logger("DEBUG", "main: JUST THE REQUIRED FILE GROUPS FOUND:\n" . S4P::PDR::show_file_groups(@requiredGroups) );
my @optionalGroups = get_optional_groups(@input_file_groups);
if ( scalar(@optionalGroups) > 0 ) {
    S4P::logger("DEBUG", "main: OPTIONAL FILE GROUPS FOUND:\n" . S4P::PDR::show_file_groups(@optionalGroups) );
}

# Now create hashes for each the required file groups and the optional
# file groups (if it exists) such that the hash keys are LUNs and the hash 
# values are arrays containing the file groups associated with that LUN. 
# Further, if in DEBUG mode, print out these new hashes.

%REQUIREDGROUPSHASH = get_groups_by_lun(@requiredGroups);
S4P::logger("DEBUG", "main: REQUIRED FILE GROUPS HASH (unsorted):\n\n");
foreach my $key ( keys %REQUIREDGROUPSHASH ) {
    S4P::logger("DEBUG", "main: LUN: $key:\n\n");
    S4P::logger("DEBUG", S4P::PDR::show_file_groups(@{$REQUIREDGROUPSHASH{$key}}) );
}
if ( scalar(@optionalGroups) > 0 ) {
    %OPTIONALGROUPSHASH = get_groups_by_lun(@optionalGroups);
    S4P::logger("DEBUG", "main: OPTIONAL FILE GROUPS HASH (unsorted):\n\n");
    foreach my $key ( keys %OPTIONALGROUPSHASH ) {
        S4P::logger("DEBUG", "main: LUN: $key:\n\n");
        S4P::logger("DEBUG", S4P::PDR::show_file_groups(@{$OPTIONALGROUPSHASH{$key}}) );
    }
}

### We don't need to do the following sorts now, but it is done here for
### debug purposes. This type of sort reorders the hashes made above by 
### the hash keys. The sort order results in having the LUN with the 
### shortest final timer be first, and the LUN with the next shortest final 
### timer be second, etc.

S4P::logger("DEBUG", "main: SORTED REQUIRED FILE GROUPS HASH:\n\n");
foreach my $key ( sort by_final_required_timer keys %REQUIREDGROUPSHASH ) {
    S4P::logger("DEBUG", "main: LUN: $key:\n\n");
    S4P::logger("DEBUG", S4P::PDR::show_file_groups(@{$REQUIREDGROUPSHASH{$key}}) );
}

if ( scalar(@optionalGroups) > 0 ) {
    S4P::logger("DEBUG", "main: SORTED OPTIONAL FILE GROUPS HASH:\n\n");
    foreach my $key ( sort by_final_optional_timer keys %OPTIONALGROUPSHASH ) {
        S4P::logger("DEBUG", "main: LUN: $key:\n\n");
        S4P::logger("DEBUG", S4P::PDR::show_file_groups(@{$OPTIONALGROUPSHASH{$key}}) );
    }
}

# Now fill in the missing information for all the required file
# groups. If all required input information cannot be retrieved,
# then we fail.

my $res = fill_in_required_file_groups($main::cfg_polling_interval, $rounded_process_start, $rounded_process_stop);
if ( $res ) {
    S4P::perish(1, "main: Could not fill in data for all required inputs.");
}

### Fill out the missing information in the optional file groups based on what
### data is available within the timers specified

if ( scalar(@optionalGroups) > 0 ) {
    my $res = fill_in_optional_file_groups($main::cfg_polling_interval, $rounded_process_start, $rounded_process_stop);
    if ( $res ) {
        S4P::logger("WARNING", "main: Could not fill in data for all optional inputs.");
    }
}

# Finally, now that all possible file groups have been written into 
# @OUTPUTFILEGROUPS, set the input PDR's file groups object to this array,
# thus overwriting completely the file groups that were in the input work 
# order. Then, force a recount of the number of file specs in the PDR so that 
# it is guaranteed to be set correctly.

$INPUTPDR->file_groups(\@OUTPUTFILEGROUPS);
$INPUTPDR->recount();
$INPUTPDR->write_pdr("$OutputWorkorder");

print LOG "$LOGSTR\n" if ( $LOG );

# Clean up any leftover signal files

if ( -e "./IGNORE_OPTIONAL" ) {
    unlink("./IGNORE_OPTIONAL");
}
if ( -e "./EXPIRE_CURRENT_TIMER" ) {
    unlink("./EXPIRE_CURRENT_TIMER");
}
if ( -e "./IGNORE_REQUIRED" ) {
    unlink("./IGNORE_REQUIRED");
}

S4P::logger("INFO", "********** s4pm_find_data.pl completed successfully! **********");

sub get_polling {

    my $t = shift;

    if ( $RECYCLE ) {
        return $RECYCLE;
    } elsif ( $SMART ) {
        my $nsteps = scalar(@main::cfg_intervals);
        my $nfreqs = scalar(@main::cfg_freqs);

        unless ( $nfreqs == $nsteps+1 ) {
            S4P::perish(124, "get_polling(): Number of frequencies (\@cfg_freqs) must be equal to one more than the number of intervals (\@cfg_intervals) in the s4pm_find_data.cfg file. ACTION: Modify the s4pm_find_data.cfg file appropriately.");
        }

        for (my $i = 0; $i < $nsteps; $i++) {
            if ( $t < $main::cfg_intervals[$i] ) {
                return $main::cfg_freqs[$i];
            }
        }
        return $main::cfg_freqs[$nsteps+1];
    } else {
        return $main::cfg_polling_interval;
    }

}

sub doze {

    my $flag = shift; # 0=Optional, 1=Required
    my $polling = shift;
    my $fg = shift;

    my $interval = $main::cfg_polling_interval;
    my $dreams = $polling/$interval;
    if ( $SMART == $interval ) { $dreams = 1; }

    my $sigfile;
    my $type;
    if ( $flag == 0 ) {
        $sigfile = 'IGNORE_OPTIONAL';
        $type = 'OPTIONAL';
    } else {
        $sigfile = 'IGNORE_REQUIRED';
        $type = 'REQUIRED';
    }
    

     S4P::logger("INFO", "doze(): Dozing $polling seconds before polling the file system again. Still polling for signal files every $interval seconds.");

    for (my $i; $i < $dreams; $i++) {

        if ( signal_file($sigfile, 1) ) { 
            S4P::snore($interval, "$sigfile signal detected. Doing one final search for " . $fg->data_type . " from " . $fg->data_start . " to " . $fg->data_end . " before calling it quits ...");
            S4P::logger("INFO", "doze(): $sigfile signal detected. Doing one final search for " . $fg->data_type . " from " . $fg->data_start . " to " . $fg->data_end . " before calling it quits ...");
            advance_clock(99999999);
            if ( $sigfile eq 'IGNORE_OPTIONAL' ) { $IGNORE_OPTIONAL = 1; }
            $LAST_HIT_TIME = time();
            return;
        } else {
            S4P::snore($interval, "Awaiting $type (" . $fg->need . ") data file of " . $fg->data_type . " from " . $fg->data_start . " to " . $fg->data_end . " to arrive.\nCurrent Clock: " . current_clock() . "\nGiving up when clock exceeds: " . $fg->timer . ".");
        }

        if ( signal_file('EXPIRE_CURRENT_TIMER', 0) ) { 
            S4P::logger("INFO", "doze(): Detected a EXPIRE_CURRENT_TIMER signal file in run directory. Expiring current timer and moving on to next input.");
            advance_clock($fg->timer);
            $LAST_HIT_TIME = time();
            return;
        }

    }

}
sub is_on_demand  {
  
    my @input_file_groups = @_;

    foreach my $file_group (@input_file_groups) {
        if ( $file_group->data_type eq 'PSPEC' ) {
            return 1;
        }
    }

    return 0;

}


sub get_best_data {

################################################################################
#                              get_best_data                                   #
################################################################################
# PURPOSE: To get the pathname to the best available data for a particular     #
#          LUN                                                                 #
################################################################################
# DESCRIPTION: get_best_data is used to get the best available data for a      #
#              particular LUN among those options possible for that LUN. OPT1  #
#              is preferred over OPT2, OPT2 over OPT3. This subroutine allows  #
#              a OPT2 file to get noticed even after the timer for it has      #
#              expired and the focus is on OPT3.                               #
################################################################################
# RETURN:  $path   The full pathname to the data found                         #
#          $fgs    The particular file group found                             #
################################################################################
# CALLS: get_handle_pathname                                                   #
################################################################################
# CALLED BY: fill_in_optional_file_groups                                      #
################################################################################

    my ($processing_start, $processing_stop, $offset, @fgs) = @_;

    my $len = scalar(@fgs);
    S4P::logger("INFO", "get_best_data(): Looking for best data among $len choices for LUN " . $fgs[0]->lun . " starting with the most desireable.");
    for (my $i = 0; $i < $len; $i++) {
        S4P::logger("INFO", "get_best_data(): Choice: " . ($i+1) . ": Looking for OPTIONAL file of " . $fgs[$i]->data_type . " from " . $fgs[$i]->data_start . " to " . $fgs[$i]->data_end . " with need of " . $fgs[$i]->need . " and currency of " . $fgs[$i]->currency . " to arrive.");
        my $path = get_handle_pathname($fgs[$i], $processing_start, $processing_stop, $offset);
        return ($path, $fgs[$i]) if ( $path );
    }
   
    return;
        
}

sub get_data_name {

################################################################################
#                              get_data_name                                   #
################################################################################
# PURPOSE: To get the pathname to the best available data for a particular     #
#          LUN                                                                 #
################################################################################
# DESCRIPTION: get_data_name returns the file name of the data file by         #
#              looking for the one that has a file type of "SCIENCE". This     #
#              won't work well, however, for multi-file files such as L0.      #
#              In such a case, the file name returned will merely be the first #
#              of possibly several.                                            #
################################################################################
# RETURN:  File name of data file                                              #
################################################################################
# CALLS: none                                                                  #
################################################################################
# CALLED BY: get_handle_pathname                                               #
################################################################################

    my $fg = shift;

    my @file_specs = @{$fg->file_specs};

### Find the science file spec out of the group

    foreach my $fspec ( @file_specs ) {
        if ( $fspec->file_type eq "SCIENCE" ) {
            return $fspec->file_id;
        }
    }

    S4P::perish(35, "get_data_name(): Cannot find any SCIENCE file specs in this file group");
}

sub get_groups_by_lun {

################################################################################
#                            get_groups_by_lun                                 #
################################################################################
# PURPOSE: To create a hash of file groups                                     #
################################################################################
# DESCRIPTION: get_groups_by_lun creates a hash using the array of file groups #
#              provided as input. The hash is organized so that the hash keys  #
#              keys are LUNs or LUNs+currencies (see below) and the hash       #
#              values are arrays of file groups sharing that LUN or            #
#              LUN+currency.                                                   #
#                                                                              #
#              Deciding how to group file groups in the hash that is returned  #
#              is one of the most critical functions of s4pm_find_data.pl since#
#              it determines are production rules are satisfied.               #
#                                                                              #
#              The overriding thing to keep in mind is that for each hash key, #
#              only one found file is needed to satisfy the rule. If we        #
#              need more than one file to satisfy the rule, then we need       #
#              each to have a unique key.                                      #
#                                                                              #
#              The trick is in trying to determine which rule is being         #
#              requested by looking at, for a particular LUN, if data types,   #
#              needs, and currencies are all the same or not.                  #
#                                                                              #
#              These are the situations need to be handled when multple file   #
#              groups share the same LUN:                                      #
#                                                                              #
#              1. The most common is where the ultimate PCF will contain a     #
#              single entry for that LUN (with one or more versions). In this  #
#              case, the multiple file groups represent options for that       #
#              single PCF entry prioritized by preference (e.g OPT1, OPT2,...  #
#              or REQ1, REQ2,...). For example, file groups with:              #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              10999    MOD01        OPT1     CURR                             #
#              10999    MOD02        OPT2     CURR                             #
#              10999    MOD03        OPT3     CURR                             #
#                                                                              #
#              represents a PCF that will contain a current MOD01 or a current #
#              MOD02 or a current MOD03, but not all three. In our hash then,  #
#              we want a single hash key to point to all three file groups     #
#              since only one should satisfy the rule. Therefore, we use the   #
#              LUN as the hash key.                                            #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Unique                                   #
#                         Needs:      Unique                                   #
#                         Currencies: Same                                     #
#                                                                              #
#              2. In a similar case, we have different temporal choices for    #
#              the same data type. For example:                                #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              10999    MOD01        OPT1     CURR                             #
#              10999    MOD01        OPT2     PREV1                            #
#              10999    MOD01        OPT3     PREV2                            #
#                                                                              #
#              represents a PCF that will contain a current MOD01 or a         #
#              previous MOD01 or the previous MOD01 before that. Again, we     #
#              want  a single hash key to point to all three file groups.      #
#              Using the LUN+currency would not work here since the currencies #
#              are not necessarily all different. Therefore, we use the LUN by #
#              itself as the hash key. Note that the needs are all different   #
#              here.                                                           #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Same                                     #
#                         Needs:      Unique                                   #
#                         Currencies: Unique                                   #
#                                                                              #
#              3. The less common case is where the ultimate PCF is meant to   #
#              contain all the entries, not just one of them. For example,     #
#              file groups with:                                               #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              10502    AM1ATTNF     REQ1     CURR                             #
#              10502    AM1ATTNF     REQ1     PREV1                            #
#              10502    AM1ATTNF     REQ1     PREV2                            #
#                                                                              #
#              Here, the intent is that the PCF contain all three files        #
#              (the CURR and the PREV1 and the PREV2), not just the one. Since #
#              we want all three files, we do NOT want to have a single        #
#              hash key point to all three. Instead, we want each associated   #
#              with a unique hash key. This is done by using LUN+currency as   #
#              the hash key. This case is distinguished from the previous case #
#              (case 2) by the fact that the needs are all the same here.      #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Same                                     #
#                         Needs:      Same                                     #
#                         Currencies: Unique                                   #
#                                                                              #
#              4. A combination case involves something like this:             #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              10502    AM1ATTNF     REQ1     CURR                             #
#              10502    AM1ATTN0     REQ2     CURR                             #
#              10502    AM1ATTNF     REQ1     PREV1                            #
#              10502    AM1ATTN0     REQ2     PREV1                            #
#                                                                              #
#              Here, we want two files for LUN 10502, a current one and a      #
#              previous one. But, we allow for one of two choices for each:    #
#              AM1ATTNF is our first choice and AM1ATTN0 is our second.        #
#              For this case, we ultimately want to resolve this to just two   #
#              PCF entries. This means we need two hash keys, one for the CURR #
#              (choice REQ1 and REQ2) and the other for the PREV1 (also,       #
#              REQ1 and REQ2). By using LUN+currency as the hash keys, we do   #
#              end up with 2 unique keys, each pointing to two options.        #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Not unique                               #
#                         Needs:      Not unique                               #
#                         Currencies: Any                                      #
#                                                                              #
#              5. For the nearest file production rule, we end up with this    #
#              siuation in the PDR:                                            #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              10502    AM1ATTNF     REQ1     NPREV1,5                         #
#              10502    AM1ATTNF     REQ2     NPREV1,5                         #
#              10502    AM1ATTNF     REQ3     NPREV1,5                         #
#                                                                              #
#              In the above, B<s4pm_select_data.pl> will maintain the currency #
#              in the form above. We want to end up with a single hash key so  #
#              that only one found file satisfies the rule. Using a key        #
#              of LUN+currency will work for this.                             #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Same                                     #
#                         Needs:      Unique                                   #
#                         Currencies: Same                                     #
#                                                                              #
#              6. A final case is when multiple inputs of the same LUN are     #
#              needed to cover the entire processing period. That is, the      #
#              temporal coverages of the input files is less than the          #
#              processing period. In this case, the eventual PCF will contain  #
#              multiple version of the same LUN. It is B<s4pm_select_data.pl>  #
#              that expands a single request into multiple entries needed to   #
#              fill out the processing period and it does this by tagging the  #
#              currency CURR with a number. For example:                       #
#                                                                              #
#               LUN     Data Type    Need     Currency                         #
#              -----    ---------    -----    --------                         #
#              50000    MOD01        OPT1     CURR1                            #
#              50000    MOD01        OPT1     CURR2                            #
#              50000    MOD01        OPT1     CURR3                            #
#                                                                              #
#              Here, we want each to have its own key in the hash since each   #
#              must end up in the PCF as a seperate entry (albeit with the     #
#              same LUN). Since B<s4pm_select_data.pl> uniquely tags each      #
#              'CURR' with a number, we will end up with a unique LUN+currency #
#              key for each item, which is exactly what we want.               #
#                                                                              #
#              Signature: LUNs:       Same                                     #
#                         Data Types: Same                                     #
#                         Needs:      Same                                     #
#                         Currencies: Unique                                   #
#                                                                              #
#    Below is a truth-like table showing when we want to have one key or       #
#    multiple keys for file groups sharing the same LUN (obviously, the LUN    #
#    is the same in all of them).                                              #
#                                                                              #
#    In the cases where we desire a single hash key per file group set, we use #
#    LUN+currency. In cases where we desire multiple hash keys per file group  #
#    set, we use LUN by itself as the hash key. We can detect the signature    #
#    for each of these cases by examining where the data types, needs, and     #
#    currencies are the same (S), unique (U), or mixed (M) among the file      #
#    groups sharing a LUN. Same means that all elements are the same; unique   #
#    means that all are unique, and mixed means that at least some of the      #
#    elements are the same as others.                                          #
#                                                                              #
#    Case    LUN    DataType    Need    Currency    Desired Hash Keys          #
#    ----    ---    --------    ----    --------    -----------------          #
#     1       S        U         U         S         One (LUN only)            #
#     2       S        S         U         U         One (LUN only)            #
#     3       S        S         S         U         Multiple (LUN+currency)   #
#     4       S        M         M         M         Multiple (LUN+currency)   #
#     5       S        S         U         S         One (LUN only)            #
#     6       S        S         S         U         Multiple (LUN+currency)   #
#                                                                              #
#    Legend: S=Same, U=Unique, M=Mixed                                         #
#                                                                              #
################################################################################
# RETURN: %LUN - The hash of LUNs                                              #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my @fg = @_;

    my %LUN = ();       # Hash where each LUN key points to an array of
                        # file groups
    my %tmp = ();

    S4P::logger("DEBUG", "get_groups_by_lun(): Entering get_groups_by_lun()");

# First pass, set the hash key to just the LUN

    foreach my $fg (@fg) {
        my $key = $fg->lun;
        push(@{$tmp{$key}}, $fg);
    }

# Second pass, go through hash and decide which file groups need to be
# seperated out into unique hash keys

    foreach my $key ( keys %tmp ) {
        my @luns = ();
        my @datatypes = ();
        my @needs = ();
        my @currencies = ();
        foreach my $fg ( @{$tmp{$key}} ) {
            push(@luns, $fg->lun);
            push(@datatypes, $fg->data_type);
            push(@needs, $fg->need);
            push(@currencies, $fg->currency);
        }

####### Here we test for the cases where we want the hash keys to be
####### LUN+currency (see prolog).

        if ( uniqueness(@datatypes)  != 1 and
             uniqueness(@needs)      != 1 and
             uniqueness(@currencies) != -1 ) {
            S4P::logger("DEBUG", "get_groups_by_lun(): Data Types: @datatypes Needs: @needs Currencies: @currencies" . " This means using LUN+currency as hash key.");
            foreach my $fg ( @{$tmp{$key}} ) {
                my $id = $fg->lun . "_" . $fg->currency;
                push(@{$LUN{$id}}, $fg);
            }
        } else {
            S4P::logger("DEBUG", "get_groups_by_lun(): Data Types: @datatypes Needs: @needs Currencies: @currencies" . " This means using only LUN as hash key.");
            foreach my $fg ( @{$tmp{$key}} ) {
                my $id = $fg->lun;
                push(@{$LUN{$id}}, $fg);
            }
        }
    }

    S4P::logger("DEBUG", "get_groups_by_lun(): Leaving get_groups_by_lun()");

    return %LUN;
}


sub fill_in_optional_file_groups {

################################################################################
#                       fill_in_optional_file_groups                           #
################################################################################
# PURPOSE: To fill in the missing information for optional file groups if      #
#          data is available                                                   #
################################################################################
# DESCRIPTION: fill_in_optional_file_groups is the main routine for the        #
#              handling of optional inputs. Data requests for each LUN are     #
#              examined in turn starting with the LUN whose final option has   #
#              the shortest timer. The interval for polling of available data  #
#              is set by $interval, the input to this routine. As timers are   #
#              expired, the routine moves to searching for the next most       #
#              desired data type, and so on until all timers for that LUN have #
#              been expired. Then, the next LUN is examined, and so on. When a #
#              desired data type is found, the missing parts of that file      #
#              group are filled out with the file name, directory location,    #
#              file size, and UR.                                              #
#                                                                              #
#              When an optional file group is found, that filled out file      #
#              group is pushed into the global array: @OUTPUTFILEGROUPS        #
################################################################################
# RETURN: 0 - Success                                                          #
#         1 - Failure                                                          #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        by_final_optional_timer                                               #
#        get_handle_pathname                                                   #
#        S4P::snore                                                            #
#        mk_array_of_multifile_specs                                           #
#        mk_array_of_file_specs                                                #
#        S4P::PDR::show_file_groups                                            #
#        S4P::PDR::show_file_specs                                             #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($interval, $processing_start, $processing_stop) = @_;

    S4P::logger("DEBUG", "fill_in_optional_file_groups(): Entering fill_in_optional_file_groups()");

    my $timer;		# timer value for a particular file group
    my $found_fg;	# a file group whose data has been located
    my $not_found_fg;	# a file group whose data has not been located
    my $fg;		# a generic file group
    my $handle;		# pathname to handle file (if found)
    my $fs;		# a generic file spec
    my @fs;		# an array of file specs
    my $ur;		# a Universal Reference or UR from the ECS
    my $isMulti = 0;    # is file a multi-file file?

### Examine each key (LUN or LUN+currency) in the %OPTIONALGROUPSHASH ordered 
### by the timer value on the last option (the least desired option)

    foreach my $lun (sort by_final_optional_timer keys %OPTIONALGROUPSHASH) {

####### Within this LUN, look at all the file groups associated with it. 
####### Look at OPT1 first, then OPT2, etc.

        my @fg_history = ();
FG:     foreach $fg ( sort by_option @{$OPTIONALGROUPSHASH{$lun}} ) {
      
            push(@fg_history, $fg);

            S4P::logger("INFO", "fill_in_optional_file_groups(): **************** Starting New Search ****************");
            S4P::logger("INFO", "fill_in_optional_file_groups(): CURRENT SEARCH: Data type: " . $fg->data_type .
                ", Start: " . $fg->data_start .
                ", End: " . $fg->data_end .
                ", Currency: " . $fg->currency .
                ", Need: " . $fg->need .
                ", Timer: " . $fg->timer
            );
            if ( $fg->timer <= 0 ) {
                S4P::logger("INFO", "fill_in_optional_file_groups(): Timer is less than or equal to zero. No wait.");
            }

########### Big do loop. As long as we haven't exceeded the timer, we poll
########### for the desired file every $interval seconds. In between, we
########### sleep.

            do {
                ($handle, $found_fg) = get_best_data($processing_start, $processing_stop, $INPUTPDR->post_processing_offset, @fg_history);
                S4P::logger("DEBUG", "fill_in_optional_file_groups: Absolute current clock value: " . time);
                S4P::logger("INFO", "fill_in_optional_file_groups(): CURRENT CLOCK: " . current_clock());
                my $hit_elapsed_time = time() - $LAST_HIT_TIME;

############### If IGNORE_OPTIONAL has been set, than all pollings are zero

                my $polling = ( $IGNORE_OPTIONAL ) ? 0 : get_polling($hit_elapsed_time);
                S4P::logger("INFO", "fill_in_optional_file_groups(): Elapsed time since last hit: $hit_elapsed_time seconds. Polling frequency: every $polling seconds.");
                if (! $handle) {
                    doze(0, $polling, $fg);
                }
            } while (! $handle and (current_clock() < $fg->timer) );

            if ( $handle ) {
                if ( S4PM::Handles::is_multi_file_granule($handle) ) {
                    $isMulti = 1;
                } else {
                    $isMulti = 0;
                }
                last;
            }

            if ( ! $handle ) {
                $not_found_fg = $fg;
                S4P::logger("INFO", "fill_in_optional_file_groups(): Timed out on waiting for optional data type " . $not_found_fg->data_type . " from " . $not_found_fg->data_start . " to " . $not_found_fg->data_end . " with a need of " . $not_found_fg->need . "\nTrying next choice for this LUN (if there is one)...");
                next FG;	# The label seems to be needed, athough I believe it shouldn't be
            } 
        }

####### Path found:  set it in the file_group and add it to the output file
####### groups array

        if ($handle) {
    
            S4P::logger("INFO", "fill_in_optional_file_groups(): FILE WAS FOUND!!!");
            $LAST_HIT_TIME = time();
            $LOGSTR .= $found_fg->data_type . "=" . $found_fg->currency . " ";

            if ( $isMulti or is_multifile_filegroup($found_fg) ) {
                S4P::logger("DEBUG", "fill_in_optional_file_groups(): This is a mult-file file.");

                my @fs = mk_array_of_multifile_specs($handle);
                if ( @fs eq undef ) {
                    S4P::logger("FATAL", "fill_in_optional_file_groups(): Could not create file specs for " . $found_fg->data_type);
                    return 1;
                }
                S4P::logger("DEBUG", $found_fg->data_type . " File Specs:\n" . S4P::PDR::show_file_specs(@fs) );
                $found_fg->file_specs(\@fs);
            } else {
                S4P::logger("DEBUG", "fill_in_optional_file_groups(): This is a single file file.");
                my @fs = mk_array_of_file_specs($handle);
                if ( @fs eq undef ) {
                    S4P::logger("FATAL", "fill_in_optional_file_groups(): Could not create file specs for " . $found_fg->data_type);
                    return 1;
                }
                S4P::logger("DEBUG", "fill_in_optional_file_groups(): Generic File Specs:\n" . S4P::PDR::show_file_specs(@fs) );
                $found_fg->file_specs(\@fs);
            }
            $ur = S4PM::Handles::get_ur_from_handle($handle);
            unless ($ur) {
                S4P::logger("FATAL", "fill_in_optional_file_groups(): No UR found in UR file corresponding to " . $found_fg->data_type . " data directory: $handle" . "A proper UR in a UR file begins with 'LGID:' or 'UR:' or 'FakeLGID:' or 'ftp'.");
                return 1;
            }
            S4P::logger("DEBUG", "fill_in_optional_file_groups(): " . $found_fg->data_type . " file found! Touching $handle file.");
            my $res = system(touch, "$handle");
            if ( $res ) {
                S4P::logger("WARNING", "fill_in_optional_file_groups(): Touch failed on $handle");
            }
            $found_fg->ur($ur);
            S4P::PDR::show_file_groups($found_fg);

            my $fg_exist = 0;
            foreach my $existing_fg (@OUTPUTFILEGROUPS) {
                if ($existing_fg->ur eq $ur) {
                    $fg_exist = 1;
                    last;
                }
            }
            unless ($fg_exist) {push (@OUTPUTFILEGROUPS, $found_fg);}

        } else {

            $LOGSTR .= $not_found_fg->data_type . "=NONE ";

        }
    }
 
    S4P::logger("INFO", "**************** Search for OPTIONAL input completed ****************");
    S4P::logger("DEBUG", "fill_in_optional_file_groups(): Leaving fill_in_optional_file_groups()");

    return 0;

}

sub fill_in_required_file_groups {

################################################################################
#                       fill_in_required_file_groups                           #
################################################################################
# PURPOSE: To fill in the missing information for required file groups if      #
#          data is available                                                   #
################################################################################
# DESCRIPTION: fill_in_required_file_groups is the main routine for the        #
#              handling of required inputs. Data requests for each LUN are     #
#              examined in turn starting with the LUN whose final choice has   #
#              the shortest timer. The interval for polling of available data  #
#              is set by $interval, the input to this routine. As timers are   #
#              expired, the routine moves to searching for the next most       #
#              desired data type, and so on until all timers for that LUN have #
#              been expired. Then, the next LUN is examined, and so on. When a #
#              desired data type is found, the missing parts of that file      #
#              group are filled out with the file name, directory location,    #
#              file size, and UR.                                              #
#                                                                              #
#              When a required file group is found, that filled out file       #
#              group is pushed into the global array: @OUTPUTFILEGROUPS        #
################################################################################
# RETURN: 0 - Success                                                          #
#         1 - Failure                                                          #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        by_final_required_timer                                               #
#        get_handle_pathname                                                   #
#        S4P::snore                                                            #
#        mk_array_of_multifile_specs                                           #
#        mk_array_of_file_specs                                                #
#        S4P::PDR::show_file_groups                                            #
#        S4P::PDR::show_file_specs                                             #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($interval, $processing_start, $processing_stop) = @_;

    S4P::logger("DEBUG", "fill_in_required_file_groups(): Entering fill_in_required_file_groups()");
    S4P::logger("INFO", "**************** Beginning search for REQUIRED input ****************");

    my $timer;		# timer value for a particular file group
    my $found_fg;	# a file group whose data has been located
    my $not_found_fg;	# a file group whose data has not been located
    my $fg;		# a generic file group
    my $handle;		# pathname to handle file (if found)
    my $fs;		# a generic file spec
    my @fs;		# an array of file specs
    my $ur;		# a Universal Reference or UR from the ECS
    my $isMulti = 0;    # is file a multi-file file?

### Examine each key (LUN or LUN+currency) in the %REQUIREDGROUPSHASH ordered 
### by the timer value on the last option (the least desired option)

LUN:foreach my $lun (sort by_final_required_timer keys %REQUIREDGROUPSHASH) {

        S4P::logger("DEBUG", "fill_in_required_file_groups(): Looking at options for LUN: [$lun]");

####### Within this LUN, look at all the file groups associated with it. 
####### Look at REQ1 first, then REQ2, etc.

FG:     foreach $fg ( sort by_option @{$REQUIREDGROUPSHASH{$lun}} ) {

            S4P::logger("INFO", "fill_in_required_file_groups(): **************** Starting New Search ****************");
            S4P::logger("INFO", "fill_in_required_file_groups(): CURRENT SEARCH: Data type: " . $fg->data_type .
                ", Start: " . $fg->data_start .
                ", End: " . $fg->data_end .
                ", Currency: " . $fg->currency .
                ", Need: " . $fg->need .
                ", Timer: " . $fg->timer
            );
            if ( $fg->timer <= 0 ) {
                S4P::logger("INFO", "fill_in_required_file_groups(): Timer is less than or equal to zero. No wait.");
            }
            S4P::logger("DEBUG", "fill_in_required_file_groups(): Trying to fill in file group:\n\n" . S4P::PDR::show_file_groups($fg) );

########### First, if the directory ID is NOT set to something like 
########### INSERT_DIRECTORY_HERE, skip it. This means we are already dealing 
########### with a filled out file group.
 

            if ( $fg->file_specs->[0]->directory_id !~ /INSERT/ ) {
                S4P::logger("DEBUG", "fill_in_required_file_groups(): Skipping " . $fg->data_type . " since it is already filled out.");
                push (@OUTPUTFILEGROUPS, $fg);    # We still want to include
                                                  # it in the output work order
                $LOGSTR .= $fg->data_type . "=" . $fg->currency . " ";
                next LUN;                         # Get next file group
            }

########### Big do loop. As long as we haven't exceeded the timer, we poll
########### for the desired file every $interval seconds. In between, we
########### sleep.

            do {
                S4P::logger("DEBUG", "fill_in_required_file_groups(): Calling get_handle_pathname()");
                $handle = get_handle_pathname($fg, $processing_start, $processing_stop, $INPUTPDR->post_processing_offset);
                S4P::logger("DEBUG", "fill_in_required_file_groups(): Absolute current clock value: " . time);
                S4P::logger("INFO", "fill_in_required_file_groups(): CURRENT CLOCK: " . current_clock());
                if (! $handle) {
                    if ( $PSPEC ) {
                        my @waitfiles = $fg->science_files();
                        my ($waitfile, undef) = fileparse($waitfiles[0]);
                    }
                    my $hit_elapsed_time = time() - $LAST_HIT_TIME;
                    my $polling = get_polling($hit_elapsed_time);
                    S4P::logger("INFO", "fill_in_required_file_groups(): Elapsed time since last hit: $hit_elapsed_time seconds. Polling frequency: every $polling seconds.");
                    doze(1, $polling, $fg);
                }
            } while (! $handle and (current_clock() < $fg->timer) );

            if ( $handle ) {
                $found_fg = $fg;
                if ( S4PM::Handles::is_multi_file_granule($handle) ) {
                    $isMulti = 1;
                } else {
                    $isMulti = 0;
                }
                last;
            }

            if ( ! $handle ) {
                $not_found_fg = $fg;
                S4P::logger("INFO", "fill_in_required_file_groups(): Timed out on waiting for required data type " . $not_found_fg->data_type . " from " . $not_found_fg->data_start . " to " . $not_found_fg->data_end . " with a need of " . $not_found_fg->need . "\nTrying next choice for this LUN (if there is one)...");
                next FG;
            } 
        }

####### Path found:  set it in the file_group and add it to the output file
####### groups array

        if ($handle) {	# Data file was found

            S4P::logger("INFO", "fill_in_required_file_groups(): FILE WAS FOUND!!!");
            $LAST_HIT_TIME = time();
            $LOGSTR .= $found_fg->data_type . "=" . $found_fg->currency . " ";

            if ( $isMulti or is_multifile_filegroup($found_fg) ) {
                S4P::logger("DEBUG", "fill_in_required_file_groups(): This is a mult-file file.");

                my @fs = mk_array_of_multifile_specs($handle);
                if ( @fs eq undef ) {
                    S4P::logger("FATAL", "fill_in_required_file_groups(): Could not create file specs for " . $found_fg->data_type);
                    return 1;
                }
                S4P::logger("DEBUG", $found_fg->data_type . " File Specs:\n" . S4P::PDR::show_file_specs(@fs) );
                $found_fg->file_specs(\@fs);
            } else {
                my @fs = mk_array_of_file_specs($handle);
                if ( @fs eq undef ) {
                    S4P::logger("FATAL", "fill_in_required_file_groups(): Could not create file specs for " . $found_fg->data_type);
                    return 1;
                }
                S4P::logger("DEBUG", "fill_in_required_file_groups(): Generic File Specs:\n" . S4P::PDR::show_file_specs(@fs) );
                $found_fg->file_specs(\@fs);
            }
            $ur = S4PM::Handles::get_ur_from_handle($handle);
            unless ($ur) {
                S4P::logger("FATAL", "fill_in_required_file_groups(): No UR found in UR file corresponding to " . $found_fg->data_type . " data directory: $handle" . "A proper UR in a UR file begins with 'LGID:' or 'UR:' or 'FakeLGID:'.");
                return 1;
            }
            S4P::logger("INFO", "fill_in_required_file_groups(): " . $found_fg->data_type . " file found! Touching $handle file.");
            my $res = system(touch, "$handle");
            if ( $res ) {
                S4P::logger("WARNING", "fill_in_required_file_groups(): Touch failed on $handle");
            }
            $found_fg->ur($ur);
            S4P::PDR::show_file_groups($found_fg);
            push (@OUTPUTFILEGROUPS, $found_fg);

        } elsif (! $handle and signal_file('IGNORE_REQUIRED', 1) ) {	# Data file not found but IGNORE_REQUIRED detected
            $LOGSTR .= $not_found_fg->data_type . "=NONE ";
            S4P::logger("INFO", "fill_in_required_file_groups(): 'IGNORE_REQUIRED' signal detected for required data type " . $not_found_fg->data_type . " from " . $not_found_fg->data_start . " to " . $not_found_fg->data_end . " with a need of " . $not_found_fg->need);
            S4P::logger("DEBUG", "fill_in_required_file_groups(): Leaving fill_in_required_file_groups()");
        } else {		# Data file not found and no IGNORE_REQUIRED detected

            $LOGSTR .= $not_found_fg->data_type . "=NONE ";
            S4P::logger("DEBUG", "fill_in_required_file_groups(): Leaving fill_in_required_file_groups()");
            return 1;
        }
    }
 
    S4P::logger("INFO", "**************** Search for REQUIRED input completed ****************");
    S4P::logger("DEBUG", "fill_in_required_file_groups(): Leaving fill_in_required_file_groups()");

    return 0;

}

sub is_multifile_filegroup {

################################################################################
#                           is_multifile_filegroup                             #
################################################################################
# PURPOSE: To determine if a file group represents a single or multi-file      #
#          file                                                                #
################################################################################
# DESCRIPTION: Given a file group, is_multifile_filegroup simply determines    #
#              if the file groups represents a single file file group or a     #
#              multi-file file group. It determines this by the number of file #
#              of type 'SCIENCE' in the file group. If there is more than one  #
#              such file, the file group represents a multi-file file.         #
################################################################################
# RETURN: 0 - File group represents a single file file                         #
#         1 - File group represents a multi-file file                          #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
################################################################################
# CALLED BY: fill_in_optional_file_groups                                      #
#            fill_in_required_file_groups                                      #
################################################################################

    my $fg = shift;

    S4P::logger("DEBUG", "is_multifile_filegroup(): Entering is_multifile_filegroup()");

    my @fs = @{$fg->file_specs};
    my $counter = 0;

    foreach my $fs (@fs) {
        if ( $fs->file_type eq "SCIENCE" ) {
            $counter++
        }
    }

    if ( $counter == 1 ) {
        S4P::logger("DEBUG", "is_multifile_filegroup(): $counter SCIENCE file found making this file group a single file file group.");
        return 0;
    } elsif ( $counter > 1 ) {
        S4P::logger("DEBUG", "is_multifile_filegroup(): $counter SCIENCE files found making this file group a multi-file file group.");
        return 1;
    } else {
        S4P::perish(21, "is_multifile_filegroup(): File group found in input work order containing no SCIENCE files. Every file group should contain at least one file spec with file type of SCIENCE.");
    }
}

sub mk_array_of_file_specs {

################################################################################
#                            mk_array_of_file_specs                            #
################################################################################
# PURPOSE: To create an array of file specs                                    #
################################################################################
# DESCRIPTION: mk_array_of_file_specs returns an array of file specs. This     #
#              array contains just two file specs, the one for the data file   #
#              and one for its metadata file.                                  #
################################################################################
# RETURN: @fs - Array of file specs                                            #
################################################################################
# CALLS: S4P::logger                                                           #
#        mk_single_file_spec                                                   #
################################################################################
# CALLED BY: fill_in_required_file_groups                                      #
#            fill_in_optional_file_groups                                      #
################################################################################

    my $handle = shift;

    my @fs;	# Array of file specs to return
    my $fs; 	# Individual file spec

    S4P::logger("DEBUG", "mk_array_of_file_specs(): Entering mk_array_of_file_specs()");

    my @paths = S4PM::Handles::get_filenames_from_handle($handle);
    $fs = mk_single_file_spec($paths[0], "SCIENCE");
    push(@fs, $fs);

### Now, create the file spec for the metadata file and add it to the array

    my $met = S4PM::Handles::get_metadata_from_handle($handle);
    $fs = mk_single_file_spec($met , "METADATA");
    push(@fs, $fs);

    S4P::logger("DEBUG", "mk_array_of_file_specs(): Leaving mk_array_of_file_specs()");

    return @fs;

}

sub mk_array_of_multifile_specs {

################################################################################
#                        mk_array_of_multifile_specs                           #
################################################################################
# PURPOSE: To create an array of multifile file specs (e.g. MOD000)            #
################################################################################
# DESCRIPTION: mk_array_of_multifile_specs returns an array of file specs      #
#              tailored for multi-file files, such as MOD000. This array       #
#              contains the file specs for each of the data files (e.g. PDS    #
#              files) and for the single metadata file.                        #
################################################################################
# RETURN: @fs - Array of file specs                                            #
################################################################################
# CALLS: S4P::logger                                                           #
#        mk_single_file_spec                                                   #
################################################################################
# CALLED BY: fill_in_required_file_groups                                      #
#            fill_in_optional_file_groups                                      #
################################################################################

    my $handle = shift;

    my @fs;	# Array of file specs to return
    my $fs; 	# Individual file spec

    S4P::logger("DEBUG", "mk_array_of_multifile_specs(): Entering mk_array_of_multifile_specs()");
    S4P::logger("DEBUG", "mk_array_of_multifile_specs(): handle: [$handle]");

### For multi-file files, the $path returned is actually a directory, not a 
### file name. This directory contains the individual files.

    my @L0_files = S4PM::Handles::get_filenames_from_handle($handle);
    S4P::logger("DEBUG", "mk_array_of_multifile_specs(): L0_files:");
    foreach my $f (@L0_files) {
        S4P::logger("DEBUG", "\t$f\n");
    }

### For each file found, add a new file spec 

    foreach my $L0_file (@L0_files) {
        $fs = mk_single_file_spec($L0_file, "SCIENCE");
        push(@fs, $fs);
    }

### Now, create the file spec for the metadata file and add it to the array

    my $met = S4PM::Handles::get_metadata_from_handle($handle);
    $fs = mk_single_file_spec($met , "METADATA");
    push(@fs, $fs);

    S4P::logger("DEBUG", "mk_array_of_multifile_specs(): Leaving mk_array_of_multifile_specs()");

    return @fs;
}

sub mk_single_file_spec {

################################################################################
#                             mk_single_file_spec                              #
################################################################################
# PURPOSE: To create a single file spec                                        #
################################################################################
# DESCRIPTION: mk_single_file_spec generically makes a single file spec object #
#              which includes the file name, directory, file type (e.g.        #
#              SCIENCE, METADATA) and the file size.                           #
################################################################################
# RETURN: $fg - file spec object                                               #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: mk_array_of_file_specs                                            #
#            mk_array_of_multifile_specs                                       #
################################################################################

    my ($path, $file_type) = @_;

    S4P::logger("DEBUG", "mk_single_file_spec(): Entering mk_single_file_spec()");

    my $file_size = (-s $path);
    my $fs = new S4P::FileSpec();
    $fs->pathname($path);
    $fs->file_type($file_type);
    $fs->file_size($file_size);

    S4P::logger("DEBUG", "mk_single_file_spec(): Leaving mk_single_file_spec()");

    return $fs;

}

sub by_final_optional_timer {

################################################################################
#                           by_final_optional_timer                            #
################################################################################
# PURPOSE: Sort optional file groups by the timer on the final option          #
################################################################################
# DESCRIPTION: by_final_optional_timer is a sort routine to be used with the   #
#              'sort' command. This one sorts the %OPTIONALGROUPSHASH hash     #
#              whose hash keys are LUNs and whose hash values are an array of  #
#              file groups sharing that LUN. What this sort routine does is    #
#              to make sure that the LUN whose final option has the shortest   #
#              timer is first and the LUN whose final option has the next      #
#              shortest timer is next, and so on.                              #
################################################################################
# RETURN: -1, 0, 1 - standard returns for a sort function                      #
################################################################################
# CALLS: by_option                                                             #
################################################################################
# CALLED BY: fill_in_optional_file_groups                                      #
################################################################################

    my @a = @{$OPTIONALGROUPSHASH{$a}};
    my @b = @{$OPTIONALGROUPSHASH{$b}};

    my @sorted_a = reverse sort by_option @a;
    my @sorted_b = reverse sort by_option @b;

    my $a_timer = $sorted_a[0]->timer;
    my $b_timer = $sorted_b[0]->timer;

    if ( $a_timer < $b_timer ) {
        return -1;
    } elsif ( $a_timer == $b_timer ) {
        return 0;
    } else { 
        return 1;
    }

}

sub by_final_required_timer {

################################################################################
#                           by_final_required_timer                            #
################################################################################
# PURPOSE: Sort required file groups by the timer on the final option          #
################################################################################
# DESCRIPTION: by_final_required_timer is a sort routine to be used with the   #
#              'sort' command. This one sorts the %REQUIREDGROUPSHASH hash     #
#              whose hash keys are LUNs and whose hash values are an array of  #
#              file groups sharing that LUN. What this sort routine does is    #
#              to make sure that the LUN whose final option has the shortest   #
#              timer is first and the LUN whose final option has the next      #
#              shortest timer is next, and so on.                              #
################################################################################
# RETURN: -1, 0, 1 - standard returns for a sort function                      #
################################################################################
# CALLS: by_option                                                             #
################################################################################
# CALLED BY: fill_in_required_file_groups                                      #
################################################################################

    my @a = @{$REQUIREDGROUPSHASH{$a}};
    my @b = @{$REQUIREDGROUPSHASH{$b}};

    my @sorted_a = reverse sort by_option @a;
    my @sorted_b = reverse sort by_option @b;

    my $a_timer = $sorted_a[0]->timer;
    my $b_timer = $sorted_b[0]->timer;

    if ( $a_timer < $b_timer ) {
        return -1;
    } elsif ( $a_timer == $b_timer ) {
        return 0;
    } else { 
        return 1;
    }

}

sub by_option {

################################################################################
#                                   by_option                                  #
################################################################################
# PURPOSE: Sort file groups by the need                                        #
################################################################################
# DESCRIPTION: by_option is a sort routine to be used with the 'sort' command. #
#              This one simply sorts by the need whose values are things like  #
#              'OPT1', 'OPT2', 'REQ1', 'REQ2', etc.                            #
################################################################################
# RETURN: -1, 0, 1 - standard returns for a sort function                      #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: by_final_optional_timer                                           #
#            fill_in_optional_file_groups                                      #
################################################################################

    my $a_opt = $a->need;
    my $b_opt = $b->need;
    my $a_val;
    my $b_val;

    if ( $a_opt =~ /([0-9]+)$/ ) {
        $a_val = $1;
    } else {
        S4P::perish(40, "by_option(): Cannot parse need: [$a_opt].");
    }
    if ( $b_opt =~ /([0-9]+)$/ ) {
        $b_val = $1;
    } else {
        S4P::perish(41, "by_option(): Cannot parse need: [$b_opt].");
    }

    if ( $a_val < $b_val ) {
        return -1;
    } elsif ( $a_val == $b_val ) {
        return 0;
    } else {
        return 1;
    }
}

sub by_timer {

################################################################################
#                                   by_timer                                   #
################################################################################
# PURPOSE: Sort file groups by the timer value                                 #
################################################################################
# DESCRIPTION: by_option is a sort routine to be used with the 'sort' command. #
#              This one simply sorts file groups by the value of the timer     #
#              associated with the file group.                                 #
################################################################################
# RETURN: -1, 0, 1 - standard returns for a sort function                      #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my $a_timer = $a->timer;
    my $b_timer = $b->timer;

    if ( $a_timer < $b_timer ) {
        return -1;
    } elsif ( $a_timer == $b_timer ) {
        return 0;
    } else {
        return 1;
    }
}

sub get_required_groups {

################################################################################
#                              get_required_groups                             #
################################################################################
# PURPOSE: To return an array containing only the required file groups         #
################################################################################
# DESCRIPTION: get_required_groups takes as input the array containing all file#
#              groups found in the input PDR and extracts out the required     #
#              file groups and returns them in another array.                  #
################################################################################
# RETURN:  @requiredGroups - array containing the required file groups         #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my @file_groups = @_;

    S4P::logger("DEBUG", "get_required_groups(): Entering get_required_groups()");
 
    my @requiredGroups = ();

    foreach my $file_group (@file_groups) {

        my $need = $file_group->need;

        if ( $PSPEC or $need =~ /^REQ/ ) {
            push(@requiredGroups, $file_group);
        }
    }

    my $numberFound = scalar(@requiredGroups);
    S4P::logger("INFO", "get_required_groups(): Found $numberFound required file groups in input PDR.");

    S4P::logger("DEBUG", "get_required_groups(): Leaving get_required_groups()");

    return @requiredGroups;
}

sub get_optional_groups {

################################################################################
#                             get_optional_groups                              #
################################################################################
# PURPOSE: To return an array containing only the optional file groups         #
################################################################################
# DESCRIPTION: get_optional_groups takes as input the array containing all file#
#              groups found in the input PDR and extracts out the optional     #
#              file groups and returns them in another array.                  #
################################################################################
# RETURN:  @optionalGroups - array containing the optional file groups         #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my @file_groups = @_;

    S4P::logger("DEBUG", "get_optional_groups(): Entering get_optional_groups()");
 
    my @optionalGroups = ();

    foreach my $file_group (@file_groups) {

        my $need = $file_group->need;

        if ( $need =~ /OPT/ ) {
            push(@optionalGroups, $file_group);
        }
    }

    my $numberFound = scalar(@optionalGroups);
    S4P::logger("INFO", "get_optional_groups(): Found $numberFound optional file groups in input PDR.");

    S4P::logger("DEBUG", "get_optional_groups(): Leaving get_optional_groups()");

    return @optionalGroups;
}

sub get_handle_pathname {

################################################################################
#                            get_handle_pathname                               #
################################################################################
# PURPOSE: To determine the pathname to a data file                            #
################################################################################
# DESCRIPTION: get_handle_pathname is the main routine that determines the     #
#              local (on S4PM) file name and directory of required or optional #
#              data. The inputs are the file group and the data start and end  #
#              times. For a path to be returned, the routine first verifies    #
#              that a metadata file and a UR file exists. Both these files     #
#              must exist for a data file to be considered extant on the       #
#              system.                                                         #
################################################################################
# RETURN: $path                                                                #
#         undef if request could not be satisfied                              #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        S4P::TimeTools::CCSDSa_Diff                                           #
#        S4P::TimeTools::get_filename_pattern                                  #
#        get_data_name                                                         #
#        S4PM::infer_platform                                                  #
################################################################################
# CALLED BY: fill_in_optional_file_groups                                      #
#            fill_in_required_file_groups                                      #
################################################################################

    my ($file_group, $start_time, $stop_time, $offset) = @_;

    S4P::logger("DEBUG", "get_handle_pathname(): Entering get_handle_pathname()");

    my $data_type = $file_group->data_type;
    my $pattern;

### If the UR field is already filled out, then we assume that the file
### name is too and all we need to do is look for that file. Otherwise, we'll
### need to form a file name pattern and search with that.

    if ( ! $PSPEC and $file_group->ur eq "INSERT_UR_HERE" ) {

        my $coverage = S4P::TimeTools::CCSDSa_Diff($file_group->data_start, $file_group->data_end);
        my $processingperiod = S4P::TimeTools::CCSDSa_Diff($start_time, $stop_time);
        if ( $processingperiod eq undef ) {
            S4P::perish(50, "get_handle_pathname(): S4P::TimeTools::CCSDSa_Diff: Failed. Input to S4P::TimeTools::CCSDSa_Diff were: Start Time: [$start_time, Stop Time: [$stop_time]");
        }
    
        $pattern = S4PM::make_patterned_glob($data_type, $file_group->data_version, $file_group->data_start);
    
        if ( $pattern eq "ERROR" ) {
            S4P::perish(50, "get_handle_pathname(): S4P::TimeTools::get_filename_pattern: Failed to generate a viable file name pattern, most likely due to a failure of one of the time tools. Inputs to S4P::TimeTools::get_filename_pattern were: Coverage: [$coverage], Start Time: [$start_time], Processing Period: [$processingperiod], Post Processing Offset: [$offset], Currency: [" . $file_group->currency . "], and Boundary: [" . $file_group->boundary . "]");
        }
        
        $pattern =~ s/\*$//;     # Get rid of trailing '*'
        S4P::logger("INFO", "get_handle_pathname(): This file name PATTERN is being used in searching for UR files: [$pattern.ur]");
    
        unless ( exists $ALLOC::datatype_pool_map{$data_type} ) {
            S4P::perish(30, "get_handle_pathname(): No pool for data type $data_type in datatype_pool_map hash. Check Allocate Disk configuration file.");
        }
    
    } else {	# File name is specified so we don't need to use a pattern

        S4P::logger("DEBUG", "get_handle_pathname(): File name already specified. No need to pattern search.");
        $pattern = get_data_name($file_group);

    }

    my $pool;
    my $orig_data_type = $data_type;
    $data_type = S4PM::get_datatype_if_proxy($data_type, \%ALLOC::proxy_esdt_map);
    unless ( $data_type eq $orig_data_type ) {
        S4P::logger("INFO", "get_handle_pathname(): Data type $data_type is a proxy for $orig_data_type.");
    }
    $pool = $ALLOC::datatype_pool_map{$data_type};
    unless ( exists $ALLOC::datatype_pool{$pool} ) {
        S4P::perish(30, "get_handle_pathname(): No directory location for data pool $pool in datatype_pool hash. Check Allocate Disk configuration file.");
    }

    my $dir = $ALLOC::datatype_pool{$pool};
    $dir .= "/ur" if ( -e "$dir/ur" );
    
    my $path;

    my $custom_find;
    my $run_custom = 0;
    if ( exists $CFG::cfg_custom_find{$PGE}{$file_group->lun} ) {
        $run_custom = 1;
        $custom_find =  $CFG::cfg_custom_find{$PGE}{$file_group->lun};
    }

    if ( $run_custom ) {
        $path = run_custom_find($custom_find, $file_group, $start_time, $stop_time, $offset, $dir);
        if ( $path eq undef ) {
            S4P::logger("ERROR", "get_data_pathname(): No file handle found using custom find: [$custom_find]");
            S4P::logger("DEBUG", "get_data_pathname(): Leaving get_data_pathname()");
            return;
        }
    } else {

        if ( $pattern =~ /\*/ ) {	# We are dealing with a glob pattern

            S4P::logger("INFO", "get_handle_pathname(): Pattern [$pattern] contains asterisks, so we will be globbing.");
            my @handles = S4PM::Handles::get_matching_handles($dir, "$pattern.ur");
            S4P::logger("INFO", "get_matching_handles(): Matching handles:");
            foreach my $h ( @handles ) {
                S4P::logger("INFO", "\t$h");
            }

########### Verify that we got one and only one file from the glob

            S4P::logger("DEBUG", "get_handle_pathname(): Files handles matching the above PATTERN: [@handles]\n");

            my $nhandles = scalar(@handles);

            if ( $nhandles == 0 ) {
                S4P::logger("ERROR", "get_handle_pathname(): No file handle found matching file name pattern: [$pattern.ur]");
                S4P::logger("DEBUG", "get_handle_pathname(): Leaving get_handle_pathname()");
                return;
            } elsif ( $nhandles > 1 ) {
                S4P::logger("WARNING", "get_handle_pathname(): More than one file handle found matching file name pattern: [$pattern.ur]. There may simply be duplicates. So, I'll let it go.");
            }
            $path = ($nhandles == 1) ? $handles[0] : $handles[-1];
        } else {
            S4P::logger("INFO", "get_handle_pathname(): Pattern [$pattern] contains NO asterisks, so we will NOT be globbing. Excellent!");
            $path = "$dir/$pattern.ur";
        }
    }

    return $path;

}

sub is_exist_ancillary {

    my $file = shift;

    my $met = "$file.met";

    if ( -e $file and -e $met ) {
        return 1;
    } else {
        unless ( -e $file ) {
            S4P::logger("ERROR", "is_exist_ancillary(): Data file: [$file] is missing even though $file.ur exists!");
        }
        unless ( -e $met ) {
            S4P::logger("ERROR", "is_exist_ancillary(): Metadata file: [$met] is missing even though $file.ur exists!");
        }
        return 0;
    }

}

sub uniqueness {

################################################################################
#                                 uniqueness                                   #
################################################################################
# PURPOSE: To determine if an array's elements are all unique, the same, or    #
#          mixed                                                               #
################################################################################
# DESCRIPTION: uniqueness simply determines how unique the elements of an      #
#              array are: all unique (no repeats), all the same, or a mix in   #
#              which some elements are repeated.                               #
################################################################################
# RETURN:  1 - all elements are unique (no repeats)                            #
#         -1 - all elements are the same                                       #
#          0 - at least some of the elements are repeats                       #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: get_groups_by_lun                                                 #
################################################################################

    my @ar = @_;

    my $num_elements = scalar(@ar);
    my %seen = ();

    foreach my $el (@ar) {
        $seen{$el} = 1;
    }

    my $kounter = 0;
    foreach my $key ( keys %seen ) {
        $kounter++;
    }

    if ( $kounter == $num_elements ) {
        return 1;       # All array elements are unique
    } elsif ( $kounter == 1 ) {
        return -1;      # All array elements are the same
    } else {
        return 0;       # Array elements are a mix, some being the same as others
    }
}

sub validate_pdr {

    S4P::logger("DEBUG", "validate_pdr(): Entering validate_pdr()");
    S4P::logger("DEBUG", "validate_pdr(): Leaving validate_pdr()");
    return;
}

sub current_clock {

    return time() - $CLOCK;

}

sub advance_clock {

    my $value = shift;

    $CLOCK = time() - $value;

}

sub signal_file {

################################################################################
#                                  signal_file                                 #
################################################################################
# PURPOSE: To determine if a signal file has been created in the job directory #
################################################################################
# DESCRIPTION: signal_file simply looks for a file named in the argument       #
#              passed into this subroutine.                                    #
################################################################################
# RETURN: 0 - Signal file was not found                                        #
#         1 - Signal file was found                                            #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: fill_in_optional_file_groups                                      #
#            fill_in_required_file_groups                                      #
################################################################################

    my ($sigfile, $sticky) = @_;

    if ( -e "./$sigfile" ) {
        S4P::logger("DEBUG", "signal_file(): $sigfile signal file detected in job directory.");
        unless ( $sticky ) {
            unless ( unlink("./$sigfile") ) {
                S4P::perish(120, "signal_file(): Failed to unlink file: [./$sigfile]");
            }
        }
        return 1;
    } else {
        return 0;
    }
}

sub run_custom_find {
    
    my ($custom_find, $file_group, $start_time, $stop_time, $offset, $data_dir) = @_;

    S4P::logger("DEBUG", "run_custom_find(): Entering run_custom_find()");

    $start_time =~  s/00Z/00.000000Z/;
    $stop_time  =~  s/00Z/00.000000Z/;

### Build parameter file to pass into script

    my $fn = "find_parm.$$.txt";
    open(PARM, ">$fn") or S4P::perish(10, "run_custom_find(): Failed to open file [$fn] new for writing:$!");
    print PARM "\$process_start = \"$start_time\";\n";
    print PARM "\$process_stop = \"$stop_time\";\n";
    print PARM "\$offset = $offset;\n";
    print PARM "\$data_dir = \"$data_dir\";\n";
    print PARM "\$data_type = \"" . $file_group->data_type . "\";\n";
    print PARM "\$data_version = \"" . $file_group->data_version . "\";\n";
    print PARM "\$data_start = \"" . $file_group->data_start . "\";\n";
    print PARM "\$data_stop = \"" . $file_group->data_end . "\";\n";
    print PARM "\$currency = \"" . $file_group->currency . "\";\n";
    print PARM "\$boundary = \"" . $file_group->boundary . "\";\n";
    close(PARM) or S4P::perish(10, "run_custom_find(): Failed to close file [$fn]:$!");

    my $cmd = $custom_find . " $fn";
    S4P::logger("INFO", "run_custom_find(): Running this command: [$cmd]");
    my $path = `$cmd`;
    S4P::logger("INFO", "run_custom_find(): Path found is: [$path]");
    unlink($fn) or S4P::perish(10, "run_custom_find(): Failed to unlink file [$fn]:$!");

    if ( $path eq undef or $path =~ /^\s*$/ ) {
        return undef;
    } elsif ( $path =~ /^[\.\/].*$/ ) {
        return $path;
    } else {
        S4P::perish(40, "run_custom_find(): command [$cmd] returned this error message: [$path]");
    }
}
