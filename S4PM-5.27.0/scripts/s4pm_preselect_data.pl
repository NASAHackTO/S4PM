#!/usr/bin/perl

eval 'exec /usr/bin/perl  -S $0 ${1+"$@"}'
    if 0; # not running under some shell

=head1 NAME

s4pm_preselect_data.pl - pre-select data prior to running s4pm_select_data.pl

=head1 SYNOPSIS

s4pm_preselect_data.pl 
B<-p[ge]> I<algorithm> 
B<-a[lloc]> I<allocate_disk_cfg>
[-B<d[irectory]> I<configfile_directory>]
[-B<th[reshold]> I<input_threshold>]
[-B<ti[mer]> I<wait_timer>]
[-B<i[nterval]> I<polling_interval>]
I<SELECT_workorder>

=head1 DESCRIPTION

The B<s4pm_preselect_data.pl> looks for data files of a particular data 
type until the threshold number of files is found or until the wait timer 
expires.  Polling for the data is carried out every I<polling_interval> 
seconds. If the minimum threshold of files is found within 
I<polling_interval> seconds, B<s4pm_preselect_data.pl> exits with a zero; 
otherwise it exits with a one. The output work order is of type FOUND and is 
identical to the input SELECT work order. The output FOUND work order is 
intended to be fed back into B<s4pm_select_data.pl> for normal processing.

Several manual overrides are provided for via signal files:

=over 4

=item RELEASE_JOB_NOW

If a file named B<RELEASE_JOB_NOW> is detected in the job directory, the 
B<s4pm_select_data.pl> will exit quickly with a zero.

=item MODIFY_TIMER

If a file named B<MODIFY_TIMER> is detected in the job directory, a small 
GUI will pop up allowing a user to modify the wait timer (up or down) while
the job is still running.

=item MODIFY_THRESHOLD

If a file named B<MODIFY_THRESHOLD> is detected in the job directory, a small 
GUI will pop up allowing a user to modify the minimum files threshold (up 
or down) while the job is still running.

=back

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_preselect_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P::PDR;
use S4P;
use S4PM::Algorithm;
use S4PM;
use Getopt::Long;
use S4P::TimeTools;
use Tk;
require 5.6.0;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw($CONFIGDIR
            $PGE
            $THRESHOLD
            $CLOCK
            $POLLING
            $PDR
            $SELECT_FILEGROUP
            $TIMER
            $GRANULE_COUNT
            $ALLOCCFG
            $DATATYPE
            $DATAVER
            $LEAPSEC_DATATYPES
            %datatype_pool
            %datatype_pool_map
            %datatype_maxsize
            %proxy_esdt_map
           );

# $CONFIGDIR is the default directory location of the algorithm select data 
# configuration files

$CONFIGDIR = "../select_data_cfg";

$PGE = undef;	# Initialize to undef so we can later test that it has been
		# passed as an argument

$THRESHOLD = undef;	# Mininum number of input files 

$CLOCK = time();

$POLLING = undef;

$ALLOCCFG = undef;

$LEAPSEC_DATATYPES = undef;

################################################################################

my $input_workorder;
my $polling_default = 7200;

# $wo_prefix is the string to prepend to all output work orders

my $wo_prefix = "FIND_";

# Read in the command line arguments

GetOptions( "pge=s"       => \$PGE,
            "alloc=s"     => \$ALLOCCFG,
            "directory=s" => \$CONFIGDIR,
            "threshold=s" => \$THRESHOLD,
            "interval=s"  => \$POLLING,
            "timer=s"     => \$TIMER,
          );
 
# Verify that the algorithm name has been specified

unless ( $PGE ) {
    S4P::perish(10, "main: No algorithm argument has been specified!");
}
unless ( $THRESHOLD ) {
    S4P::perish(10, "main: No threshold argument has been specified!");
}
unless ( $ALLOCCFG ) {
    S4P::perish(30, "main: No Allocate Disk configuration file was specified with the -a option.");
}
unless ( $TIMER ) {
    S4P::logger("WARNING", "main: No timer value has been specified. Using default of 86400.");
    $TIMER = 86400;
}

if ( $POLLING ) {
    $main::Polling_Interval = $POLLING;
    if ( $main::Polling_Interval < 0 or $main::Polling_Interval > 86400 ) {
        S4P::perish(30, "main: Invalid polling frequency: $main::Polling_Interval. Values must be > 0 and less than 86400 seconds (1 day).");
    }
} else {
    $main::Polling_Interval = $polling_default;
    S4P::logger("INFO", "main: Polling frequency was set to default of $main::Polling_Interval seconds.");
}

if ( $PGE =~ /^DO\.SELECT/ ) {
    S4P::logger("ERROR", "main: algorithm name looks like a SELECT work order file name. If I die, you'll know why!");
}

# Set the name of the algorithm configuration file to look for

my $configfile = "s4pm_select_data_" . "$PGE" . ".cfg";
my $oldstyle_configfile = "specify_data_" . "$PGE" . ".cfg";

# First, try new name for config files. If that fails, try the old name.

if ( ! -e "$CONFIGDIR/$configfile" ) {
    unless ( -e "$CONFIGDIR/$oldstyle_configfile" ) {
        S4P::perish(30, "main: No algorithm configuration file seems to exist for algorithm $PGE. Looking for $CONFIGDIR/$configfile or $CONFIGDIR/$oldstyle_configfile.");
    }
    $configfile = $oldstyle_configfile;
}

S4P::logger("DEBUG", "main: algorithm: [$PGE], configfile: [$CONFIGDIR/$configfile]");

# Verify that an input work order (last command-line argument) has been
# specified and that it exists. Note that GetOptions (above) will remove
# from ARGV only those arguments it recognizes and leave the remaining in
# there. Since the input work order is not "recognized", it is guaranteed
# to be the only remaining argument and will thus be the first: $ARGV[0]

if ( !defined $ARGV[0] ) {
    S4P::perish(10, "main: No input work order specified! Must be second argument and file name of the input SELECT work order");
} else {
    $input_workorder = $ARGV[0];
    unless ( -e $input_workorder ) {
        S4P::perish(20, "main: Work order $input_workorder doesn't seem to exist!");
    }
}

S4P::logger("INFO", "********** s4pm_preselect_data.pl starting algorithm $PGE and work order $input_workorder **********");
S4P::logger("DEBUG", "main: input_workorder: [$input_workorder]");

# Dump contents of select data config file to log file for posterity

dump_config("$CONFIGDIR/$configfile");

################################################################################

# Read in the configuration files

my $string = S4P::read_file($ALLOCCFG);
if ( ! $string ) {
    S4P::perish(30, "main: Failed to read $ALLOCCFG. Make sure a link to this file exists in the granfind station directory.");
}
S4P::logger("DEBUG", "main: string: [$string]");
eval ($string);
if ( $@ ) {
    S4P::perish(32, "main: Failed to eval string returned from S4P::read_file(): $@");
}

### Read and parse the input work order which should only contain a single
### file group, the one triggering the chain of events leading to a 
### algorithm run

    $PDR = new S4P::PDR('text' => S4P::read_file($input_workorder));
    my @fg = @{$PDR->file_groups};

### If the input work order does have more than one file group, something has
### gone very wrong and we should bail out.

    if ( scalar( @fg ) > 1 ) {
        S4P::perish(20, "main: Input work order: $input_workorder contains more than one FILE_GROUP object. This should NEVER happen. Something is very wrong.");
    }

    $SELECT_FILEGROUP = $fg[0];

# Create a new algorithm config object from the algorithm configuration file 
# for this algorithm

my $algorithm = new S4PM::Algorithm("$CONFIGDIR/$configfile");
if ( ! $algorithm ) {
    S4P::perish(30, "main: Could not create new S4PM::Algorithm object from reading $CONFIGDIR/$configfile");
}

$DATATYPE = $algorithm->trigger_datatype;
$DATAVER  = $algorithm->trigger_version;
S4P::logger("DEBUG", "main: DATATYPE: [$DATATYPE]");
my @inputs = @{ $algorithm->input_groups };

$LEAPSEC_DATATYPES = $algorithm->leapsec_datatypes;
S4P::logger("DEBUG", "main: LEAPSEC_DATATYPES: [$LEAPSEC_DATATYPES]");

# Perform some work order validation

S4P::logger("DEBUG", "main: Calling validate_work_order()");
validate_work_order($algorithm);
S4P::logger("DEBUG", "main: Returned from validate_work_order()");

# Get the data coverage of the trigger input data

my $coverage;
my @inputs = @{ $algorithm->input_groups };
foreach my $input ( @inputs ) {
    if ( $input->need eq "TRIG" ) {
        $coverage = $input->coverage;
        last;
    }
}

# Create output work order which, in content, is identical to the input
# work order.

my $process_start_time = get_start_time($algorithm, $algorithm->apply_leapsec_correction);
if ( ($algorithm->processing_start eq "START_OF_MONTH") ||
     ($algorithm->processing_start eq "START_OF_AIRS_PENTAD")) {
      $algorithm->update_info($process_start_time);
      $THRESHOLD = int($algorithm->product_coverage/$coverage) ;
}
my $output_workorder = get_output_workorder($process_start_time);
S4P::logger("DEBUG", "main: process_start_time: [$process_start_time], output_workorder: [$output_workorder]");
my ($errstr, $rc) = S4P::exec_system("/bin/cp $input_workorder $output_workorder");
if ( $rc ) {
    S4P::perish(110, "main: Failed to cp input work order to output work order: $errstr");
}

# Do the main thing

my $status = poll_for_data($process_start_time, $algorithm->processing_period, $coverage);

clean_up();

S4P::logger("INFO", "********** s4pm_preselect_data.pl completed successfully! **********");

################################################################################

sub clean_up {

### Clean up any leftover signal files

    if ( -e "./RELEASE_JOB_NOW" ) {
        unlink("./RELEASE_JOB_NOW");
    }
    if ( -e "./MODIFY_TIMER" ) {
        unlink("./MODIFY_TIMER");
    }
    if ( -e "./MODIFY_THRESHOLD" ) {
        unlink("./MODIFY_THRESHOLD");
    }
    if ( -e "./sleep.message.$$" ) {
        unlink("./sleep.message.$$" );
    }

}

sub current_clock {

    return time() - $CLOCK;

}

sub doze {

    my $file = "sleep.message.$$";
    my $add_timer = undef;
    my $add_threshold = undef;


    my $dreams = $POLLING/30;
    for (my $i; $i < $dreams; $i++) {
        S4P::write_file($file, "Sleeping $POLLING secs: " . sleep_msg(). "\n");
        if ( release_job_now() ) { 
            S4P::logger("INFO", "doze(): RELEASE_JOB_NOW signal detected. The search for files will be discontinued.");
            if ( $GRANULE_COUNT < $THRESHOLD ) {
                S4P::logger("WARNING", "doze(): This job is being released without the minimum number of files having been found. File count is $GRANULE_COUNT, but minimum threshold was $THRESHOLD.");
            } else {
                S4P::logger("WARNING", "doze(): This job is being released with $GRANULE_COUNT files found which exceeds the minimum of $THRESHOLD.");
            }
            clean_up();
            exit 0;
        }
        if ( ($add_timer = modify_timer()) ) {
            if ( $add_timer ) {
                S4P::logger("INFO", "doze(): MODIFY_TIMER signal detected. Timer was extended by $add_timer seconds.");
                $TIMER += $add_timer;
            }
            if ( -e "./MODIFY_TIMER" ) {
                unlink("./MODIFY_TIMER");
            }
        }
        if ( ($add_threshold = modify_threshold()) ) {
            if ( $add_threshold ) {
                S4P::logger("INFO", "doze(): MODIFY_THRESHOLD signal detected. File threshold was modified by $add_threshold files.");
                $THRESHOLD += $add_threshold;
            }
            if ( -e "./MODIFY_THRESHOLD" ) {
                unlink("./MODIFY_THRESHOLD");
            }
        }

        sleep(30);
    }
    unlink($file);

}

sub dump_config {

    my $configfile = shift;

    my $str = ""; 
    my $line;

    open(CFG, "$configfile") or 
        S4P::perish(100, "dump_config(): Could not open select data configuration file: $configfile: $!");
    while ( ($line = <CFG>) ) {
        $line =~ s/#.*//;                        # Get rid of comment lines
        $line =~ s/^\s+//;                       # Trim off leading whitespace
        $line =~ s/\s+$//;                       # Trim off trailing whitespace
        next unless length($line);               # Anything left?
        $str .= "$line\n";
    }

    close(CFG) or S4P::perish(100, "dump_config(): Could not close select data configuration file: $configfile: $!");

    S4P::logger("INFO", "dump_config(): Contents of $configfile:\n\nBegin active contents of $configfile  ------------------------\n\n$str\nEnd active contents of $configfile  --------------------------\n\n");
    
}

sub get_output_workorder {
    
    my $start = shift;
    my ($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($start);
    my $doy = S4P::TimeTools::day_of_year($year, $month, $day);
    return "FOUND_" . $PGE . "." . $year . $doy . $hour . $min . $sec . ".wo";
}

sub get_start_time {

    my ($algorithm, $leapsec_correction_flag) = @_;

    my $start = $algorithm->accumulation_start($SELECT_FILEGROUP->data_start);
    unless ( $start ) {
        S4P::perish(100, "get_start_time(): Failed to determine start time of data accumulation window.");
    }

    if ( $leapsec_correction_flag ) {
        $start = S4PM::leapsec_correction($start, 1, 1);
    }

    return $start;

}

sub make_patterns_hash {
 
    my ($start_time, $duration, $coverage) = @_;

    my %patterns = ();

    my $npatterns = int($duration/$coverage);	# Number of file name patterns

### If we're dealing with a leap second data type, we need to apply a correction

    if ( $LEAPSEC_DATATYPES ) {
        if ( $LEAPSEC_DATATYPES =~ /$DATATYPE/ ) {
            $start_time = S4PM::leapsec_correction($start_time, 1, 1);
        }
    }

### All data times are computed relative to the process start time ($start_time)

    for (my $i = 0; $i < $npatterns; $i++) {
        my $datastart = S4P::TimeTools::CCSDSa_DateAdd($start_time, ($i*$coverage));
        my $pat = S4PM::make_patterned_glob($DATATYPE, $DATAVER, $datastart);
        S4P::logger("INFO", "make_patterns_hash(): Adding this pattern: [$pat] to search list.");
        $patterns{$pat} = 0;	# Initially, set all patterns to not found yet
    }

    return %patterns;

}

sub modify_threshold {

    my $value;	# Value submitted by user
    my $title;
    my $diff = $THRESHOLD - $GRANULE_COUNT;

    if ( -e "./MODIFY_THRESHOLD" ) {
        S4P::logger("INFO", "modify_threshold(): MODIFY_THRESHOLD detected in job directory. Querying user for timer modification.");
        $title = "Enter number to be added or subtracted from current file threshold of $THRESHOLD files. Current count is $GRANULE_COUNT, a difference of $diff granules exists.";
    } else {
        return undef;
    }

    my $status = tkmodify($title);
    unless ( $status ) {
        clean_up();
    }
  
    return $status;

}

sub modify_timer {

    my $value;	# Value submitted by user
    my $title;

    if ( -e "./MODIFY_TIMER" ) {
        S4P::logger("INFO", "modify_timer(): MODIFY_TIMER detected in job directory. Querying user for timer modification.");
        $title = "Enter number of seconds to add or subtract from current timer which is set to $TIMER seconds:";
    } else {
        return undef;
    }

    my $status = tkmodify($title);
    unless ( $status ) {
        clean_up();
    }
  
    return $status;

}

sub tkmodify {

    my $title = shift;

    my $value;
    my $main_window = new MainWindow(-screen => $ENV{'DISPLAY'});
    $main_window->title("Modify Timer");

### Frames

    my $label_frame = $main_window->Frame->pack(-expand => 1, -fill => 'both', -padx => 5, -pady => 5);
    my $entry_frame = $main_window->Frame->pack(-expand => 1, -fill => 'both', -padx => 5, -pady => 5);
    my $button_frame = $main_window->Frame->pack(-expand => 1, -fill => 'both', -padx => 5, -pady => 5);

    $label_frame->Label(-text => $title, -background => "white", -foreground => "red", -padx => 20, -pady => 10, -wraplength => 200)->pack(-expand => 1);

    my $entry = $entry_frame->Entry(-textvariable => \$value)->pack(-expand => 1, -side => "left", -padx => 5, -pady => 5);
    $entry->bind( '<Key-Return>', [ sub { $main_window->destroy(); }, Ev('K') ] );

    $button_frame->Button( -text => "Submit", -command => sub { $main_window->destroy(); })->pack(-expand => 1, -side => 'left', -padx => 10, -pady => 10, -fill => 'both');

    $button_frame->Button(-text => "Cancel", -command => sub { $value = undef; $main_window->destroy(); })->pack(-side => 'left', -fill => 'both', -padx => 10, -pady => 10, -expand => 1);

    MainLoop;

    return $value;
}

sub poll_for_data {

    my ($start_time, $duration, $coverage) = @_;

    S4P::logger("DEBUG", "poll_for_data(): Entering poll_for_data() with start_time: [$start_time], duration: [$duration], coverage: [$coverage], DATATYPE: [$DATATYPE]");

    $GRANULE_COUNT = 0;	# Counts how many files found so far

### Get array of file name glob patterns with which to search file system

    my %pattern_hash = make_patterns_hash($start_time, $duration, $coverage);

### Using the data type, find the disk pool location in which to search

    unless ( exists $main::datatype_pool_map{$DATATYPE} ) {
        S4P::perish(30, "poll_for_data(): No pool for data type $DATATYPE in datatype_pool_map hash. Check Allocate Disk configuration file.");
    }
    my $pool = $main::datatype_pool_map{$DATATYPE};
    unless ( exists $main::datatype_pool{$pool} ) {
        S4P::perish(30, "poll_for_data(): No directory location for data pool $pool in datatype_pool hash. Check Allocate Disk configuration file.");
    }
    my $dir = $main::datatype_pool{$pool};

### Big loop to carry out the search

    my $first_iteration = 1;
    do {

        foreach my $pattern ( keys %pattern_hash ) {

            next if ( $pattern_hash{$pattern} == 1);	# Skip if already found

            my $numURfiles   = 0;
            my $numMetFiles  = 0;
            my $numDataFiles = 0;

            my @files = glob("$dir/$pattern");
            S4P::logger("DEBUG", "poll_for_data(): Files matching the glob pattern [$pattern] in directory [$dir] are: [@files]\n");

########### Verify that we have the data file, the metadata file, and the UR
########### file For multifile files, the "data file" is really the directory
########### name where the individual files are located

            my $urpath;
            my $matching_files = "";
            foreach my $file (@files) {
                if ( $file =~ /\.ur$/ ) { 
                    $numURfiles++; 
                    $urpath = $file;
                }
                if ( $file =~ /\.met$/ ) { $numMetFiles++; }
                if ( $file !~ /\.ur$/ and $file !~ /\.met$/ and $file !~ /\.xml$/ ) {
                    $matching_files .= "$file\n";
                    $numDataFiles++;
               }
               S4P::logger("DEBUG", "poll_for_data(): path: [$urpath]");
            }

            if ( $numURfiles == 0 ) {
                S4P::logger("DEBUG", "poll_for_data(): No UR file found matching file name pattern: [$pattern]");
            } elsif ( $numURfiles > 1 ) {
                S4P::perish(80, "poll_for_data(): More than one UR file found matching file name pattern: [$pattern]. ACTION: Decide which is the one you want (if any) and get rid of the rest.");
            }
            if ( $numMetFiles == 0 ) {
                S4P::logger("DEBUG", "poll_for_data(): No metadata file found matching file name pattern: [$pattern]");
            } elsif ( $numMetFiles > 1 ) {
                S4P::perish(80, "poll_for_data(): More than one metadata file found matching file name pattern: [$pattern]. ACTION: Decide which is the one you want (if any) and get rid of the rest.");
            }
            if ( $numDataFiles == 0 ) {
                S4P::logger("DEBUG", "poll_for_data(): No data file found matching file name pattern: [$pattern]");
            } elsif ( $numDataFiles > 1 ) {
                S4P::perish(80, "poll_for_data(): More than one data file (or directory for multifile files) found matching file name pattern: [$pattern]. These are their names: $matching_files ACTION: Decide which is the one you want (if any) and get rid of the rest.");
            }

########### If file found and all looks ok, let's count it

            if ( $numURfiles == 1 and $numMetFiles == 1 and $numDataFiles == 1 ) {
                $GRANULE_COUNT++;
                $pattern_hash{$pattern} = 1;	# Mark pattern as found
                S4P::logger("DEBUG", "poll_for_data(): touching: [$urpath]");
                my $res = system(touch, "$urpath");
                if ( $res ) {
                    S4P::logger("WARNING", "poll_for_data(): Touch failed on $urpath");
                }

            } else {
                S4P::logger("DEBUG", "poll_for_data(): No file found matching file name pattern: [$pattern]");
            }

        }

####### Now doze and at the same time, look for any signal files

        unless ( $first_iteration ) {
            doze();
        } else {
            $first_iteration = 0;
        }

        S4P::logger("INFO", "poll_for_data(): Current clock: " . current_clock() . "\nClock will expire at: $TIMER" . "\nCurrent file count for $DATATYPE: $GRANULE_COUNT\nThreshold count for releasing: $THRESHOLD");

    } while ( $GRANULE_COUNT < $THRESHOLD && current_clock() < $TIMER );

    if ( $GRANULE_COUNT < $THRESHOLD ) {
        S4P::perish(130, "poll_for_data(): Failed to find minimum number of files (set to $THRESHOLD) of $DATATYPE for algorithm $PGE within $TIMER seconds. ACTION: Restart this process after investigating why this has happened.");
        return 1;
    } else {
        return 0;
    }
}

sub release_job_now {

    if ( -e "./RELEASE_JOB_NOW" ) {
        S4P::logger("INFO", "release_job_now(): RELEASE_JOB_NOW detected in job directory. Expiring all wait timer and releasing job now.");
        return 1;
    } else {
        return 0;
    }

}

sub sleep_msg {

    return "\n\nCurrent clock: " . current_clock() . "\nClock will expire at: $TIMER" . "\nCurrent file count for $DATATYPE: $GRANULE_COUNT\nThreshold count for releasing: $THRESHOLD";

}
sub validate_work_order {

################################################################################
#                           validate_work_order                                #
################################################################################
# PURPOSE: To perform some sanity checking on the input SELECT work order      #
################################################################################
# DESCRIPTION: validate_work_order does some simple sanity checks on the input #
#              work order where possible. If it does NOT pass, the error is    #
#              considered fatal.                                               #
################################################################################
# RETURN: none                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        S4P::TimeTools::CCSDSa_DateParse                                      #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my $algorithm = shift;

    my ($year, $month, $day, $hour, $min, $sec, $error);

### Verify that the input work order contains the trigger data intended for
### this algorithm.

    S4P::logger("DEBUG", "validate_work_order(): Entering validate_work_order()");

    if  ($SELECT_FILEGROUP->data_type ne $algorithm->trigger_datatype ) {
        S4P::perish(20, "validate_work_order(): DATA_TYPE in input SELECT work order is not a trigger data type for this algorithm");
    }

    my $start = $SELECT_FILEGROUP->data_start;
    ($year, $month, $day, $hour, $min, $sec, $error) =
        S4P::TimeTools::CCSDSa_DateParse($start);
    if ( $error ) {
        S4P::perish(50, "validate_work_order(): S4P::TimeTools::CCSDSa_DateParse: Cannot parse DATA_START in input SELECT work order. Input to S4P::TimeTools::CCSDSa_DateParse was: Start: [$start]");
    }
 
    my $end = $SELECT_FILEGROUP->data_end;
    ($year, $month, $day, $hour, $min, $sec, $error) =
        S4P::TimeTools::CCSDSa_DateParse($end);
    if ( $error ) {
        S4P::perish(50, "validate_work_order(): Cannot parse DATA_END in input SELECT work order: [$end]");
    }

    S4P::logger("DEBUG", "validate_work_order(): Leaving validate_work_order()");
}

