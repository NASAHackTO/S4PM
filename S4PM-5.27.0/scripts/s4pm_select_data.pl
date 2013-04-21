#!/usr/bin/perl

=head1 NAME

s4pm_select_data.pl - decide inputs for an algorithm based on production rules

=head1 SYNOPSIS

s4pm_select_data.pl 
B<-p[ge]> I<algorithm_name> 
[-B<d[irectory]> I<configfile_directory>]
I<SELECT_workorder>

=head1 DESCRIPTION

The B<Select Data> station is the main station where production rules
for a algorithm are implemented. The input work orders are SELECT work 
orders which inform S4PM about the arrival of some data type that is to 
initiate the run of a algorithm. The new data may be products produced within 
S4PM or products that came into S4PM from the ECS archive. Based upon the 
contents of the SELECT work order, the B<Select Data> station outputs one 
or more FIND work orders for the additional input data needed. These 
additional data include the required inputs and the optional inputs, if
any. The FIND work orders contain placeholders for file names, directory
locations, and URs. These work orders are then sent to the B<Granule Find> 
station where actual file names and directory locations are determined.

A configuration file tailored for each algorithm describes the production rules
for the algorithm. See internal documentation in I<s4pm_select_data.cfg>
file.

The B<-pge> argument is the algorithm name. For each algorithm, a configuration 
file with the file name: s4pm_select_data_I<algorithm_name>.cfg must exist.

The B<-directory> argument is optional and specifies the location of
the appropriate select_data configuration file for the algorithm being
specified.

The final argument is the input SELECT work order file name.

Output FIND work orders are named: FIND_I<algorithm_name>.I<jobid>.wo
where I<jobid> is the year, day of year, hours, minutes, and seconds
of the processing start time for that work order. For example, a
work order with the file name FIND_MoPGE01.2000143121000.wo 
represents a processing start time of day 143 in the year 2000 at 
12:00:00.

B<s4pm_select_data.pl> performs basic checking of both the input SELECT 
work order and the algorithm configuration file. 

=head1 PRODUCTION RULES

B<s4pm_select_data.pl> supports, via the algorithm-specific configuration 
files, the following production rules:

=over 4

=item Simple Time-Based

The input file staged have data coverages that match the processing
time. If the processing period is longer than the input's temporal coverage,
more than one input will be matched to cover the entire period.

=item Optional Input with Expiration Timer

One or more optional files may be associated with a single Process 
Control File (PCF) logical unit number (LUN). Each is associated with an
expiration timer and ranked according to preference. If an optional file 
is available within the time set by the timer, it is used by the algorithm. If 
it is not available, B<s4pm_select_data.pl> will seek the next desirable choice 
(if specified). This process continues until either all options have failed 
to be retrieved within the time allowed or an option is found. If no option 
is found, the algorithm is allowed to run without that LUN being fulfilled.

=item Required Input with Expiration Timer

One or more required files may be associated with a single Process 
Control File (PCF) logical unit number (LUN). Each is associated with an
expiration timer and ranked according to preference. If a required file 
is available within the time set by the timer, it is used by the algorithm. If 
it is not available, B<s4pm_select_data.pl> will seek the next desirable choice 
(if specified). This process continues until either all choices have failed 
to be retrieved within the time allowed or a choice is found. If no choice 
is found, the algorithm will not be allowed to run. In ECS PDPS parlance, this
production rules is referred to as the 'alternate inputs' rule.

=item Previous n Input Granule

An input can be for a time earlier than the time indicated by the processing
period by n times the input's temporal coverage, where n = 1, 2, 3,..
Such a file may be either optional or required.

=item Following n Input Granule

An input can be for a time later than the time indicated by the processing
period by n times the input's temporal coverage, where n = 1, 2, 3,..
Such a file may be either optional or required.

=item Nearest Granule in Past

Look backward in time for the nearest input file to the current processing
time, with the nearest being the file matching the current processing
time. Limits are placed on where to begin looking back and how far to look
back.

=item Nearest Granule in Future

Look forward in time for the nearest input file to the current processing
time, with the nearest being the file matching the current processing
time. Limits are placed on where to begin looking forward and how far to look
forward.

=back

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_select_data.pl,v 1.4 2006/11/27 16:41:09 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Env;
use File::Basename;
use S4P::PDR;
use S4P;
use S4P::TimeTools;
use S4PM::Algorithm;
use S4PM;
use Getopt::Long;
use strict;

################################################################################
# Global variables                                                             #
################################################################################

use vars qw($PGE 
            $CONFIGDIR
            $PROCESSINGPERIOD
            $SELECT_FILEGROUP
            $NUMFILEGROUPS
            $PDR
            $NRTFLAG
           );

$NUMFILEGROUPS = 1;		# Number of FILE_GROUP objects in the output
				# FIND work order. It is at least 1 (i.e.
				# it at least contains the FILE GROUP from the 
				# input SELECT work order).

# $CONFIGDIR is the default directory location of the algorithm select data 
# configuration files

$CONFIGDIR = "../select_data_cfg";

$PGE = undef;	# Initialize to undef so we can later test that it has been
		# passed as an argument

################################################################################

my $input_workorder;

# $wo_prefix is the string to prepend to all output work orders

my $wo_prefix = "FIND_";

# Read in the command line arguments

GetOptions( "pge=s"       => \$PGE,
            "directory=s" => \$CONFIGDIR,
            "nrt" => \$NRTFLAG
          );

# Verify that the algorithm name has been specified

unless ( $PGE ) {
    S4P::perish(10, "main: No algorithm argument has been specified!");
}

if ( $PGE =~ /^DO\.SELECT/ ) {
    S4P::logger("ERROR", "main: algorithm name looks like a SELECT work order file name. If I die, you'll know why!");
}

# Set the name of the algorithm configuration file to look for

my $configfile = "s4pm_select_data_" . "$PGE" . ".cfg";

# First, try new name for config files. If that fails, try the old name.

if ( ! -e "$CONFIGDIR/$configfile" ) {
    S4P::perish(30, "main: No algorithm configuration file seems to exist for algorithm $PGE. Looking for $CONFIGDIR/$configfile.");
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

S4P::logger("INFO", "********** s4pm_select_data.pl starting algorithm $PGE and work order $input_workorder **********");
S4P::logger("DEBUG", "main: input_workorder: [$input_workorder]");

# Dump contents of select data config file to log file for posterity

dump_config($configfile);

################################################################################

### Read and parse the input work order which should only contain a single
### file group, the one triggering the chain of events leading to a algorithm 
### run

    $PDR = new S4P::PDR('text' => S4P::read_file($input_workorder));
    my @fg = @{$PDR->file_groups};

### If the input work order does have more than one file group, something has
### gone very wrong and we should bail out.

    if ( scalar( @fg ) > 1 ) {
        S4P::perish(20, "main: Input work order: $input_workorder contains more than one FILE_GROUP object");
    }

    $SELECT_FILEGROUP = $fg[0];

# Create a new algorithm config object from the algorithm configuration file 
# for this algorithm

my $algorithm = new S4PM::Algorithm("$CONFIGDIR/$configfile");
if ( ! $algorithm ) {
    S4P::perish(30, "main: Could not create new S4PM::Algorithm object from reading $CONFIGDIR/$configfile");
}

my @inputs = @{ $algorithm->input_groups };

# The algorithm's processing period is set in the algorithm configuration file

$PROCESSINGPERIOD = $algorithm->processing_period;
S4P::logger("DEBUG", "main: PROCESSINGPERIOD: [$PROCESSINGPERIOD]");

# Perform some work order validation

S4P::logger("DEBUG", "main: Calling validate_work_order()");
validate_work_order($algorithm);
S4P::logger("DEBUG", "main: Returned from validate_work_order()");

# Determine the number of output work orders needed based upon the processing
# period and the input data coverage

# The coverage of the trigger data type is set in the algorithm configuration 
# file

my $TriggerCoverage = $algorithm->trigger_coverage;
S4P::logger("DEBUG", "main: TriggerCoverage: [$TriggerCoverage]");

# Get the number of output FIND work orders we're going to produce. This
# number is simply based on the processing period of the algorithm and the 
# temporal coverage of the trigger data type.

S4P::logger("DEBUG", "main: Calling get_num_work_orders()");
my $NumWorkOrders = get_num_work_orders($TriggerCoverage, $algorithm->product_coverage);
S4P::logger("DEBUG", "main: Returned from get_num_work_orders(), NumWorkOrders: [$NumWorkOrders]");

# Begin building the output FIND work order(s) which are PDR-style files

S4P::logger("DEBUG", "main: Beginning main loop over number of output work orders...");

# Here, we loop over the number of output FIND work orders we're going
# to produce. For each one, we'll construct the three sections: the header,
# containing the process start and stop times; the body, containing the 
# file groups for required and optional inputs (including the one from the 
# input SELECT work order; and the footer, to complete the PDR work order
# file and write it out.

for (my $iteration = 0; $iteration < $NumWorkOrders; $iteration++) {

    S4P::logger("DEBUG", "main: loop iteration: [$iteration]");

### Create array of the input specs from the select data configuration 
### file for this algorithm

    S4P::logger("DEBUG", "main: Calling get_process_start_time()");
    my $start = get_process_start_time($iteration, $algorithm->trigger_coverage, $algorithm->product_coverage, $algorithm->apply_leapsec_correction, $algorithm->pre_processing_offset, $algorithm->processing_start);
    S4P::logger("DEBUG", "main: Returned from get_process_start_time(), start: [$start]");
    if ( $start eq "ERROR" ) {
        S4P::perish(50, "main: Could not compute processing start time.\n");
    }

    if ($NRTFLAG) {
# for AIRS NRT processing only begin ----
        my $local_now = S4P::TimeTools::CCSDSa_Now;
        my $utc_now = S4P::TimeTools::CCSDSa_DateAdd($local_now, 18000);
        my $utc_cutoff = S4P::TimeTools::CCSDSa_DateAdd($utc_now, -43200);

        if (S4P::TimeTools::CCSDSa_DateCompare($start, $utc_cutoff) >= 0) {
            S4P::logger("INFO", "main: Start time [$start] is older than 24 hours. Skip it.");
            next;
        }
    }
    S4P::logger("DEBUG", "main: Updating Algorithm info");
    $algorithm->update_info($start) ;
    $PROCESSINGPERIOD = $algorithm->processing_period;
    S4P::logger("DEBUG", "main: PROCESSINGPERIOD: [$PROCESSINGPERIOD]");

    S4P::logger("DEBUG", "main: Calling get_process_end_time()");
    my $end   = get_process_end_time($start, $algorithm->product_coverage, $algorithm->apply_leapsec_correction, $algorithm->processing_start);
    S4P::logger("DEBUG", "main: Returned from get_process_end_time(), end: [$end]");
    if ( $end eq "ERROR" ) {
        S4P::perish(50, "main: Could not compute processing end time.\n");
    }

    S4P::logger("DEBUG", "main: Calling mk_work_order_header()");
    mk_work_order_header($start, $end, $algorithm->post_processing_offset,
                         $algorithm->pre_processing_offset,$algorithm->processing_start);
    S4P::logger("DEBUG", "main: Returned from mk_work_order_header()");

    S4P::logger("DEBUG", "main: Before calling mk_work_order_body:");
    foreach my $input (@inputs) {
        my $msg_str = "UR: 'INSERT_UR_HERE', Data Type: " . $input->data_type .
                      ", Data Version: " . $input->data_version . ", Need: " . $input->need .
                      ", Timer: " . $input->timer . ", LUN: " . $input->lun .
                      ", Currency: " . $input->currency;
        S4P::logger("DEBUG", "main: mk_work_order_body: $msg_str");
    }
    S4P::logger("DEBUG", "main: Calling mk_work_order_body()");
    mk_work_order_body($start, $iteration, $algorithm->trigger_coverage, 
                       $algorithm->product_coverage, 
                       $algorithm->apply_leapsec_correction, 
                       $algorithm->processing_start,
                       $algorithm->pre_processing_offset, 
                       $algorithm->leapsec_datatypes,
                       @inputs
                      );
    S4P::logger("DEBUG", "main: Returned from mk_work_order_body()");

    S4P::logger("DEBUG", "main: Calling mk_work_order_end()");
    mk_work_order_end($start, $algorithm->post_processing_offset, $wo_prefix);
    S4P::logger("DEBUG", "main: Returned from mk_work_order_end()");

    S4P::logger("DEBUG", "main: Calling clean_work_order()");
    clean_work_order();
    S4P::logger("DEBUG", "main: Returned from clean_work_order()");

}	# End loop over number of output FIND work orders

S4P::logger("INFO", "********** s4pm_select_data.pl completed successfully! **********");

################################################################################

sub clean_work_order {

################################################################################
#                                clean_work_order                              #
################################################################################
# PURPOSE: To pop off an added PDR files                                       #
################################################################################
# DESCRIPTION: clean_work_order undoes what $PDR->add_file does, popping off   #
#              every file add. It then resets the $NUMFILEGROUPS to 1.         #
################################################################################
# RETURN: None                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::S4P::PDR::pop_granule                                            #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    S4P::logger("DEBUG", "clean_work_order(): Entering clean_work_order()");

    S4P::logger("DEBUG", "clean_work_order(): NUMFILEGROUPS: [$NUMFILEGROUPS]");
    for (my $i = 0; $i < ($NUMFILEGROUPS - 1); $i++) {
        $PDR->pop_granule();
    }

    $NUMFILEGROUPS = 1;		# Reset

    S4P::logger("DEBUG", "clean_work_order(): Leaving clean_work_order()");

}

sub dump_config {

    my $configfile = shift;

    my $str = ""; 
    my $line;

    open(CFG, "../select_data_cfg/$configfile") or 
        S4P::perish(100, "dump_config(): Could not open select data configuration file: ../select_data_cfg/$configfile: $!");
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

sub get_num_work_orders {

################################################################################
#                             get_num_work_orders                              #
################################################################################
# PURPOSE: To determine the number of output FIND work orders to produce       #
################################################################################
# DESCRIPTION: get_num_work_orders determines the number of output work orders #
#              to produce based upon the processing period of the algorithm    #
#              and the temporal coverage of the data. The data used is the     #
#              trigger data type, that is, the data type whose SELECT          #
#              work order results in a new algorithm being triggered to run.   #
################################################################################
# RETURN: $num - The number of work orders                                     #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($TriggerCoverage, $ProductCoverage) = @_;

    S4P::logger("DEBUG", "get_num_work_orders(): Entering get_num_work_orders() with TriggerCoverage: [$TriggerCoverage], ProductCoverage: [$ProductCoverage]");

    my $period;
    my $coverage;
    if ( $PROCESSINGPERIOD == 0 and $TriggerCoverage == 0 ) {
        S4P::logger("INFO", "get_num_work_orders(): Processing period and trigger data coverage are set to zero. Using product coverage and SELECT start and end times to determine number of output work orders to process.");
        $period = S4P::TimeTools::CCSDSa_Diff($SELECT_FILEGROUP->data_start, $SELECT_FILEGROUP->data_end);
        if ( $period eq undef or $period < 0 ) {
            S4P::perish(100, "get_num_work_orders(): Difference between data start and data end in SELECT work order was less than zero or undefined. ACTION: Check trgger input start and end times. It may be too small to process.");
        }
        $coverage = $ProductCoverage;
    } else {
        $period = $PROCESSINGPERIOD;
        $coverage = $TriggerCoverage;
    }

    my $num = int($coverage / $period + 0.5);
    if ( $num < 1 ) { $num = 1; }

    S4P::logger("DEBUG", "get_num_work_orders(): Leaving get_num_work_orders() with num: [$num]");
    return $num;

}

sub get_process_end_time {

################################################################################
#                            get_process_end_time                              #
################################################################################
# PURPOSE: To determine the the algorithm processing end time                  #
################################################################################
# DESCRIPTION: get_process_end_time determines the processing end time based   #
#              upon the process start time and the processing period of the    #
#              algorithm.                                                      #
#                                                                              #
#              For algorithms requesting a leap second correction to the       #
#              process start and end times, this correction to the end time is #
#              applied here.                                                   #
################################################################################
# RETURN: $new_time - The process end date/time                                #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::TimeTools::CCSDSa_DateAdd                                        #
#        S4PM::leapsec_correction                                              #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($start, $product_coverage, $leapsec_correction_flag, $processing_start) = @_;

    S4P::logger("DEBUG", "get_process_end_time(): Entering get_process_end_time() with start: [$start]");

    my $new_time ;
    if ($processing_start eq "START_OF_MONTH") {
        my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($start,32*86400) ;
        $new_time = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
    } elsif ($processing_start eq "START_OF_AIRS_PENTAD") {
        my $nstart = S4P::TimeTools::CCSDSa_DateRound($start,86400) ;
        my ($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($nstart) ;
        if ($day < 26) {
           $new_time = S4P::TimeTools::CCSDSa_DateAdd($start,5*86400) ;
        } else {
            my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($start,32*86400) ;
            $new_time = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
        }
    } else {
        my $period;
        if ( $PROCESSINGPERIOD == 0 ) {
            $period = S4P::TimeTools::CCSDSa_Diff($SELECT_FILEGROUP->data_start, $SELECT_FILEGROUP->data_end);
            if ( $period eq undef or $period < 0 ) {
                S4P::perish(100, "get_process_end_time(): Difference between data start and data end in SELECT work order was less than zero or undefined.");
            }
        } else {
            $period = $PROCESSINGPERIOD;
        }

#   Back out start leapsec correction, and apply independently to new_time

        if ( $leapsec_correction_flag ) {
           $start = S4PM::leapsec_correction($start, 1, -1);
        }
        S4P::logger("DEBUG", "get_process_end_time(): After removing leapsec corrections, start: [$start]");

#   Add period to start to get end time

        $new_time = S4P::TimeTools::CCSDSa_DateAdd($start, $period);
        if ( $new_time eq "ERROR" ) {
            S4P::perish(50, "get_process_end_time(): S4P::TimeTools::CCSDSa_DateAdd: Failed. Inputs to S4P::TimeTools::CCSDSa_DateAdd were: Start Time: [$start], Processing Period: [$PROCESSINGPERIOD]");
        }
    }

### Make any further leap second corrections

    S4P::logger("DEBUG", "get_process_end_time(): Before any corrections, start: [$new_time]");
    if ( $leapsec_correction_flag ) {
       $new_time = S4PM::leapsec_correction($new_time, 1, 1);
    }
    S4P::logger("DEBUG", "get_process_end_time(): After any corrections, start: [$new_time]");

    S4P::logger("DEBUG", "get_process_end_time(): Leaving get_process_end_time() with new_time: [$new_time]");

    return $new_time;

}

sub get_process_start_time {

################################################################################
#                            get_process_start_time                            #
################################################################################
# PURPOSE: To determine the algorithm processing end time.                     #
################################################################################
# DESCRIPTION: get_process_start_time gets the processing start time based upon#
#              the data start time of the input SELECT work order. For         #
#              algorithms whose processing period is less than the temporal    #
#              coverage of the input file, multiple processing start times     #
#              are generated (for multiple output work orders). The $iteration #
#              determines the offset needed when this occurs.                  #
#                                                                              #
#              For algorithms requesting a leap second correction to the       #
#              process start and end times, this correction to the start time  #
#              is applied here.                                                #
################################################################################
# RETURN: $start - The procss start date/time                                  #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::S4P::PDR::data_start                                             #
#        S4P::TimeTools::CCSDSa_DateAdd                                        #
#        S4PM::leapsec_correction                                              #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($iteration, $trigger_coverage, $product_coverage, 
        $leapsec_correction_flag, $pre_processing_offset, 
        $processing_start) = @_;

    S4P::logger("DEBUG", "get_process_start_time(): Entering get_process_start_time() with iteration: [$iteration]");

    my $start;

### First, test to see of the file accumulation rule is being invoked

    my $accum_start = $algorithm->accumulation_start($SELECT_FILEGROUP->data_start);

    if ( $accum_start ) {

        $start = $accum_start;

#   Apply pre-processing offset to start time

        $start = S4P::TimeTools::CCSDSa_DateAdd($start, $pre_processing_offset);

### If processing start time is set (and we're not using file accumulation 
### rule as in first test above), then start is fixed to a time of day and 
### NOT to the input data in the SELECT work order, then determine it.

    } elsif ( $processing_start ) {

####### First, let's take care of the situation where the start and end times 
####### straddle a day boundary. If it does, we used the end time to define 
####### what day we're in. Otherwise, we use the start time.
####### CORRECTION:  Always use the file begin time.

        my (undef, undef, $startday, undef, undef, undef, $error) = S4P::TimeTools::CCSDSa_DateParse($SELECT_FILEGROUP->data_start);
        if ( $error ) {
            S4P::perish(30, "get_process_start_time(): Failed to parse start time in SELECT work order.");
        }
        my (undef, undef, $endday, undef, undef, undef, $error) = S4P::TimeTools::CCSDSa_DateParse($SELECT_FILEGROUP->data_end);
        if ( $error ) {
            S4P::perish(30, "get_process_start_time(): Failed to parse end time in SELECT work order.");
        }
        my $basetime = $SELECT_FILEGROUP->data_start;

        S4P::logger("DEBUG","get_process_start_time(): processing_start: $processing_start") ;
        if ( $processing_start eq "START_OF_DAY" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "day", 1);
        } elsif ( $processing_start eq "START_OF_WEEK" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "week", 1);
        } elsif ( $processing_start eq "START_OF_MONTH" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "month", 1);
        } elsif ( $processing_start eq "START_OF_HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 1);
        } elsif ( $processing_start eq "START_OF_12HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 12);
        } elsif ( $processing_start eq "START_OF_6HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 6);
        } elsif ( $processing_start eq "START_OF_4HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 4);
        } elsif ( $processing_start eq "START_OF_3HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 3);
        } elsif ( $processing_start eq "START_OF_2HOUR" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "hour", 2);
        } elsif ( $processing_start eq "START_OF_MIN" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "min", 1);
        } elsif ( $processing_start eq "START_OF_SEC" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($basetime, "sec", 2);
        } elsif ( $processing_start eq "START_OF_AIRS_8DAY" ) {
            my $epoch = "1993-01-01T00:00:00.0Z" ;
            my $diff = S4P::TimeTools::CCSDSa_Diff($epoch,$basetime) ;
            my $ndays = int($diff/86400) - 1 ;
            $start = S4P::TimeTools::CCSDSa_DateAdd($epoch,86400*($ndays+2-($ndays % 8))) ;
        } elsif ( $processing_start eq "START_OF_AIRS_PENTAD" ) {
            my ($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($basetime) ;
            my $pentad = int(($day-1)/5) ;
            if ($pentad == 6) { $pentad = 5 ; }
            $day = 1 + 5 * $pentad ;
            $start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, 0, 0, 0) ;
        }

#   Apply pre-processing offset to start time

        $start = S4P::TimeTools::CCSDSa_DateAdd($start, $pre_processing_offset);

### Otherwise, we base the process start time on the data start time in the
### SELECT work order. If a pre-processing offset is included, we add 
### it here as well.

    } else {

        $start = S4P::TimeTools::CCSDSa_DateAdd($SELECT_FILEGROUP->data_start, $pre_processing_offset);
        S4P::logger("DEBUG", "get_process_start_time(): Original start time [$start]");
    }

### For where there is more than one work order to be output, we loop adding a 
### processing period of time to the start of each. Where there is only one 
### work order output, we effectively bypass this loop (since $interation = 0).

    for ( my $i = 0; $i < $iteration; $i++ ) {
        $start = S4P::TimeTools::CCSDSa_DateAdd($start, $PROCESSINGPERIOD);
        if ( $start eq "ERROR" ) {
            S4P::perish(50, "get_process_start_time(): S4P::TimeTools::CCSDSa_DateAdd: Failed. Inputs to S4P::TimeTools::CCSDSa_DateAdd were: Start Time: [$start], Processing Period: [$PROCESSINGPERIOD]");
        }
    }

### Make any further leap second corrections

    S4P::logger("DEBUG", "get_process_start_time(): Before any corrections, start: [$start]");
    if ( $leapsec_correction_flag ) {
        $start = S4PM::leapsec_correction($start, 1, 1);
    }
    S4P::logger("DEBUG", "get_process_start_time(): After any corrections, start: [$start]");

    S4P::logger("DEBUG", "get_process_start_time(): Leaving get_process_start_time() with start: [$start]");

    return $start;

}

sub is_addable {

################################################################################
#                                 is_addable                                   #
################################################################################
# PURPOSE: Make final determinatation if file should be added to the           #
#          FIND work order                                                     #
################################################################################
# DESCRIPTION: is_addable handles algorithm-specific issues. It was designed to#
#              really handle MoPGE01 where FIND work orders after the          #
#              first one should omit the posting of the previous input         #
#              files (even as an option). This sub should work to handle       #
#              other algorithm-specific issues as well.                        #
################################################################################
# RETURN: $ret - 0 means do NOT add the file                                   #
#                1 means do add the file                                       #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: mk_work_order_body                                                #
################################################################################

    my ($iteration, $datatype, $currency, $start) = @_;
    my $ret = 1;

    S4P::logger("DEBUG", "is_addable(): Entering is_addable() with iteration: [$iteration], datatype: [$datatype], currency: [$currency], start: [$start]");

####### For MoPGE01, exclude "PREV" MOD000 and s/c anc product dependencies
####### for runs not occurring at gran boundary (i.e., with iteration > 0)

    if ( $PGE eq "MoPGE01" and $currency =~ /^PREV/ ) {
        if ( $iteration > 0 and
             ($datatype eq "MOD000" or $datatype eq "AM1ATTNF" or
              $datatype eq "AM1ATTN0" or $datatype eq "AM1EPHN0") ) {
            $ret = 0 ;
        }
    }

####### PM1EPHND is a 24-hr file going from 12:00 to 12:00 the next day.
####### It is only needed for runs that occur on this noon boundary.
####### For MODPML0 and PM1ATTNR, apply same logic as for Terra.

    if ( $PGE eq "MyPGE01" and $currency =~ /^PREV/ ) {
        if ( $iteration > 0 and
             ($datatype eq "MODPML0" or $datatype eq "PM1ATTNR") ) {
            $ret = 0;
        }
        if ( $datatype eq "PM1EPHND" and
             !( $start =~ /12:00:00Z$/ and $iteration == 0 ) ) {
            $ret = 0;
        }
    }

####### Similar considerations for AIRS L1A and PM1EPHND
####### Also need to suppress "PREV" dependencies where $iteration >0

    if ( $PGE =~ /^AiL1A/ and $currency =~ /^PREV/ ) {
        if ( $iteration > 0 and
             ($datatype eq "PMCO_HK" or $datatype eq "PM1ATTNR" or
              $datatype =~ /^AIR/) ) {
            $ret = 0;
        }
        if ( !$NRTFLAG and $datatype eq "PM1EPHND" and
             !( $start =~ /11:59:\d\dZ$/ and $iteration == 0 ) ) {
            $ret = 0;
        }
    }

    S4P::logger("DEBUG", "is_addable(): Leaving is_addable() with ret: [$ret]");
    return $ret;

}


sub mk_work_order_body {

################################################################################
#                             mk_work_order_body                               #
################################################################################
# PURPOSE: Make the bulk of the output FIND work order                         #
################################################################################
# DESCRIPTION: mk_work_order_body handles most the production rule issues based#
#              upon the confirguration data for that algorithm. First, the     #
#              configuration data is read in and parsed. Then for each input   #
#              file specified in the configuration file, a determination is    #
#              made as to what the requested data times should be in the output#
#              FIND work order. A call to is_addable() makes a final           #
#              determination as to whether the entry should be included in     #
#              the output work order.                                          #
################################################################################
# RETURN: none                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        is_addable                                                            #
#        get_process_start_time                                                #
#        S4P::TimeTools::getDataTimes                                          #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($process_start, $iteration, $trigger_coverage, $product_coverage,
        $apply_leapsec_correction, $processing_start_override, 
        $pre_processing_offset, $leapsec_datatypes, @inputs
       ) = @_;

    S4P::logger("DEBUG", "mk_work_order_body(): Entering mk_work_order_body() with work order number: [$iteration]");

    my ($ur, $need, $data_type, $data_version, $data_start, $data_end, $lun);
    my ($start, $inputkey);
    my ($expansions, $first, $last);
    my ($data_type, $data_version, $need, $currency, $lun, $boundary);
    my ($coverage, $timer);
    my ($new_data_type, $new_data_version, $new_need, $new_currency, $new_lun); 
    my ($new_boundary, $new_coverage, $new_timer);

    S4P::logger("DEBUG", "mk_work_order_body(): List of inputs from configuration file:");
    foreach my $input (@inputs) {
        my $msg_str = "UR: 'INSERT_UR_HERE', Data Type: " . $input->data_type .
                      ", Data Version: " . $input->data_version . ", Need: " . 
                      $input->need .
                      ", Timer: " . $input->timer . ", LUN: " . $input->lun .
                      ", Currency: " . $input->currency;
        S4P::logger("DEBUG", "mk_work_order_body(): $msg_str");
    }

    foreach my $input (@inputs) {
        if ( $input->need eq "TRIG" and $input->coverage < $trigger_coverage ) {
            $PDR->pop_granule();
        }
    }

### Big loop. Loop over each input needed for the algorithm and specified in the
### algorithm select data configuration file

    my $accumulate_flag;

    foreach my $input (@inputs) {
        $data_type    = $input->data_type;
        $data_version = $input->data_version;
        $need         = $input->need;
        $currency     = $input->currency;
        $lun          = $input->lun;
        $boundary     = $input->boundary;
        $coverage     = $input->coverage;
        $timer        = $input->timer;

        if ( $need =~ /^REQIF/ ) {
            my $trig_file = $SELECT_FILEGROUP->file_specs->[0]->directory_id . "/" . $SELECT_FILEGROUP->file_specs->[0]->file_id;
            if ( pass_trigger_test($trig_file, $input->test) ) {
                $need =~ s/REQIF/REQ/;
                S4P::logger("INFO", "mk_work_order_body(): Test " . $input->test . " on $trig_file succeeded. Therefore, this production rule is being retained.");
            } else {
                S4P::logger("INFO", "mk_work_order_body(): Test " . $input->test . " on $trig_file failed. Therefore, this production rule is being dropped.");
                next;
            }
        }

        S4P::logger("DEBUG", "mk_work_order_body(): Next Input is $data_type, $data_version: with a need of: $need, a currency of: $currency, a timer of: $timer, a LUN of: $lun, and a boundary of: $boundary");

        my $expand_flag = 0;

### The accumulate flag indicates that we're dealing with a algorithm whose 
### trigger data is shorter than the processing period (e.g. a daily Browse 
### algorithm triggered by a single 5-min file). By default, we assume that 
### the processing period is shorter than the trigger data coverage.

        if ( $need eq "TRIG" and $coverage < $trigger_coverage ) {
            $accumulate_flag = 1;
        } else {
            $accumulate_flag = 0;
        }

####### If the need is set to "TRIG", it is equivalent to "REQ1". We
####### just need to regurgitate the stuff already saved in the original
####### SELECT file group with minor modifications. The exception to this
####### is if the accumulate flag is set. In that case, that single trigger
####### data file needs to be expanded to fill up the processing period.

####### First, the simple case:

        if ( $need eq "TRIG" and $accumulate_flag == 0 ) {
            $SELECT_FILEGROUP->need('REQ1');
            $SELECT_FILEGROUP->timer($timer);
            $SELECT_FILEGROUP->lun($lun);
            $SELECT_FILEGROUP->currency($currency);
            $SELECT_FILEGROUP->boundary($boundary);

####### Otherwise, we add a new file group for a new file to the PDR.

        } else {        # Input has need other than TRIG or accumulate is on

########### If the accumlate flag is set, we need to pop off the original
########### file from the input work order since the code following will
########### put it back in there. If we don't pop, we end up with a redundant
########### file group in the output work order.

            if ( $need eq "TRIG" ) { $need = "OPT1"; }
            if ( $accumulate_flag == 1 ) {
                $accumulate_flag = 0;	# Now unset so this doesn't happen again
            }

########### A currency of NPREVn or NFOLLn is expanded into an equivalent
########### number of PREVn or FOLLn, respectively. If currency is set to
########### one of these, figure out how many this expands to. If currency
########### is something else, then we effectively don't expand.

            if ( $currency =~ /^NPREV([0-9]+),([0-9]+)/ or
                 $currency =~ /^NFOLL([0-9]+),([0-9]+)/ ) {
                $first = $1;
                $last  = $2;
                $expansions  = $last - $first + 1;
                $expand_flag = 1;
            } else {
                $first = 1;
                $last  = 1;
                $expansions  = 1;
                $expand_flag = 0;
            }
            S4P::logger("DEBUG", "mk_work_order_body(): Before expansions: Datatype: $data_type, $data_version and currency of $currency, expand flag has been set to [$expand_flag], number of expansions: $expansions with $first being the first and $last being the last, and need: $need");

            for (my $i = $first; $i <= $last; $i++) {
                S4P::logger("DEBUG", "mk_work_order_body(): Expansion iteration is: [$i]");

                if ( $expand_flag and $i == 0 ) {
                    $new_currency = "CURR";
                    $new_need = $need . "1";
                } elsif ( $expand_flag and $i > 0 ) {
                    if ( $currency =~ /^NPREV/ ) {
                        $new_currency = "PREV" . $i;
                        $new_need = $need . $i;
                    } elsif ( $currency =~ /^NFOLL/ ) {
                        $new_currency = "FOLL" . $i;
                        $new_need = $need . $i;
                    }
                } else {
                    $new_currency     = $currency;
                    $new_need         = $need;
                }
                $new_data_type    = $data_type;
                $new_data_version = $data_version;
                $new_lun          = $lun;
                $new_boundary     = $boundary;
                $new_coverage     = $coverage;
                $new_timer        = $i * int($timer/$expansions);

                S4P::logger("DEBUG", "mk_work_order_body(): After expansions: Datatype: $new_data_type, $new_data_version and currency of $new_currency, expand flag has been set to [$expand_flag], number of expansions: $expansions with $first being the first and $last being the last, and need: $new_need");

############### Here, we get the process start time again for the purposes
############### of computing the data times (not for setting LUNs 10258 and
############### 10259; that is done already). For data types listed in
############### LEAPSEC_DATATYPES (select data config file), we apply the
############### leap second and AIRS instrument offset corrections also.

                S4P::logger("DEBUG", "mk_work_order_body(): leapsec_datatypes: [$leapsec_datatypes], new_data_type: [$new_data_type]");
                if ( $leapsec_datatypes ) {
                    if ( $leapsec_datatypes =~ /$new_data_type/ ) {
                        $start = get_process_start_time($iteration, $trigger_coverage, $product_coverage, 1, $pre_processing_offset, $processing_start_override);
                    } else {
                        S4P::logger("DEBUG", "mk_work_order_body(): data type: $new_data_type is not in list of leapsec data types: $leapsec_datatypes");
                        $start = get_process_start_time($iteration, $trigger_coverage, $product_coverage, 0, $pre_processing_offset, $processing_start_override);
                    }
                } else {
                    $start = get_process_start_time($iteration, $trigger_coverage, $product_coverage, 0, $pre_processing_offset, $processing_start_override);
                }

############### Get the start and end times of the data file needed

                S4P::logger("DEBUG", "mk_work_order_body(): Calling get_data_times() with start of: [$start]");
                my @dtime_pairs = S4P::TimeTools::get_data_times($input->coverage, $start, $PROCESSINGPERIOD, $new_currency, $new_boundary);
                my $kounter = 0;
                foreach my $pair (@dtime_pairs) {
                    $kounter++;
                    my ($s, $e) = split(/,/, $pair);
                    if ( $s eq "ERROR" or $e eq "ERROR" ) {
                        S4P::perish(50, "mk_work_order_body(): S4P::TimeTools::get_data_times: Failed. Inputs to S4P::TimeTools::get_data_times were: Coverage: [$new_coverage], Start: $start Processing Period: $PROCESSINGPERIOD, Currency: [$new_currency], and Boundary: [$new_boundary]");
                    }

                    if ( $new_need eq "REQ" ) { $new_need = "REQ1"; }

################### First, we see if adding this new file (or files if
################### that's what it expands to) to the PDR gets allowed by
################### is_addable(). This is the routine that incorporates
################### algorithm-specific stuff that shouldn't be considered a 
################### general "production rule" feature.

                    if ( is_addable($iteration, $new_data_type, $new_currency, $process_start) ) {
                        S4P::logger("DEBUG", "mk_work_order_body(): $new_data_type with currency of $new_currency IS addable and will be included in PDR.");

####################### Now, add the file to the PDR as a new S4P::PDR stanza

                        my $msg_str = "UR: 'INSERT_UR_HERE', Data Type: $new_data_type, Data Version: $new_data_version, Need: $new_need, Timer: $new_timer, LUN: $new_lun, Currency: $new_currency";
                        S4P::logger("DEBUG", "mk_work_order_body(): Adding the following file to the PDR: $msg_str");

####################### If the nearest file production rules is specified,
####################### (with NPREVm,n or NFOLLm,n) then we want to write out
####################### the currency in the output work order in the form of
####################### NPREVm,n or NFOLLm,n. That is, we don't convert it
####################### into the equivalent PREVn or FOLLn entries. In
####################### s4pm_find_data.pl, hash keys are LUN+currency. Thus, 
####################### this will result in a single hash key which, in turn,
####################### means that one found file will cause the rule to
####################### be satisfied. If we didn't do this,the result would
####################### be a unique hash key for each. ALL files would then
####################### be looked for.

                        my $written_currency;
                        if ( $currency =~ /^NPREV/ or $currency =~ /^NFOLL/ ) {
                            $written_currency = $currency;
                        } else {

########################### If more than one data file is needed to fill up
########################### the processing period, then append $kounter to
########################### the currency. This will make the currencies all
########################### unique for this LUN in the output PDR (s4pm_find_data.pl
########################### will make use of this fact). Otherwise, do not
########################### append $kounter.

                            if ( scalar(@dtime_pairs) > 1 ) {
                                $written_currency = $new_currency . $kounter;
                            } else {
                                $written_currency = $new_currency;
                            }
                        }

                        $PDR->add_granule(
                            'ur'           => 'INSERT_UR_HERE',
                            'data_type'    => $new_data_type,
                            'data_version' => $new_data_version,
                            'need'         => $new_need,
                            'timer'        => $new_timer,
                            'lun'          => $new_lun,
                            'data_start'   => "$s",
                            'data_end'     => "$e",
                            'boundary'     => $new_boundary,
                            'currency'     => "$written_currency",
                            'files' => ['INSERT_DIRECTORY_HERE/INSERT_FILE_HERE']
                        );
                        $NUMFILEGROUPS++;
                        my @filegroups = @{$PDR->file_groups};
                        S4P::logger("DEBUG", "mk_work_order_body(): CURRENT FILE GROUPS:\n" . S4P::PDR::show_file_groups(@filegroups) );

                    } else {    # the input is NOT addable

####################### The is_addable() routine determined that this file
####################### (or files) should NOT be added to the PDR due to
####################### algorithm-specific logic

                        S4P::logger("DEBUG", "mk_work_order_body(): $new_data_type with currency of $new_currency is NOT addable and will NOT be included in PDR.");

                    }   # end if file is addable

                }               # end loop over start/end data time pairs

            }           # end for loop over number of expansions

        }       # end if input has need of TRIG

    }   # end loop over each input in the select data configuration file

    S4P::logger("DEBUG", "mk_work_order_body(): Leaving mk_work_order_body()");
}

sub mk_work_order_end {

################################################################################
#                              mk_work_order_end                               #
################################################################################
# PURPOSE: To wrap up the output FIND work order and write it out              #
################################################################################
# DESCRIPTION: mk_work_order_end determines the output FIND work order         #
#              file name and then writes it out.                               #
################################################################################
# RETURN: None                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::S4P::PDR::write_pdr                                              #
#        S4P::TimeTools::CCSDSa_DateParse                                      #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($start, $post_processing_offset, $wo_prefix) = @_;

    S4P::logger("DEBUG", "mk_work_order_end(): Entering mk_work_order_end() with start: [$start]");

### If there is a post processing offset, then we want the work order file 
### name to reflect that.

    my $new_start = S4P::TimeTools::CCSDSa_DateAdd($start, $post_processing_offset);
    if ( $new_start eq "ERROR" ) {
        S4P::perish(50, "mk_work_order_end(): S4P::TimeTools::CCSDSa_DateAdd: Invalid start time after adding in POST_PROCESSING_OFFSET. Inputs to S4P::TimeTools::CCSDSa_DateAdd were: Start: [$start] and Post Processing Offset: " . $post_processing_offset);
    }

    my ($year, $month, $day, $hour, $min, $sec, $error) =
        S4P::TimeTools::CCSDSa_DateParse($new_start);

    if ( $error ) {
        S4P::logger("WARNING", "mk_work_order_end(): S4P::TimeTools::CCSDSa_DateParse: Failed to parse start time to make a unique output work order name. Input to S4P::TimeTools::CCSDSa_DateParse was: Start: [$new_start]");
    }

    my $doy = S4P::TimeTools::day_of_year($year, $month, $day);

### We want to always have 3-digit day of year values

    if ( length($doy) == 2 ) { $doy = "0"  . $doy; }
    if ( length($doy) == 1 ) { $doy = "00" . $doy; }

    my $OutputWorkorder = "$wo_prefix" . "$PGE." . "$year$doy$hour$min$sec" . ".wo";
    if ( -e "$OutputWorkorder" ) {
        my $pid = getppid;
        S4P::logger("WARNING", "mk_work_order_end(): Output work order: $OutputWorkorder already exists. Creating a new name using process id.");
        $OutputWorkorder = "$wo_prefix" . "$PGE." . "$year$doy$hour$min$sec" . ".$pid" . ".wo";
    }

    S4P::logger("DEBUG", "mk_work_order_end(): doy: [$doy], OutputWorkorder: [$OutputWorkorder]");

    $PDR->write_pdr($OutputWorkorder);

    S4P::logger("DEBUG", "mk_work_order_end(): Leaving mk_work_order_end()");
}

sub mk_work_order_header {

################################################################################
#                             mk_work_order_header                             #
################################################################################
# PURPOSE: To create the top portion of the output FIND work order             #
################################################################################
# DESCRIPTION: mk_work_order_header determines and sets the FIND work          #
#              order PDR objects (those not in a FILE_GROUP object).           #
################################################################################
# RETURN: None                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::TimeTools::CCSDSa_DateAdd                                        #
#        S4P::S4P::PDR::processing_start                                       #
#        S4P::S4P::PDR::processing_stop                                        #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($start, $end, $post_processing_offset, $pre_processing_offset, $processing_start) = @_;

    S4P::logger("DEBUG", "mk_work_order_header(): Entering mk_work_order_header() with start: [$start], end: [$end]");

    if ( $post_processing_offset ) {
        $start = S4P::TimeTools::CCSDSa_DateAdd($start, $post_processing_offset);
        if ( $start eq "ERROR" ) {
            S4P::perish(50, "mk_work_order_header(): S4P::TimeTools::CCSDSa_DateAdd: Invalid start time after adding in POST_PROCESSING_OFFSET. Inputs to S4P::TimeTools::CCSDSa_DateAdd were: Start: [$start] and Post Processing Offset: " . $post_processing_offset);
        }
        $end   = S4P::TimeTools::CCSDSa_DateAdd($end, $post_processing_offset);
        if ( $end eq "ERROR" ) {
            S4P::perish(50, "mk_work_order_header(): S4P::TimeTools::CCSDSa_DateAdd: Invalid end time after adding in POST_PROCESSING_OFFSET. Inputs to S4P::TimeTools::CCSDSa_DateAdd were: End: [endstart] and Post Processing Offset: " . $post_processing_offset);
        }
    }

    if ($processing_start eq "START_OF_AIRS_PENTAD") {
        $start = S4P::TimeTools::CCSDSa_DateRound($start,86400) ;
        $end = S4P::TimeTools::CCSDSa_DateRound($end,86400) ;
    }

    $PDR->processing_start("$start");
    $PDR->processing_stop("$end");
    if ( $post_processing_offset ) {
        $PDR->post_processing_offset($post_processing_offset);
    } else {
        $PDR->post_processing_offset("0");
    }
    if ( $pre_processing_offset ) {
        $PDR->pre_processing_offset($pre_processing_offset);
    } else {
        $PDR->pre_processing_offset("0");
    }

    S4P::logger("DEBUG", "mk_work_order_header(): Leaving mk_work_order_header()");
}

sub pass_trigger_test {

    my $file = shift;
    my $test = shift;

    if ( $test =~ /^\s*$/ ) {
        S4P::perish(30, "pass_trigger_test(): No test defined for file $file. ACTION: Add the 'test' attribute to the Stringmaker Algorithm configuration file for this algorithm where the 'need' is set to REQIF.");
    }

    my $cmd = $test . " " . $file;
    S4P::logger('DEBUG', "pass_trigger_test(): cmd: [$cmd]");
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger('INFO', "pass_trigger_test(): REQIF test [$test] on $file succeeded.");
        return 1;
    } else {
        S4P::logger('INFO', "pass_trigger_test(): REQIF test [$test] on $file failed.");
        return 0;
    }

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

