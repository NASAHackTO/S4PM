#!/usr/bin/perl

=head1 NAME

s4pm_regular_block.pl - implements several potentially useful blocking rules

=head1 SYNOPSIS

s4pm_regular_block.pl
B<-b> I<boundary>
B<-d> I<duration>
B<-t> I<block_type>
I<PGE>
I<start_time>
I<stop_time>

=head1 DESCRIPTION

Generalized Register Local Data blocking script to check whether specified 
start/stop time for an algorithm meets the specified blocking criteria. Script 
allows use of various types of boundaries, offsets therefrom, and durations.

Depending on the specified block type, the script will either always indicate 
a block if the input times match the specified criteria, or will indicate a 
block only after the first matching request has been processed. One can set 
up absolute blocks for a specified window such that no SELECT is issued in 
that window, or one can set up a block such that only one SELECT is 
issued in that window (such as for AIRS daily summaries or MoPGE55).

=head1 ARGUMENTS

=over 4

=item B<-b> I<boundary>

Specifies the blocking boundary criteria and offset therefrom. The following 
are supported generic boundaries: START_OF_MIN, START_OF_DAY, START_OF_WEEK, 
START_OF_MONTH, START_OF_YEAR.  The AIRS-specific boundaries START_OF_AIRS_8DAY
START_OF_AIRS_PENTAD are also now supported.

Alternately, an absolute boundary in CCSDSa format may be specified.

Offsets may be specified by appending "+" or "-" followed by an integer number 
of seconds offset value.

Examples:  START_OF_DAY
           START_OF_WEEK+86400
           2002-07-04T00:00:00Z

=item B<-d> I<duration>

Specifies the span of the block, in integral seconds. May be positive or 
negative.

=item B<-t> I<block_type>

Flag to control whether the block is absolute (block_type = 0) or 
block-after-first (block_type = 1). With the former, the script will return 
1 for all requests that satisfy the specified boundary/duration criteria.  
With the block-after-first option, the first request that satisfies the 
specified criteria will return 0, but subsequent qualifying requests will 
return 1.

=item I<PGE>

Name of daily summary algorithm to check blocking for:  specifically,
one of AiBr_AIRS, AiBr_AMSU, AiBr_HSB, AiBr_L2Ret, AiBr_L2CC, AiVISMap1D,
or MoPGE55,

=item I<start_time>

Begin time of period to be evaluated, in CCSDSa YYYY-MM-DDTHH:MM:SS.dddd format

=item I<stop_time>

End time of period to be evaluated, in CCSDSa YYYY-MM-DDTHH:MM:SS.dddd format

=back

=head1 EXIT STATUS VALUES

=head2 Status = 0

No block was found for the specified time range for the specified algorithm.
One was subsequently created, by way of instantiating one or more directories
corresponding to the specific instances of the algorithm that are covered
by the input begin/end times. Example:

    $BLOCKINGROOT/AiBr_AIRS/2002-07-14T00:00:00Z_2002-07-15T00:00:00Z/

=head2 Status = 1

An existing block was found.

=head2 Status other than 0 or 1.

An internal script error was encountered.

=head1 AUTHOR

Mike Theobald

=cut

################################################################################
# s4pm_regular_block.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use vars qw($opt_b $opt_d $opt_t) ;
use Getopt::Std;
use S4PM::Blocking;
use S4P;
use S4P::TimeTools;
use strict ;

my ($PGE, $t_start, $t_stop) ;
my ($boundary, $sign, $offset, $duration, $block_type) ;
my ($year, $month, $day, $hour, $min, $sec, $error) ;
my ($b_start, $b_stop, $block_dir) ;
my $status ;
my $CONFIGFILE = "../s4pm_regular_block.cfg" ;
my $BLOCKINGROOT = undef ;

# get command line switch args that describe the blocking criteria

getopts('b:d:t:') ;

unless (defined($opt_b)) { S4P::perish(30, "$0: Failure to specify -b block boundary on command line.") ; }
unless (defined($opt_d)) { S4P::perish(31, "$0: Failure to specify -d block duration on command line.") ; }
unless (defined($opt_t) ){ S4P::perish(32, "$0: Failure to specify -t block action type on command line.") ; }

$duration = $opt_d ;
$block_type = $opt_t ;

# The rest of the command line args are the specific times to be tested

$PGE = shift ;
$t_start = shift ;
$t_stop = shift ;
$t_start =~ s/\.\d+Z$/Z/ ;
$t_stop =~ s/\.\d+Z$/Z/ ;

# load config file (contains BLOCKINGROOT directory in which blocks are created)

my $string = S4P::read_file("$CONFIGFILE");
if ( $string ) {
    eval ($string);
} else {
    $BLOCKINGROOT = "../Blocks" ;
    S4P::logger('INFO', "Unable to read configfile.  Using $BLOCKINGROOT for BLOCKINGROOT") ;
}

# If necessary, set up directories in which to create blocks
$block_dir = "$BLOCKINGROOT/$PGE" ;
unless (-d "$BLOCKINGROOT") { mkdir("$BLOCKINGROOT",0775) ; S4P::logger('INFO',"Created $BLOCKINGROOT") ; }
unless (-d "$block_dir") { mkdir("$block_dir",0775) ; S4P::logger('INFO',"Created $block_dir") ; }


# Use PGE end time as the basis for testing the block criteria
# (end time needed for AIRS; should be suitable for MODIS as well)

$b_start = $t_start ;

# Extract components of time to be tested

($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($b_start) ;
if ($error != 0)
{
  S4P::perish(33, "$0: Error returned from S4P::TimeTools::CCSDSa_DateParse: $error") ;
}

# Determine start of block.

$offset = undef ;

if ($opt_b =~ /START/) {
# boundary criteria are relative

    ($boundary,$sign,$offset) = split /([+-])/,$opt_b ;
    if ($boundary eq "START_OF_MIN") { $sec = 0 ; }
    if ($boundary eq "START_OF_HOUR") { $sec = 0 ; $min = 0 ; }
    if ($boundary eq "START_OF_12HOUR") {
        $sec = 0 ;
        $min = 0 ;
        $hour = 12 * int ($hour/12) ;
    }
    if ($boundary eq "START_OF_6HOUR") {
        $sec = 0 ;
        $min = 0 ;
        $hour = 6 * int ($hour/6) ;
    }
    if ($boundary eq "START_OF_4HOUR") {
        $sec = 0 ;
        $min = 0 ;
        $hour = 4 * int ($hour/4) ;
    }
    if ($boundary eq "START_OF_3HOUR") {
        $sec = 0 ;
        $min = 0 ;
        $hour = 3 * int ($hour/3) ;
    }
    if ($boundary eq "START_OF_2HOUR") {
        $sec = 0 ;
        $min = 0 ;
        $hour = 2 * int ($hour/2) ;
    }
    if ($boundary eq "START_OF_DAY") { $sec = 0 ; $min = 0 ; $hour = 0 ; }
    if ($boundary eq "START_OF_WEEK") { $sec = 0 ; $min = 0 ; $hour = 0 ; }
    if ($boundary eq "START_OF_MONTH") { $sec = 0 ; $min = 0 ; $hour = 0 ; $day = 1 ; }
    if ($boundary eq "START_OF_YEAR") { $sec = 0 ; $min = 0 ; $hour = 0 ; $day = 1 ; $month = 1 ; }
    if ($boundary eq "START_OF_AIRS_8DAY") {
        my $epoch = "1993-01-01T00:00:00.0Z" ;
        my $diff = S4P::TimeTools::CCSDSa_Diff($epoch,$b_start) ;
        my $ndays = int($diff/86400) - 1 ;
        my $start = S4P::TimeTools::CCSDSa_DateAdd($epoch,86400*($ndays+2-($ndays % 8))) ;
        ($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($start) ;
        S4P::perish(33, "$0: Error returned from S4P::TimeTools::CCSDSa_DateParse: $error") unless ($error == 0);
    }
    if ($boundary eq "START_OF_AIRS_PENTAD") {
         $sec = 0 ;
         $min = 0 ;
         $hour = 0 ;
         my $pentad = int(($day-1)/5) ;
         if ($pentad == 6) { $pentad = 5 ; }
         $day = 1 + 5 * $pentad ;
    }

    if (defined($sign) and defined($offset)) {
        if ($sign eq "-") { $offset = - $offset ; }
    }
    print "$opt_b $boundary $sign $offset\n" ;

} elsif ($opt_b =~ /(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)/) {
# boundary criteria are absolute

    $year = $1 ;
    $month = $2 ;
    $day = $3 ;
    $hour = $4 ;
    $min = $5 ;
    $sec = $6 ;

} else {
# error condition

    S4P::perish(34, "$0: Unrecognized boundary type specified: $opt_b") ;

}

unless (defined($offset)) { $offset = 0 ; } 

if ($duration > 0) {
    if ($boundary eq "START_OF_MONTH") {
        $b_start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$offset) ;
        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_start,32*86400) ;
        $b_stop = S4P::TimeTools::CCSDSa_DateFloor($b_stop,'month',1) ;
#        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_stop,-1) ;
    } elsif ($boundary eq "START_OF_AIRS_PENTAD") {
        $b_start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$offset) ;
        if ($day < 26) {
           $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_start,5*86400) ;
        } else {
            my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($b_start,32*86400) ;
            $b_stop = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
        }
    } else {
        $b_start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$offset) ;
        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_start,$duration) ;
    }
} else {
    if ($boundary eq "START_OF_MONTH") {
        $b_start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$offset) ;
        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_start,32*86400) ;
        $b_stop = S4P::TimeTools::CCSDSa_DateFloor($b_stop,'month',1) ;
#        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_stop,-1) ;
    } elsif ($boundary eq "START_OF_AIRS_PENTAD") {
        $b_start = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$offset) ;
        if ($day < 26) {
           $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_start,5*86400) ;
        } else {
            my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($b_start,32*86400) ;
            $b_stop = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
        }
    } else {
        $b_stop = S4P::TimeTools::CCSDSa_DateUnparse($year, $month, $day, $hour, $min, $sec);
        $b_stop = S4P::TimeTools::CCSDSa_DateAdd($b_stop,$offset) ;
        $b_start = S4P::TimeTools::CCSDSa_DateAdd($b_start,$duration) ;
    }
}

# Make a block with the supplied criteria
my $make_status = S4PM::Blocking::MakeBlock($block_dir,$b_start,$b_stop) ;
my $check_status = S4PM::Blocking::CheckBlocks($block_dir,$t_start,$t_stop) ;

if ($block_type == 1) { $status = $check_status ; }
else { $status = $make_status ; }

exit ($status) ;
