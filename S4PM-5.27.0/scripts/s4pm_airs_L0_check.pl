#!/usr/bin/perl

=head1 NAME

s4pm_airs_L0_check.pl - check packet completeness of AIRS L0 data

=head1 SYNOPSIS

s4pm_airs_L0_check.pl
[-B<C>]

=head1 DESCRIPTION

AIRS Level 0 data is composed of 17 APIDs.  These 17 APIDs feed in
certain combination the 3 L1A algorithms; however, depending on the mode the
instruments are operating in, one or more of the 17 may not be present
(in fact, the nominal operating mode only uses 11 of the 17 APIDs).

We think of each algorithm as processing a stream of packets.  For the
AiL1A_HSB algorithm, that stream is made up of a single APID (342, AIRH0ScE).

For AiL1A_AMSU, there are two streams:  one for the A1 instrument and
one for the A2 instrument.  Both instruments can be in either a
non-science mode, a staring mode, or the nominal scanning mode In each
mode the instrument produces different APID data.  For example, for A2,
in non-science mode the APID produced is 288, for staring it's 289, and
for scanning it's 290.  Were the instrument to be cycled between those
modes, we would be able to construct a continuous packet stream from
the three APIDs.  Similarly for the A1 instrument.

For the AIRS instrument, there are two engineering packet streams
(packet 1 and packet 2) which may be in one of two modes (normal or
flex packets).  These four APIDs make up a pair of substitutional pairs.
The other AIRS instrument APIDs should all be present.

The B<s4pm_airs_L0_check.pl> routine examines the set of PDSes staged for
a given two hour interval and assesses the completeness of the packet
streams therein.  It does this by having knowledge of the "combination
rules" describing which APIDs appear in which combination, of the packet
size of each APID, and of the staged file size of the packet file.
Given this info, the routine calculates the total number of packets
staged for each individual packet stream (made up from one or more APIDs),
compares those values with the expected values, and either exits with
success or failure.

This program is meant to be run prior to invoking s4pm_select_data.pl, such
that only complete data sets are passed down through S4PM, with any
shortfalls being trapped for operator visibility and action.

B<s4pm_airs_L0_check.pl> requires both its own configuration file
(s4pm_airs_L0_check.cfg, which describes the stream APID combinations, the
packet size info, and other needed items), as well as access to the
s4pm_allocate_disk.cfg and s4pm_select_data_AiL1A_*.cfg files.

B<s4pm_airs_L0_check.pl> processes SELECT workorders. On successful 
completion, the input SELECT work order is simply passed to 
s4pm_select_data.pl; on error (packet shortfall), a IGNORE_INCOMPLETE workorder 
is created, with which processing through s4pm_select_data.pl can be continued.

The B<-C> option allows the user to run the completeness assessment
from the command line without modifying the input workorder, and without
cycling through any sleep periods.

The program returns one of the following exit codes:
0 (success):  the input workorder references a complete set of inputs
for both the previous and current granules.
60 (failure):  neither the previous nor the current interval contain
a complete set of inputs.
61 (failure):  incomplete previous data, complete current data
62 (failure):  complete previous data, incomplete current data

Normal operation is for s4pm_airs_L0_check.pl and s4pm_select_data.pl to
be called serially from a top-level shell script (e.g., check_n_spec.pl)

=head1 AUTHOR

Dr. Mike Theobald, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_airs_L0_check.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Long;
use Safe;
use S4P;
use S4P::PDR;
use S4P::TimeTools;
use S4PM;
use S4PM::Algorithm;
require 5.6.0;

################################################################################
# Global variables                                                             #
################################################################################

use vars qw(
            $INPUTWORKORDER
            $OUTPUTWORKORDER
            $INPUTPDR
            $CONFIGFILE
            $algorithm
            $airs_cfg
            $spin_interval
            $check_only
            %timer_thresholds_curr_gran
            %timer_thresholds_prev_gran
            %packet_sizes
            %combination_counts
            %pge_combination_map
            %datatype_pool
            %datatype_pool_map
            %datatype_maxsize
            %proxy_esdt_map
);

################################################################################

$check_only = undef ;
$CONFIGFILE = undef ;
$airs_cfg = undef ;
$spin_interval = undef ;
%timer_thresholds_curr_gran = undef ;
%timer_thresholds_prev_gran = undef ;
%packet_sizes = undef ;
%combination_counts = undef ;
%pge_combination_map = undef ;
%datatype_pool = undef ;
%datatype_pool_map = undef ;
%datatype_maxsize = undef ;

GetOptions( "Check"       => \$check_only) ;

if (defined($check_only)) { $check_only = 1 ; }
else { $check_only = 0 ; }

my $pge;                # algorithm name as gleaned from the input work order
                        # file name
my $OutputWorkorder;    # File name of the output work order
my $specify_data_cfg;   # File name of the specify data config file for
                        # the pge defined in the input workorder

################################################################################
# Verify that an input work order (last command-line argument) has been
# specified and that it exists. Note that GetOptions (above) will remove
# from ARGV only those arguments it recognizes and leave the remaining in
# place. Since the input work order is not "recognized", it is guaranteed
# to be the only remaining argument and will thus be the first: $ARGV[0]
################################################################################

foreach (glob("DO.*_AiL1A*"))
{
    $INPUTWORKORDER = $_ ;
}
if ( !defined $INPUTWORKORDER ) {
    S4P::perish(20, "main: No input work order specified! Must be first argument and file name of the input SELECT work order");
}
else {
    unless ( -e $INPUTWORKORDER ) {
        S4P::perish(20, "main: Work order $INPUTWORKORDER doesn't seem to exist!");
    }
    if ( $INPUTWORKORDER =~ /^DO\.SELECT_([^.]+)\./ ) {
        $pge = $1;
        $OUTPUTWORKORDER = $INPUTWORKORDER.".wo" ;
        $OUTPUTWORKORDER =~ s/SELECT/IGNORE_INCOMPLETE/ ;
    }
    elsif ( $INPUTWORKORDER =~ /^DO.IGNORE_INCOMPLETE/ ) {
        $OUTPUTWORKORDER = $INPUTWORKORDER ;
        $OUTPUTWORKORDER =~ s/IGNORE_INCOMPLETE/SELECT /;
        unless ($check_only) { `/bin/cp -f $INPUTWORKORDER $OUTPUTWORKORDER` ; }
        S4P::perish(0, "main: Beginning recovery from incompete inputs.");
    }
    else {
        S4P::perish(20, "main: Work order type is unrecognized!");
    }
}

unless ("AiL1A_AIRS AiL1A_AMSU AiL1A_HSB" =~ /$pge/) {
    S4P::perish(0, "main: Algorithm name $pge in input workorder $INPUTWORKORDER is not an AIRS L1A algorithm.") ;
}

################################################################################
# Read in the configuration files
################################################################################

# alloc_disk.cfg

my $string = S4P::read_file("../s4pm_allocate_disk.cfg");
if ( ! $string ) {
    S4P::perish(30, "main: Failed to read ../s4pm_allocate_disk.cfg. Make sure a link to this file exists in the Select Data station directory.");
}
S4P::logger("INFO", "main: string: [$string]");
eval ($string);
if ( $@ ) {
    S4P::perish(32, "main: Failed to eval string returned from S4P::read_file(): $@");
}

# airs_L0_check.cfg

unless ( $CONFIGFILE ) {
    $CONFIGFILE = "../airs_L0_check,cfg" ;
}

# Load config file containing packet/file details from file

my $string = S4P::read_file("../s4pm_airs_L0_check.cfg");
if ( ! $string ) {
    S4P::perish(30, "main: Failed to read ../s4pm_airs_L0_check.cfg. Make sure this file exists in the Select Data station directory.");
}
S4P::logger("INFO", "main: string: [$string]");
eval ($string);
if ( $@ ) {
    S4P::perish(32, "main: Failed to eval string returned from S4P::read_file(): $@");
}

S4P::logger("INFO", "main: Starting airs_L0_check.pl with this configuration file: $CONFIGFILE");

# select_data pge cfg

$specify_data_cfg = "../select_data_cfg/s4pm_select_data_" . $pge . ".cfg" ;

unless ( -e "$specify_data_cfg" ) {
    S4P::perish(30, "main: No Select Data configuration file seems to exist for algorithm $pge. Looking for $specify_data_cfg.");
}

my $algorithm = new S4PM::Algorithm($specify_data_cfg);
if ( ! $algorithm ) {
    S4P::perish(30, "main: Could not create new S4PM::Algorithm object from reading $specify_data_cfg");
}

S4P::logger("INFO", "********** s4pm_airs_L0_check.pl starting for algorithm $pge **********");

################################################################################
# Get data start time from work order
################################################################################

# Read in the input work order

my $INPUTPDR = new S4P::PDR('text' => S4P::read_file($INPUTWORKORDER));
my @fg = @{$INPUTPDR->file_groups};

### If the input work order does have more than one file group, something has
### gone very wrong and we should bail out.

if ( scalar( @fg ) > 1 ) {
    S4P::perish(20, "main: Input work order: $INPUTWORKORDER contains more than one FILE_GROUP object") ;
}

# Round the data start time from the input PDR to an even boundary
# so as to be able to do name searches with it.

my $data_start_curr_gran = $fg[0]->data_start;
my $data_start_prev_gran = S4P::TimeTools::CCSDSa_DateAdd($data_start_curr_gran, -7200) ;

my $status_curr_gran = 1 ;
my $status_prev_gran = 1 ;

$status_prev_gran = CheckCompleteSpin($pge,$data_start_prev_gran,$spin_interval,$timer_thresholds_prev_gran{$pge}) ;
$status_curr_gran = CheckCompleteSpin($pge,$data_start_curr_gran,$spin_interval,$timer_thresholds_curr_gran{$pge}) ;

if ( (! $status_prev_gran) and (! $status_curr_gran) ) {
    S4P::logger("INFO", "********** s4pm_airs_L0_check.pl finished with success for algorithm $pge **********") ;
    exit 0 ;
} else {
    unless ($check_only) { `/bin/cp -f $INPUTWORKORDER $OUTPUTWORKORDER` ; }
}

if ($status_prev_gran) { S4P::logger("ERROR", "s4pm_airs_L0_check.pl failed to find complete previous granule input data for algorithm $pge for time = $data_start_prev_gran") ; }

if ($status_curr_gran) { S4P::logger("ERROR", "s4pm_airs_L0_check.pl failed to find complete current granule input data for algorithm $pge for time = $data_start_curr_gran") ; }

if ($status_prev_gran and $status_curr_gran) { S4P::perish(60,"********** s4pm_airs_L0_check.pl failed to find complete set of L0 input data for algorithm $pge **********") ; }

if ($status_prev_gran) { S4P::perish(61,"********** s4pm_airs_L0_check.pl failed to find complete set of L0 input data for algorithm $pge (for previous granule) **********") ; }

if ($status_curr_gran) { S4P::perish(62,"********** s4pm_airs_L0_check.pl failed to find complete set of L0 input data for algorithm $pge (for current granule) **********") ; }

exit(1) ;

################################################################################
# Check AIRS L0 Completeness; if incomplete, spin;
#     if complete return 0; if timer expires return 1
################################################################################

sub CheckCompleteSpin($$$$)
{
    my $pge = shift ;
    my $data_start = shift ;
    my $spin_interval = shift ;
    my $threshold = shift ;

    my $status = 1 ;
    my $clock = 0 ;

    if ($check_only) {
        $status = airs_L0_check($pge,$data_start) ;
    }
    else {
        while ( ($clock < $threshold) and ($status = airs_L0_check($pge,$data_start)) ) {
            S4P::snore($spin_interval,"Still awaiting complete AIRS dataset for algorithm $pge for processing start time $data_start") ;
            $clock += $spin_interval ;
        }
    }

    return($status) ;

}

################################################################################
# airs_L0_check - a routine that accepts a pge name and start datetime
# and then assess whether the L0 inputs to that pge are a complete set.
################################################################################

sub airs_L0_check($$)
{
    my $PGE = shift ;
    my $StartTime = shift ;

    my $status = 0 ;
    my %granule_packet_count = () ;
    my %net_packet_count = () ;

    my $airs_L0_check_cfg = "../s4pm_airs_L0_check.cfg" ;
    my $allocdisk_cfg = "../s4pm_allocate_disk.cfg" ;
    my $specify_data_cfg = "../s4pm_select_data_" . $PGE . ".cfg" ;

    foreach my $input (@{ $algorithm->input_groups })
    {
        my $data_type = $input->data_type ;
        next unless ( ($data_type =~ /^AIR/) and ($input->currency eq 'CURR') ) ;

# ... see if a granule matching the input StartTime has been staged
# ... if so, get the size in bytes of the associated packet file
# ... and determine the number of packets in it

        my $path = $datatype_pool{$datatype_pool_map{$data_type}} ;
#       my $granuleId = S4P::TimeTools::getFilenamePattern(7200,$StartTime,7200,0,"CURR","START_OF_DAY","$data_type.A%YYYY%jjj.%HH%MM.") ;
        my $granuleId = S4P::TimeTools::get_filename_pattern($StartTime, "$data_type.A%YYYY%jjj.%HH%MM.") ;
        my $pattern = $path . "/" . $granuleId . "*.ur" ;
        S4P::logger("DEBUG","Looking for $data_type granule for time $StartTime") ;
        S4P::logger("DEBUG","Looking for pattern $granuleId in path $path") ;

        my @urfiles = glob($pattern) ;

        if (scalar(@urfiles) == 0)
        {
            S4P::logger("INFO","No ur file found for type $data_type and datetime $StartTime.") ;
        }
        else
        {
            foreach my $urfile (@urfiles)
            {
                if (-e $urfile)
                {
                    my $pds_dir = $urfile ;
                    $pds_dir =~ s/\.ur$// ;
                    my $packetfile = "" ;
                    foreach (glob("$pds_dir/*1.PDS"))
                    {
                        $packetfile = $_ ;
                        my $size = (-s $packetfile) ;
                        S4P::logger("DEBUG","Found: $size byte $packetfile of $data_type") ;
                        $granule_packet_count{$data_type} = $size/$packet_sizes{$data_type} ;
                        S4P::logger("INFO","Packet count of granule $granuleId = $granule_packet_count{$data_type}") ;
                    }
                    if ($packetfile eq "") { S4P::logger("WARN","ur file exists but no packet file.") ; }
                }
            }
        }
    }

# Determine net packet count for any applicable APID combinations

    foreach (split " ",$pge_combination_map{$PGE}) {
        $net_packet_count{$_} = 0 ;
        foreach my $esdt (split /\./,$_) {
            $net_packet_count{$_} += $granule_packet_count{$esdt} ;
        }
        S4P::logger("DEBUG","Net packet count for combination $_ = $net_packet_count{$_}") ;
        S4P::logger("DEBUG","Req packet count for combination $_ = $combination_counts{$_}") ;
        if ($net_packet_count{$_} != $combination_counts{$_}) {
            $status = 1 ;
            S4P::logger("INFO","Insufficient packets in combination $_") ;
            S4P::logger("INFO","Need $combination_counts{$_} packets, have $net_packet_count{$_}") ;
        } else {
            S4P::logger("INFO","Sufficient packets for combination $_");
        }
    }
        
# Return 0 if all combinations are present in correct amounts, 1 otherwise

    return($status) ;

}
