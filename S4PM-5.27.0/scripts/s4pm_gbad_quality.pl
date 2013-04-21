#!/usr/bin/perl

=head1 NAME

s4pm_gbad_quality.pl - check quality (gaplessness) of GBAD carryout files

=head1 SYNOPSIS

s4pm_gbad_quality.pl
[B<-a> I<apid>]
[B<-c> I<cum_gap>]
[B<-g> I<max_gap>]
[B<-h>]
[B<-n> I<n_gaps>]
[B<-v>]

=head1 DESCRIPTION

B<s4pm_gbad_quality.pl> loops through a GBAD carryout file to see what the 
quality is, which is measured in terms of gaps.  If any of the gap measurements 
exceeds a threshold, the program exits non-zero; if no thresholds are exceeded, 
it exits 0.  Some allowance is made for "mini-gaps" at the start and end of the 
records, which can be 1.5 times the nominal interval.  

Only the values for an "index" APID are examined.  The nominal interval is 
computed by looping through once and taking the minimum interval between points.

Quality is measured by the following:

=over 4

=item n_gaps

Number of intervals exceeded 1.51 times the nominal interval.

=item cum_gap

Sum of intervals that are more than 1.5 times the size of the nominal interval.

=item max_gap

Maximum interval.

=back

=head1 ARGUMENTS

=over 4

=item -a apid

Index apid to use for computing intervals.  Default = 5885.

=item -c cum_gap

Cumulative gap threshold.  Default = 3.01 seconds.

=item -g max_gap

Maximum gap threshold.  Default = 1.51 seconds.

=item -n n_gaps

Number of gaps threshold.  Default = 0.

=item -v

Verbose mode, prints to standard error.

=back

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_gbad_quality.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use vars qw($opt_a $opt_c $opt_g $opt_h $opt_n $opt_v);

# Parse command line
getopts('a:c:g:hn:v');

usage() if $opt_h;

my %threshold;
# 60 seconds is the most the toolkit can extrapolate over
# However, a missing status word (on 8 second increments) will lead to 
# 16 seconds between points; take out a couple seconds either way, and
# you get the answer to life, the universe and everything: 42
$threshold{'max_gap'} = $opt_g || 42.;
$threshold{'cum_gap'} = $opt_c || 42.;
$threshold{'n_gaps'} = $opt_n || 0;
my $verbose = $opt_v;
my $index_apid = $opt_a || 5885;

# Read file
my $file = $ARGV[0] || usage();
open FILE, $file or die "Cannot open file $file: $!";
# Line 1: YYYY/DDD HH:MM:SS.SSS
my $s = <FILE>;
my ($year, $doy, $hours, $minutes, $seconds) = split(/[\/: ]/, $s);
# Assume GBAD starts on the even hour
my $start_gap = ($hours % 2) * 3600 + $minutes * 60 + $seconds;
# Line 2: number of apids
my $n_apid = <FILE>;
# Line 3 to 3+n_apid: info on APIDs not needed for this program)
foreach (1..$n_apid) {
    my $a = <FILE>;
}
my @time;
my $last_time = 0.;
my $min_interval = 7200.;
my $max_gap = 0.;
my $t_max_gap = $last_time;
my ($t, $dt);
# Loop through file getting times for index APID (i.e., the one we want
# to use for determining interval)
# On first loop, find minimum and maximum intervals
# Maximum interval is our max gap
# Minimum interval is what we use to compute our number of gaps and
# cumulative gap, i.e., we assume that it is the nominal interval
while (<FILE>) {
    my ($dt, $apid, $stuff) = split('\|', $_);
    if ($apid == $index_apid){
        # Add time to start time
        $t = $start_gap + $dt;
        # Store in an array for later use
        push @time, $t;
        my $interval = $t - $last_time;
        $last_time = $t;
        $min_interval = $interval if ($interval && $interval < $min_interval);
        # If larger than hitherto seen, change max_gap
        if ($interval > $max_gap) {
            $max_gap = $interval;
            $t_max_gap = $t;
        }
    }
}
# Compute gap at the end, assuming a 7200 second granule
my $end_gap = 7200. - $t;
if ($end_gap > $max_gap) {
    $max_gap = $end_gap;
    $t_max_gap = $t;
}
my $cum_gap = $start_gap - $min_interval;
my $n_gaps = 0;
my $npts = scalar(@time);
my $i;
# Add a little to the minimum interval so rounding errors don't trip us up
my $gap_trip = $min_interval * 1.1;
for ($i = 1; $i < $npts; $i++) {
    $dt = $time[$i] - $time[$i-1];
    $cum_gap += $dt - $min_interval;
    $n_gaps++ if ($dt > $gap_trip);
}
$n_gaps++ if ($start_gap > $gap_trip);
$n_gaps++ if ($end_gap > $gap_trip);
$cum_gap += $end_gap;

print STDERR "Min interval = $min_interval\n" if ($opt_v);
print STDERR "Max gap = $max_gap at $t_max_gap after the even hour\n" if ($opt_v);
print STDERR "Number of gaps = $n_gaps\n";
print STDERR "Cumulative gap = $cum_gap\n";

# Check values against thresholds and exit
my $n_err = 0;
if ($n_gaps > $threshold{'n_gaps'}) {
    print STDERR "Number of gaps $n_gaps exceeds threshold $threshold{'n_gaps'}\n";
    $n_err++;
}
if ($cum_gap > $threshold{'cum_gap'}) {
    print STDERR "Cumulative gap of $cum_gap exceeds threshold $threshold{'cum_gap'}\n";
    $n_err++;
}
if ($max_gap > $threshold{'max_gap'}) {
    print STDERR "Max gap of $max_gap exceeds threshold $threshold{'max_gap'}\n";
    $n_err++;
}
print STDERR "No significant gaps in GBAD\n" if ($verbose && $n_err < 1);
exit($n_err);

sub usage {
    print STDERR "$0 [-a apid] [-c cum_gaps] [-g max_gap] [-n ngaps] [-v] filename\n";
    exit(1);
}
