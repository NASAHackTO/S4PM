#!/usr/bin/perl

=head1 NAME

s4pm_attitude_check.pl - check attitude file for signs of a spacecraft maneuver

=head1 SYNOPSIS

s4pm_attitude_check.pl 
[B<-t> I<threshold>] 
[B<-v>]
[B<-V>]
file

=head1 DESCRIPTION

B<s4pm_attitude_check.pl> runs through a DPREP'd attitude file looking for 
signs that a spacecraft maneuver has taken place.  This typically shows up
as anomalously large attitude angles.  If a threshold is tripped on any
one of the three angles (roll/pitch/yaw), it writes to standard error 
with the number of trips and the time range (in TAI93 time) during which
the trips occurred, finally exiting with the number of angles that had 
anomalies.  If the threshold is not tripped, it is silent (unless B<-v> 
or B<-V> is specified) and the exit code is 0.

This is meant to be used within data_catcher to automatically screen out
AM1ATTN0 files with maneuvers.  However, one can also run it from the
command line, and on AM1ATTNF files as well.

=head1 ARGUMENTS

=over 4

=item B<-t> I<threshold>

The threshold for "large" can be set on the command line.  Units are radians.
Default is 0.0002.  The AM1 (Terra) onboard attitude pegs at 2048 arc-seconds, 
or about 0.0097.

=item B<-v>

Verbose mode:  prints attitude file header to standard error.

=item B<-V>

I<Really> verbose mode:  prints attitude angles to standard error.

=back

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_attitude_check.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use vars qw($opt_t $opt_v $opt_V);
my $have_s4p = require S4P::TimeTools;

getopts('vt:V');
my $threshold = $opt_t || 0.0002;

my ($buf, $spacecraft_id, $ascii_time, $source, $version, @tai93);
open FILE, $ARGV[0] or die "Cannot open file $ARGV[0]";

read FILE, $spacecraft_id, 24;
print STDERR "Spacecraft ID: $spacecraft_id\n" if $opt_v;

read FILE, $ascii_time, 48;
print STDERR "ASCII Time Range: $ascii_time\n" if $opt_v;


read FILE, $source, 32;
print STDERR "Source: $source\n" if $opt_v;

read FILE, $version, 8;
print STDERR "Version: $version\n" if $opt_v;

read FILE, $buf, 8;
$tai93[0] = unpack 'd', $buf;
print STDERR "Start (TAI93): $tai93[0]\n" if $opt_v;

read FILE, $buf, 8;
$tai93[1] = unpack 'd', $buf;
print STDERR "Stop (TAI93): $tai93[1]\n" if $opt_v;

read FILE, $buf, 4;
my $interval = unpack 'f', $buf;
print STDERR "Record interval: $interval\n" if $opt_v;

read FILE, $buf, 4;
my $nURs = unpack 'L', $buf;
print STDERR "Number of URs: $nURs\n" if $opt_v;

read FILE, $buf, 4;
my $nrecs = unpack 'L', $buf;
print STDERR "Number of records: $nrecs\n" if $opt_v;

read FILE, $buf, 12;
my @euler_order = unpack 'LLL', $buf;
print STDERR "Euler Order: ", join(' ', @euler_order, "\n") if $opt_v;

read FILE, $buf, (4 * 16);
my @qaParameters = unpack 'f16', $buf;
print STDERR "QA Parameters:\n\t", join("\n\t", @qaParameters), "\n" if $opt_v;

read FILE, $buf, (4 * 4);
my @qaStatistics = unpack 'f4', $buf;
print STDERR "QA Statistics:\n\t", join("\n\t", @qaStatistics), "\n" if $opt_v;

read FILE, $buf, 280;   # Spare

my $i;
for ($i < 0; $i < $nURs; $i++) {
    read FILE, $buf, 256;
}
my @excess = (0,0,0);
my (@start, @stop);
my $first_tai93;

for ($i = 0; $i < $nrecs; $i++) {
    my ($secTAI93, $ra_euler_angle, $ra_angular_velocity, $quality_flag) = read_record();
    $first_tai93 ||= $secTAI93;
    foreach my $angle(0..2) {
        if (abs($ra_euler_angle->[$angle]) > $threshold) {
            $excess[$angle]++;
            $start[$angle] = $secTAI93 unless $start[$angle];
            $stop[$angle] = $secTAI93;
        }
    }
    if ($opt_V) {
        printf "%lf  %11.4le  %11.4le  %11.4le\n", $secTAI93,
            $ra_euler_angle->[0], $ra_euler_angle->[1], $ra_euler_angle->[2]
    }
}
my $err = 0;
my @angle_names = ('roll', 'pitch', 'yaw');
foreach (0..2) {
    if ($excess[$_]) {
        my $angle_name = $angle_names[$euler_order[$_] - 1];
        printf STDERR "%5s exceeded threshold %.6f %5d times from %s to %s\n", 
            $angle_name, $threshold, $excess[$_], $start[$_], $stop[$_];
        $err++;
    }
}
exit($err);

# Read software developed from structure info in R. Kummerrer's DPREP paper:
# struct {
#    PGSt_double   secTAI93;
#    PGSt_double   eulerAngle[3];
#    PGSt_double   angularVelocity[3];
#    PGSt_uinteger qualityFlag;
#    char          spare[4];
# } PGSt_attitRecord;
sub read_record {
    my $buf;
    read FILE, $buf, 8;
    my $secTAI93 = unpack 'd', $buf;

    read FILE, $buf, (8 * 3);
    my @euler_angle = unpack('d3', $buf);

    read FILE, $buf, (8 * 3);
    my @angular_velocity = unpack('d3', $buf);

    read FILE, $buf, 4;
    my $quality_flag = unpack('L', $buf);
    read FILE, $buf, 4;   # spare
    return ($secTAI93, \@euler_angle, \@angular_velocity, $quality_flag);
}
