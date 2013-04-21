#!/usr/bin/perl

=head1 NAME

s4pm_algorithm_stats.pl - compute rusage stats from rolled up usage logs

=head1 SYNOPSIS

s4pm_algorithm_stats.pl
B<-a> i<alloc_disk_cfg>
B<-d> I<rusage_log_dir>
B<-o> I<output_dir>
B<-f> I<config_file>

=head1 DESCRIPTION

Algorithm stats computes and prints out statistics from rolled-up rusage files. 
It computes hourly numbers, a daily number and hourly averages for the last 
24 hours.

=head1 FILES

=over 4

=item STATS_PGES.txt

Number of algorithm runs.

=item STATS_GRANULES.txt

Number of granules generated from algorithm runs.  This uses "index" data
types, one per algorithm, rather than all algorithms, which still gives a good 
idea of how much time was processed.

This also includes "X", which is computed assuming all of the files are 
5-minute files.

=item STATS_VOLUME.txt

Volume generated from algorithm runs.  This uses all significant data types, 
from a volume standpoint.  Tiny data types (e.g. Browse, MODVOLC) are excluded.

=back

=head1 FILES

Configuration files are perl segments with one or more of the following:

=over 4

=item @pges

Algorithm names for which to report DPR-like statistics.

=item @esdts

Data type names for which to report numbers of files and volumes.

=item @index_esdts

ESDTs to key off for computing X; this should be a file type that is generated 
for every run of the algorithm.

=item $granules_per_hour

Number of files generated per hour (also used for X rate).

=back

=head1 SEE ALSO

L<rollup_rusage>

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_algorithm_stats.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P::TimeTools;
use S4PM;
use vars qw($opt_d
            $opt_f
            $opt_o
            $opt_a
);
use Getopt::Std;
use File::Basename;
require 5.6.0;
use S4P;
use Safe;

getopts('d:f:o:a:');
my $dir = $opt_d || 'rusage_logs';
my $out = $opt_o || '.';
my $alloc_cfg = $opt_a;

my $compartment1 = new Safe 'ALLOC';
$compartment1->share('%proxy_esdt_map');
$compartment1->rdo($alloc_cfg) or
    S4P::perish(30, "main: Failed to read in configuration file $alloc_cfg in safe mode: $!");

my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = 
    localtime(time);
$yday++;
$year += 1900;
$mon++;

# Start time for stats is 24 hours previous, on the hour
my ($yesteryear, $mon_0, $day_0) = S4P::TimeTools::add_delta_days($year, $mon, $mday, -1);
my $yesterday = S4P::TimeTools::day_of_year($yesteryear, $mon_0, $day_0);
my @files;
my @hours;
foreach ($hour..23) {
    push(@hours, "%04d-%03d %02d00", $yesteryear, $yesterday, $hour);
    my $filename = sprintf("$dir/%04d%03d/%04d%03d_%02d.rul", 
        $yesteryear, $yesterday, $yesteryear, $yesterday, $_);
    S4P::logger('INFO', "main: Adding $filename to list of rollup log files");
    push (@files, $filename);
}
if ($hour) {
    foreach (0..($hour-1)) {
        push(@hours, "%04d-%03d %02d00", $year, $yday, $hour);
        my $filename = sprintf("$dir/%04d%03d/%04d%03d_%02d.rul", 
            $year, $yday, $year, $yday, $_);
        S4P::logger('INFO', "main: Adding $filename to list of rollup log files");
        push (@files, $filename);
    }
}

# Delimiter between records is a line of 72 = signs

$/ = ('=' x 72) . "\n";
my $file;
my %total;
my $summary_fmt = "%15s: %4d %4d %6.1f\n";
my @summary;

my @pges = ('MoPGE01', 'MoPGE02', 'MoPGE03', 'GdPGE02B', 'MoPGE71');
# Use "index" ESDTs, one per PGE, for granule-based calculations
my @index_esdts = ('MOD01', 'MOD021KM', 'MOD35_L2');
my @esdts = ('MOD01', 'MOD03', 'MOD021KM', 'MOD02HKM', 'MOD02QKM', 'MOD02OBC',
    'MOD35_L2', 'MOD07_L2');
my $granules_per_hour = 12.;

# Merge variables from the configuration file

if ($opt_f) {
    my $compartment2 = new Safe 'CFG';
    $compartment2->share('@pges','@index_esdts','@esdts','$granules_per_hour');
    $compartment2->rdo($opt_f) or
        S4P::perish(30, "main: Failed to read in configuration file $opt_f in safe mode: $!");
    @pges = @CFG::pges if @CFG::pges;
    @index_esdts = @CFG::index_esdts if @CFG::index_esdts;
    @esdts = @CFG::esdts if @CFG::esdts;
    $granules_per_hour = $CFG::granules_per_hour if $CFG::granules_per_hour;
}

my (@pge_metrics, @esdt_metrics, @volume_metrics, @X_metrics);

# Setup header records

push @pge_metrics, join(' ', (' ' x 10), map{sprintf "%6s", $_} @pges, 'All', "\n");
push @esdt_metrics, join(' ', (' ' x 10), map{sprintf "%8s", $_} @index_esdts, 'All', "\n");
push @volume_metrics, join(' ', (' ' x 10), map{sprintf "%8s", $_} @esdts, 'All', "\n");
push @X_metrics, join(' ', (' ' x 10), map{sprintf "%8s", $_} @index_esdts, 'All', "\n");

# Loop through hourly files

foreach $file(@files) {
    my %hourly = ('GRANULES' => 0, 'VOLUME' => 0, 'PGES' => 0);
    # Read the log, accumulating numbers as we go
    if (-f $file) {
        open LOG, $file or S4P::perish(100, "main: Cannot open logfile $file");
        foreach (<LOG>) {
            my %record = parse_record($_);
            accumulate(\%hourly, \%record);
        }
    }
    my ($hour) = fileparse($file, '\.rul');
    push @summary, sprintf $summary_fmt, $hour, $hourly{'PGES'}, 
        $hourly{'GRANULES'}, $hourly{'VOLUME'} / (1024*1024*1024);
    push (@pge_metrics, sprintf("%10s %s\n", $hour, pge_metrics(\%hourly, \@pges)));
    push (@esdt_metrics, sprintf("%10s %s\n", $hour, esdt_metrics(\%hourly, \@index_esdts)));
    push (@volume_metrics, sprintf("%10s %s\n", $hour, volume_metrics(\%hourly, \@esdts)));
    push (@X_metrics, sprintf("%10s %s\n", $hour, X(\%hourly, 1, \@index_esdts)));
    # Compute totals for each attribute
    my $attr;
    foreach $attr(keys %hourly) {
        $total{$attr} += $hourly{$attr};
    }
}

# Compute daily projections and hourly averages

my ($attr, %average, %daily);
foreach $attr(keys %total) {
    $daily{$attr} = $total{$attr};
    $average{$attr} = $total{$attr} / 24;
}

# Summary is currently unused

push @summary, sprintf $summary_fmt, 'DAILY', $daily{'PGES'}, 
    $daily{'GRANULES'}, $daily{'VOLUME'} / (1024*1024*1024);
push @summary, sprintf $summary_fmt, 'HOURLY AVERAGE', $average{'PGES'}, 
    $average{'GRANULES'}, $average{'VOLUME'} / (1024*1024*1024);

# Add average/daily to metrics

push (@pge_metrics, ('-' x 72) . "\n");
push(@pge_metrics, sprintf("%10s %s\n", "HOURLY AVG", 
    pge_metrics(\%average, \@pges)));
push(@pge_metrics, sprintf("%10s %s\n", "DAILY", pge_metrics(\%daily, \@pges)));

# Add average/daily to metrics

push (@esdt_metrics, ('-' x 72) . "\n");
push(@esdt_metrics, sprintf("%10s %s\n", "HOURLY AVG", 
    esdt_metrics(\%average, \@index_esdts)));
push(@esdt_metrics, 
    sprintf("%10s %s\n", "DAILY", esdt_metrics(\%daily, \@index_esdts)));

# X computed for daily projections (hence the 24 hours)

push(@esdt_metrics, sprintf("%10s %s\n", "X", X(\%daily, 24, \@index_esdts)));
push(@X_metrics, ('-' x 72) . "\n");
push(@X_metrics, sprintf("%10s %s\n", "AVERAGE", X(\%daily, 24, \@index_esdts)));

# Add average/daily to metrics

push (@volume_metrics, ('-' x 72) . "\n");
push(@volume_metrics, sprintf("%10s %s\n", "HOURLY AVG", 
    volume_metrics(\%average, \@esdts)));
push(@volume_metrics, sprintf("%10s %s\n", "DAILY", volume_metrics(\%daily, \@esdts)));

# Write out files

S4P::write_file("$out/STATS_PGES.txt", join("", @pge_metrics, "\n"));
S4P::write_file("$out/STATS_GRANULES.txt", join("", @esdt_metrics, "\n"));
S4P::write_file("$out/STATS_VOLUME.txt", join("", @volume_metrics, "\n"));
S4P::write_file("$out/STATS_X.txt", join("", @X_metrics, "\n"));
exit(0);

sub accumulate {
    my ($r_metrics, $r_record) = @_;

    # Extract PGE, minus script extension
    my ($pge) = split('\.', $r_record->{'COMMAND'});

    # Count number of PGE runs by PGE
    $r_metrics->{"$pge.PGES"}++;
    $r_metrics->{'PGES'}++;

    # Computer time
    $r_metrics->{"$pge.SYSTEM_TIME"} += $r_record->{'SYSTEM_TIME'};
    $r_metrics->{"$pge.USER_TIME"} += $r_record->{'USER_TIME'};
    $r_metrics->{"$pge.SWAPS"} += $r_record->{'SWAPS'};

    # Count number of granules and volume by ESDT
    # Keys are made by concatenating ESDT with .GRANULES and .VOLUME
    # Totals are GRANULES and VOLUME
    my %granules = %{$r_record->{'GRANULES'}};
    my %volume = %{$r_record->{'VOLUME'}};
    my $esdt;
    foreach $esdt(keys %granules) {
        my $grankey = "$esdt.GRANULES";
        my $volkey = "$esdt.VOLUME";
        $r_metrics->{$grankey} += $granules{$esdt};
        $r_metrics->{$volkey} += $volume{$esdt};
        $r_metrics->{'GRANULES'} += $granules{$esdt};
        $r_metrics->{'VOLUME'} += $volume{$esdt};
    }
}
sub parse_record {
    my $text = shift;
    my @attrs = ($text =~ 
       m/
         (COMMAND=\S+).*?
         (USER_TIME=\S+).*?
         (SYSTEM_TIME=\S+).*?
         (MAXIMUM_RESIDENT_SET_SIZE=\S+).*?
         (SWAPS=\S+)
        /gsx);
    # Put attributes in a hash
    my %record;
    foreach (@attrs) {
        my ($par, $val) = split('=');
        $record{$par} = $val;
    }

    # Get volume and granules by data type
    my (%volume, %granules);
    while ($text =~ m/\nFILE=(\S+)/g) {
        my ($path, $volume) = split('=', $1);
        my $fname = basename($path);
        my ($esdt) = split('\.', $fname);
        $esdt = S4PM::get_datatype_if_proxy($esdt, \%ALLOC::proxy_esdt_map);
        $volume{$esdt} += $volume;
        $granules{$esdt}++;
    }
    $record{'GRANULES'} = \%granules;
    $record{'VOLUME'} = \%volume;
    return %record;
}
sub pge_metrics {
    my ($rh_metrics, $ra_pges) = @_;
    my @string;
    my $pge;
    foreach $pge(@{$ra_pges}) {
        push(@string, sprintf("%6d", $rh_metrics->{"$pge.PGES"}));
    }
    push(@string, sprintf("%6d", $rh_metrics->{"PGES"}));
    return join(' ', @string);
}
sub esdt_metrics {
    my ($rh_metrics, $ra_esdts) = @_;
    my @string;
    my $esdt;
    foreach $esdt(@{$ra_esdts}) {
        push(@string, sprintf("%8d", $rh_metrics->{"$esdt.GRANULES"}));
    }
    push(@string, sprintf("%8d", $rh_metrics->{"GRANULES"}));
    return join(' ', @string);
}
sub volume_metrics {
    my ($rh_metrics, $ra_esdts) = @_;
    my @string;
    my $esdt;
    my $gb = (1024 * 1024 * 1024);
    foreach $esdt(@{$ra_esdts}) {
        push(@string, sprintf("%8.1f", $rh_metrics->{"$esdt.VOLUME"}/$gb));
    }
    push(@string, sprintf("%8.1f", $rh_metrics->{"VOLUME"}/$gb));
    return join(' ', @string);
}
sub X {
    my ($rh_metrics, $hours, $ra_esdts) = @_;
    my ($esdt, $granules, @string);
    foreach $esdt(@{$ra_esdts}) {
        push(@string, sprintf("%8.2f", $rh_metrics->{"$esdt.GRANULES"} / ($hours * $granules_per_hour)));
        $granules += $rh_metrics->{"$esdt.GRANULES"};
    }
    push(@string, 
        sprintf("%8.2f", $granules / ($hours * $granules_per_hour * scalar(@{$ra_esdts}) )));
    return join(' ', @string);
}
