#!/usr/bin/perl

=head1 NAME

s4pm_parse_rusage_from_ph.pl - grab rusage stats from PH tar files

=head1 SYNOPSIS

s4pm_parse_rusage_from_ph.pl
I<directory>
 
=head1 DESCRIPTION
 
B<s4pm_parse_rusage_from_ph.pl> examines all production history (PH) tar files 
in directory I<directory> and pulls out rusage elapsed, user, and system times. 
For each algorithm, the average elapsed, user, and system time is displayed on 
STDOUT along with the number used in computing the average.

B<s4pm_parse_rusage_from_ph.pl> assumes that all PH tar files have the file 
name: PH.<PGEname>.<DateTime>.tar and that each contains rusage statistics.

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2
 
=cut
 
################################################################################
# s4pm_parse_rusage_from_ph.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################
 
use strict;

my %pge_user_sums    = ();
my %pge_sys_sums     = ();
my %pge_elapse_sums  = ();
my %pge_nums = ();
my $file;

$| = 1;		# Forces output buffer to be flushed

unless ( $ARGV[0] ) {
    die "\nYou need to specify a PH directory location as an argument!\n\n";
}
my $dir = $ARGV[0];

opendir(DIR, $dir) or die "Cannot opendir: $dir: $!\n\n";

print "\nWorking. This may take a while. Please, be patient.\n\n";
while ( defined($file = readdir(DIR)) ) {
    next unless ( $file =~ /^PH\.([^.]+).*tar$/ );
    print ".";
    my $pge = $1;
    my $user_time   = `grep USER_TIME $file`;
    my $sys_time    = `grep SYSTEM_TIME $file`;
    my $elapse_time = `grep ELAPSED_TIME $file`;
    chomp($user_time);
    chomp($sys_time);
    chomp($elapse_time);
    if ( $user_time =~ /\s*USER_TIME\s*=\s*([^\s]+).*$/ ) {
        $user_time = $1;
    }
    if ( $sys_time =~ /\s*SYSTEM_TIME\s*=\s*([^\s]+).*$/ ) {
        $sys_time = $1;
    }
    if ( $elapse_time =~ /\s*ELAPSED_TIME\s*=\s*([^\s]+).*$/ ) {
        $elapse_time = $1;
    }
  
    if ( exists $pge_user_sums{$pge} ) {
        $pge_user_sums{$pge} += $user_time;
    } else {
        $pge_user_sums{$pge} = $user_time;
    }
    if ( exists $pge_sys_sums{$pge} ) {
        $pge_sys_sums{$pge} += $sys_time;
    } else {
        $pge_sys_sums{$pge} = $sys_time;
    }
    if ( exists $pge_elapse_sums{$pge} ) {
        $pge_elapse_sums{$pge} += $elapse_time;
    } else {
        $pge_elapse_sums{$pge} = $elapse_time;
    }

    if ( exists $pge_nums{$pge} ) {
        $pge_nums{$pge} = $pge_nums{$pge} + 1;
    } else {
        $pge_nums{$pge} = 1;
    }
}

print "\n\nDone!\n\n";

my ($pgename, $avg_elapse_time, $avg_user_time, $avg_sys_time, $num);

print " Algorithm      Sample Size  Avg Elapsed Time  Avg User Time  Avg Sys Time\n";
print " -------------  -----------  ----------------  -------------  ------------\n";

format PRETTY =
 @<<<<<<<<<<<<<   @########   @#####.###       @#####.###    @#####.###
 $pgename,     $num,        $avg_elapse_time, $avg_user_time, $avg_sys_time
.

$~ = "PRETTY";
foreach $pgename ( keys %pge_sys_sums ) {
    $num = $pge_nums{$pgename};
    $avg_elapse_time  = $pge_elapse_sums{$pgename} / $num;
    $avg_user_time  = $pge_user_sums{$pgename} / $num;
    $avg_sys_time  = $pge_sys_sums{$pgename} / $num;
    write;
}
$~ = "STDOUT";

print "\n";
