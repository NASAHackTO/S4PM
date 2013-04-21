#!/usr/bin/perl

=head1 NAME

s4pm_rollup_rusage.pl - collect individual rusage logs and put into hourly files

=head1 SYNOPSIS

s4pm_rollup_usage.pl
B<-d> I<rusage_logs>
B<-n> I<number_of_dirs>
B<-r> I<rollup_dir>

=head1 DESCRIPTION

Collects up all of the rusage files for an hour and cats them into one file.

=head1 FILES

All output files are of the form YYYYDDD_HH.rul.
Each rusage record is separated by a line of 72 equal signs (=).

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_rollup_rusage.pl,v 1.3 2007/08/17 21:07:15 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_n $opt_d $opt_r);
use Getopt::Std;
use File::Basename;
require 5.6.0;
use S4P;

getopts('d:n:r:');
my $dir = $opt_d || 'rusage_logs';
my $rollup_dest = $opt_r || $dir;

# Get current time
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday) = localtime(time);
$yday++;

# Rollup dirs up to but not including currently active dir
my $max_dir = sprintf("%s/%04d%03d_%02d", $dir, $year + 1900, $yday, $hour);
S4P::logger('INFO', "Rolling up to (but not including) $max_dir");

# Get list of dirs (will sort later)
my @dirs = glob("$dir/[0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9]");
my $done = 0;
my $dir;

# Loop through dirs, rolling up files in each one.
foreach $dir(sort @dirs) {
    last if ($opt_n && $done >= $opt_n);
    last if ($dir ge $max_dir);
    $done++;
    my $dest = "$rollup_dest/" . basename($dir);
    $dest =~ s/...$//;  # Lop off last three characters (hour)
    if (! -d $dest) {
        mkdir($dest) or S4P::perish(1, "Cannot mkdir $dest");
    }
    rollup_dir($dir, $dest);
}
exit(0);

sub rollup_dir {
    my $dir = shift;
    my $dest = shift;
    my $parent = dirname($dir);
    my $rollup = "$dest/" . basename("$dir.rul");
    my $delimiter = '=' x 72;
    my @logfiles = glob("$dir/*.Log");
    my ($file);
    S4P::logger('INFO', "Rolling up rusage logs in $dir into $rollup");
    foreach $file (sort @logfiles) {
        S4P::logger('DEBUG', 'Rolling up rusage file $file');
        my ($rs, $rc) = S4P::exec_system("cat $file >> $rollup");
        S4P::perish(1, $rs) if ($rc);
        my ($rs, $rc) = S4P::exec_system("echo $delimiter >> $rollup");
        unlink $file or S4P::perish(2, "Cannot unlink $file: $!");
    }
    rmdir($dir) or S4P::perish(2, "Cannot rmdir $dir: $!");
    return 1;
}
