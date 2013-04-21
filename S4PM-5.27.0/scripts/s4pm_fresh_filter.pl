#!/usr/bin/perl

=head1 NAME

s4pm_fresh_filter.pl - Register Data QA filter to block stale data

=head1 SYNOPSIS

s4pm_fresh_filter.pl 
[B<-h> I<hours_old>] 
[B<-d>] 
work_order

=head1 DESCRIPTION

B<s4pm_fresh_filter.pl> checks to see if an incoming file is too old to process.
It obtains the age from the .met file, comparing it to the -h argument (hours 
old, default = 24) to see if it is worth processing.  If the data are too old, 
it exits 10.  If the -d option is specified, it will also delete the data, so 
that only a remove_job need be executed.

=head1 AUTHOR

Christopher S. Lynnes, NASA/GSFC.

=cut

################################################################################
# s4pm_fresh_filter.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use Time::Local;
use vars qw($opt_d $opt_h);

# Parse command line
getopts('dh:');
my $hours = $opt_h || 24;

# Get current time
my $now = time();

# Get start time from l0 file
my @files = sort @ARGV;
my $construction_record = $files[0];
my @times = `l0.pl -t $construction_record`;
my $start = shift @times;
chomp($start);
die "Cannot get time from $construction_record\n" unless $start;

# Compare age with hours-old argument
my ($yyyy,$mm,$day,$hr,$min, $sec) = split(/[\-TZ:]/, $start);
my $gmt = timegm($sec, $min, $hr, $day, $mm - 1, $yyyy-1900);
my $age = ($now - $gmt)/3600.;
if ($age > $hours) {
    printf STDERR "Age of file = %.1f hours, > %.1f hours (too old).\n", 
        $age, $hours;
    # Delete files if -d option specified
    if ($opt_d) {
        # Try to find the .met file for Direct Broadcast
        my $met = $files[-1];
        $met =~ s/\d$/.met/;
        push (@files, $met) if (-f $met);
        foreach my $file(@files) {
            printf STDERR "Deleting stale file $file\n";
            unlink($file) or printf STDERR "Failed to unlink $file: $!\n";
        }
    }
    exit(10);
}
else {
    exit(0);
}
