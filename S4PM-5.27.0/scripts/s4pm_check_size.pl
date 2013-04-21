#!/usr/bin/perl

=head1 NAME

s4pm_check_size - check file size against numbers in config file

=head1 SYNOPSIS

s4pm_check_size.pl B<-f> I<config_file> met_file data_file

=head1 DESCRIPTION

s4pm_check_size checks that a data file matches the expected
size range for its data type and DayNightFlag.  It uses the same
configuration file that Steve Kreisler's DUe uses for the expected sizes.
DayNightFlag is used because Night granules are sometimes smaller than
Day.  If the flag is Both, the minimum of the Day and Night minima is used.
Likewise for the maxima.  If no DayNightFlag can be found in the metadata,
Both is assumed.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFc, Code 610.2

=cut

################################################################################
# s4pm_check_size.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use S4P::MetFile;
use Getopt::Std;
use Safe;

use vars qw($opt_f);
getopts('f:');

# Get config file info
usage() unless $opt_f;
my $compartment = new Safe 'CFG';
$compartment->share('$Products');
$compartment->rdo($opt_f) or S4P::perish(30, "Cannot read config file $opt_f");

my $metfile = shift @ARGV or usage();
my $data_file = shift @ARGV or usage();

# Get Short Name:  sizes depend on it
my %attr = S4P::MetFile::get_from_met($metfile, "SHORTNAME", "DAYNIGHTFLAG") or
   S4P::perish(40, "Cannot get attributes from $metfile");
my $short_name = $attr{'SHORTNAME'} or
   S4P::perish(41, "Cannot get SHORTNAME from $metfile");
my $day_night = $attr{'DAYNIGHTFLAG'} || 'Both';

# Exit success if not found in config file
# Don't want to require every new ESDT to be in config file
if (! exists $CFG::Products->{$short_name}) {
    S4P::logger('WARN', "ESDT $short_name not found in config file $opt_f");
    exit(0);
}
my ($max_size, $min_size);
if ($day_night eq 'Both') {
    my $max_day = $CFG::Products->{$short_name}->{'Day'}->{'maxSize'};
    my $max_night = $CFG::Products->{$short_name}->{'Night'}->{'maxSize'};
    $max_size = ($max_day > $max_night) ? $max_day : $max_night;
    my $min_day = $CFG::Products->{$short_name}->{'Day'}->{'minSize'};
    my $min_night = $CFG::Products->{$short_name}->{'Night'}->{'minSize'};
    $min_size = ($min_day < $min_night) ? $min_day : $min_night;
}
else {
    $max_size = $CFG::Products->{$short_name}->{$day_night}->{'maxSize'};
    $min_size = $CFG::Products->{$short_name}->{$day_night}->{'minSize'};
}

# OK, time to actually check the file size: use ECS MB (10^6 bytes)
my $file_size = (-s $data_file) / (1000000);
S4P::perish(80, sprintf("%s is %.2f MB, less than %.2f minimum for %s", 
    $data_file, $file_size, $min_size, $short_name)) if ($file_size < $min_size);
S4P::perish(81, sprintf("%s is %.2f MB, less than %.2f maximum for %s", 
    $data_file, $file_size, $max_size, $short_name)) if ($file_size > $max_size);
S4P::logger('INFO', "$data_file passed file size check: $min_size < $file_size < $max_size");
exit(0);

sub usage {
    S4P::perish(10, "Usage: $0 -f config_file met_file data_file");
}
