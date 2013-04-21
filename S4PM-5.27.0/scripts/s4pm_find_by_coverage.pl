#!/usr/bin/perl

=head1 NAME

s4pm_find_by_coverage.pl - find data based on coverage of processing time

=head1 SYNOPSIS

s4pm_find_by_coverage.pl 
[B<-f[ull]>]
I<config_parms>

=head1 DESCRIPTION

This script is meant to work with the s4pm_find_data.pl script of the Find
Data station via the -find option. This option allows a custom script to 
locate desired data rather than having the s4pm_find_data.pl script do it.

This find script examines all metadata files in a data directory and examines
the beginning and ending date/time to see which completely (if -full is
specified) or partially covers the processing period. If one is found, it 
immediately exits with that path name. If none are found, it exits with no 
return value which Find Data will interpret as a null result.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

# s4pm_find_by_coverage.pl,v 1.6 2006/12/27 19:29:09 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Safe;
use strict;
use S4P::MetFile;
use S4P::TimeTools;
use Getopt::Long;

use vars qw($FULL);

$FULL = undef;

GetOptions("full" => \$FULL);

my $config = $ARGV[0];

my $compartment = new Safe 'CFG';
$compartment->share('$data_dir', '$data_start', '$data_stop', '$currency');
$compartment->rdo($config) or return "Failed to read $config in safe mode.";

my @urfiles = glob("$CFG::data_dir/*.ur");
foreach my $urf ( @urfiles ) {
    my $met = $urf;
    $met =~ s/ur$/met/;
    my ($begin, $end) = S4P::MetFile::get_spatial_temporal_metadata($met);
    my $res1;
    my $res2;
    if ( $FULL ) {
        $res1 = S4P::TimeTools::CCSDSa_DateCompare($begin, $CFG::data_start);
        $res2 = S4P::TimeTools::CCSDSa_DateCompare($end, $CFG::data_stop);
    } else {
        my $time_length = S4P::TimeTools::CCSDSa_Diff($CFG::data_start, $CFG::data_stop);
        my $curr = $CFG::currency; 
        my $t_point;
        if ($curr =~ /CURR/) {
            $t_point = $CFG::data_stop;
        } elsif ($curr =~ /PREV(\d)/) {
            $t_point = S4P::TimeTools::CCSDSa_DateAdd($CFG::data_stop, -($1 * $time_length));
        }
        $res1 = S4P::TimeTools::CCSDSa_DateCompare($begin, $t_point);
        $res2 = S4P::TimeTools::CCSDSa_DateCompare($end, $t_point);
    }
    unless ( $res1 == -1 or $res2 == +1 ) {
        print $urf;
        exit 0;
    }
}

exit 0;
