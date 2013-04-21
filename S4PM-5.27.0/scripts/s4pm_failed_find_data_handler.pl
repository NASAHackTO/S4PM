#!/usr/bin/perl

=head1 NAME

s4pm_failed_find_data_handler.pl - failure handler for the Find Data station

=head1 SYNOPSIS

s4pm_failed_find_data_handler.pl

=head1 DESCRIPTION

This script is a failure handler script for the Find Data station in 
on-demand processing onlh. This script merely renames the FIND log
file to a ORDER_FAILURE work order and places it in the Track Requests
station.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_failed_find_data_handler.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
require 5.6.0;

my @debris = glob("*.log");

unless ( $debris[0] ) {
    S4P::perish(100, "Failed to find FIND log file in failed job directory.");
}

my $jobid;
if ( $debris[0] =~ /^FIND_[^\.]+\.([A-Za-z0-9_]+)\.log$/ ) {
    $jobid=$1;
} else {
    S4P::perish(40, "Failed to parse log file name for job id: " . $debris[0]);
}

rename($debris[0], "../../track_requests/DO.ORDER_FAILURE.$jobid.wo");

exit(0);

