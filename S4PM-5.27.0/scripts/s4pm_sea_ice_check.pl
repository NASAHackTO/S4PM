#!/usr/bin/perl

=head1 NAME

s4pm_sea_ice_check.pl - QC for SEA_ICE

=head1 SYNOPSIS

s4pm_sea_ice_check.pl.pl
file

=head1 DESCRIPTION

B<s4pm_sea_ice_check.pl.pl> performs QC of the SEA_ICE product. Currently,
only two file sizes for this product are known to be acceptable: 226884
bytes and 259284 bytes. Any other file size results in a failure.

=head1 AUTHOR

Stephen W Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_sea_ice_check.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;

# The remaining argument on the stack is the file itself

my $seaice = $ARGV[0];
my $fsize = -s $seaice;

# Check for existence of SEA_ICE file

unless ( -e $seaice ) {
    S4P::perish(10, "SEA_ICE file: $seaice doesn't seem to exist!:$!");
}

if ( $fsize != 226884 and $fsize != 259284 ) {
    S4P::logger("FATAL", "QC checking FAILED! File size of $seaice is $fsize. The allowable file sizes are 226884 and 259284 bytes exactly. ACTION: File corruption is suspected and needs to be investigated.");
    exit 1;
} else {
    S4P::logger("INFO", "QC checking PASSED. File size of $seaice is $fsize. This is an allowable file size.");
    exit 0;
}

