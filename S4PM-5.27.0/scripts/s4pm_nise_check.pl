#!/usr/bin/perl

=head1 NAME

s4pm_nise_check.pl - QC checking for NISE

=head1 SYNOPSIS

s4pm_nise_check.pl.pl
file

=head1 DESCRIPTION

B<s4pm_nise_check.pl.pl> checks for a particular known form of corruption of 
the NISE data file (provided by NSIDC DAAC). The method of detection is 
simply to run 'ncdump -h' on the file. If the output is empty, then the 
command failed and the file is assumed corrupt.

This form of corruption is now known to happen when the NISE file is
retreived via ftp in ASCII mode, as opposed to binary mode.

=head1 AUTHOR

Stephen W Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_nise_check.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;

my $nise = $ARGV[0];

# Check for existence of NISE file

unless ( -e $nise ) {
    S4P::perish(10, "NISE file: $nise doesn't seem to exist!:$!");
}

my $slurp = "";

# If the ncdump command cannot be found for some reason, the exit code
# will still be non-zero

$slurp = `ncdump -h $nise`;

if ( $slurp eq "" ) {
    S4P::perish(10, "nise_check: ncdump check using 'ncdump -h' produced no output.");
    exit 1;
} else {
    exit 0;
}

