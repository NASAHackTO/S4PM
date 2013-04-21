#!/usr/bin/perl

=head1 NAME

s4pm_gdas_0zf_check.pl - QC checking for GDAS_0ZF

=head1 SYNOPSIS

s4pm_gdas_0zf_check.pl.pl
B<-M> I<max_size>
B<-m> I<min_size>
file

=head1 DESCRIPTION

B<s4pm_gdas_0zf_check.pl.pl> performs QC checking of the GDAS_0ZF product. The 
B<-M> specifies the maximum allowable file size in MB and B<-m> specifies the
minimum file size in MB where MB is computed by dividing the number of bytes
by exactly 1000000.

=head1 AUTHOR

Stephen W Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_gdas_0zf_check.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use vars qw($opt_M $opt_m);
use Getopt::Std;

my $min_size = undef;
my $max_size = undef;

getopts('M:m:');

unless ( $opt_M ) {
    S4P::perish(30, "No maximum file size was specified with the -M argument.");
}
unless ( $opt_m ) {
    S4P::perish(30, "No minimum file size was specified with the -m argument.");
}

# The remaining argument on the stack is the file itself

my $gdas = $ARGV[0];
my $fsize = -s $gdas;
$fsize /= 1000000;

# Check for existence of GDAS_0ZF file

unless ( -e $gdas ) {
    S4P::perish(10, "GDAS_0ZF file: $gdas doesn't seem to exist!:$!");
}

if ( $fsize < $opt_m or $fsize > $opt_M ) {
    S4P::logger("FATAL", "QC checking FAILED! File size of $gdas is $fsize. The allowable range is between $opt_m and $opt_M. ACTION: File corruption is suspected and needs to be investigated.");
    exit 1;
} else {
    S4P::logger("INFO", "QC checking PASSED. File size of $gdas is $fsize. This is within the allowable range of between $opt_m and $opt_M.");
    exit 0;
}

