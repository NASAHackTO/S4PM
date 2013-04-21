#!/usr/bin/perl

=head1 NAME

s4pm_is_hdf.pl - check to see if a file is HDF

=head1 SYNOPSIS

s4pm_is_hdf.pl filename

=head1 DESCRIPTION

s4pm_is_hdf.pl checks the first four bytes of a file for
NCSA HDF "magic", to wit, ^N^C^S^A. 

=head1 DIAGNOSTICS

 0: is HDf
 1: bad usage
 2: cannot open file
 3: can open file, but is not HDF

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_is_hdf.pl,v 1.3 2008/07/07 21:54:10 clynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;

my $n_args = scalar(@ARGV);
usage() unless ($n_args == 1);
my $file = shift @ARGV;
if (! open F, $file) {
    warn "$0: cannot open $file: $!\n";
    exit(2);
}
binmode F;
my $buf;
my $n = read(F, $buf, 4);
if ($n != 4) {
    warn "$0: read only $n bytes from $file: $!\n";
    exit(3);
}
my $magic = pack('c4', 14, 3, 19, 1);
if ($buf ne $magic) {
    warn "$0: $file is not HDF\n";
    exit(3);
}
exit(0);
    
sub usage { die "Usage: s4pm_is_hdf.pl file\n"; }
