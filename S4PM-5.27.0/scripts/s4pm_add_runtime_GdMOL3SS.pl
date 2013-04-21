#!/usr/bin/perl

=head1 NAME

s4pm_add_runtime_GdMOL3SS.pl - special handling of specialized criteria for GdMOL3SS

=head1 SYNOPSIS

s4pm_add_runtime_GdMOL3SS.pl
I<pcf_file>
I<PSPEC_file>

=head1 DESCRIPTION

This is a special script to translate the specialized criteria for the 
GdMOL3SS service into the PCF runtime parameters expected by the PGE for
the service. In this case, the single specialized criteria CHANNELS contains
a colon separated list of minimum and maximum longitudes and latitudes
for the subset produced. This single line is split up into four PCF
entries by this script. The PCF LUNs involved are 20100 for minimum 
longitude, 20101 for the maximum longitude, 20102 for the minimum latitude,
and 20103 for the maximum latitude.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_add_runtime_GdMOL3SS.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P::PCF;
use S4P::PCFEntry;
use S4P;

my $pcf_file = $ARGV[0];
my $pspec    = $ARGV[1];

unless ( -e $pspec ) {
    S4P::perish(30, "main: PSPEC file: [$pspec] doesn't seem to exist!");
}

# Read in PCF template

my $pcf = S4P::PCF::read_pcf($pcf_file);
if ( ! $pcf ) {
    S4P::perish(100, "main: Cannot read PCF template: $pcf_file");
}
$pcf->read_file;
$pcf->parse;

my $text = $pcf->text;

$pcf->text($text);
$pcf->parse;

my $OdlTree = S4P::OdlTree->new(FILE => $pspec);

my %criteria_hash = S4P::criteria_hash($OdlTree);
foreach my $name_and_esdt ( keys %criteria_hash ) {
    if ( $name_and_esdt =~ /^CHANNELS/ ) {
        my @coords = split(/:/, $criteria_hash{$name_and_esdt});
        foreach my $coord ( @coords ) {
            if ( $coord =~ /^MIN_LON=(.*)$/ ) {
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '20100', 'value' => $1);
            } elsif ( $coord =~ /^MAX_LON=(.*)$/ ) {
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '20101', 'value' => $1);
            } elsif ( $coord =~ /^MIN_LAT=(.*)$/ ) {
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '20102', 'value' => $1);
            } elsif ( $coord =~ /^MAX_LAT=(.*)$/ ) {
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '20103', 'value' => $1);
            }
        }
    }
}

$pcf->text($text);
$pcf->parse;

S4P::logger("INFO", "main: Writing output PCF file: $pcf_file");
S4P::write_file($pcf_file, $pcf->text);

