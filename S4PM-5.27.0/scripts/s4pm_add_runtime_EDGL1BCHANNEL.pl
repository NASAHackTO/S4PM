#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_add_runtime_EDG_L1BCHANNEL.pl - special handling of specialized criteria for EDG_L1BCHANNEL

=head1 SYNOPSIS

s4pm_add_runtime_EDG_L1BCHANNEL.pl
I<pcf_file>
I<PSPEC_file>

=head1 DESCRIPTION

This is a special script to translate the specialized criteria for the 
EDG_L1BCHANNEL service into the PCF runtime parameters expected by the 
algorithm for the service. 

The EDG_L1BCHANNEL service is a clone of the GdMODL1B service, except
that it handles requests coming from EDG via the V0 Gateway. The single 
specialized criteria Parameter contains the channels in the form:
("Band1(620-670nm)", "Band3(459-479nm)", ...). This needs to be converted
into a string like: 1010000.... at LUN 20100 in the PCF. We assume that
the format (in LUN 21212) is always HDFEOS.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_add_runtime_EDGL1BCHANNEL.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
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
    if ( $name_and_esdt =~ /^Parameter/ ) {
                my $bandlist = parse_edg_string($criteria_hash{$name_and_esdt});
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '20100', 'value' => $bandlist);
                $text = S4P::PCFEntry::replace('text' => $text, 'lun' => '21212', 'value' => 'HDFEOS');
    }
}

$pcf->text($text);
$pcf->parse;

S4P::logger("INFO", "main: Writing output PCF file: $pcf_file");
S4P::write_file($pcf_file, $pcf->text);

sub parse_edg_string {

    my $str = shift;

    my $bandnum = "00000000000000000000000000000000000000";

### Strip of parentheses

    $str =~ s/^\s*\(\s*//;
    $str =~ s/\s*\)\s*$//;
    my @bands = split(/,/, $str);

    foreach my $band ( @bands ) {
        $band =~ s/^\s+//;
        $band =~ s/\s+$//;
        if ( $band =~ /^Band([0-9]{1,2})([LH]?)\(/ ) {
            if ( $1 < 13 ) {
                substr($bandnum, ($1-1), 1) = "1"
            } elsif ( $1 > 14 ) {
                substr($bandnum, ($1+1), 1) = "1"
            } else {
                my $ch = $1 . $2;
                if ( $ch eq "13L" ) { substr($bandnum, 12, 1) = "1"; }
                if ( $ch eq "13H" ) { substr($bandnum, 13, 1) = "1"; }
                if ( $ch eq "14L" ) { substr($bandnum, 14, 1) = "1"; }
                if ( $ch eq "14H" ) { substr($bandnum, 15, 1) = "1"; }
            }
        } else {
            S4P::perish(40, "parse_edg_string: Failed to parse band number from EDG parameter in specialized criteria.");
        }
    }

    return $bandnum;
}


