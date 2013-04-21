#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

eval 'exec /tools/gdaac/COTS/perl-5.8.5/bin/perl  -S $0 ${1+"$@"}'
    if 0; # not running under some shell

=head1 NAME

s4pm_old2new_stringcfg.pl - convert old style string configuration file to new

=head1 SYNOPSIS

s4pm_old2new_stringcfg.pl
B<-i> I<input>
B<-c> I<s4pm_pge_esdt_cfg>
[B<-a>]

=head1 DESCRIPTION

This stand-alone script makes an attempt at converting pre-5.6.0 S4PM
Stringmaster string-specific configuration files into new 5.6.0+
string-specific Stringmaker configuration files. The input (old style)
string-specific configuration file is specified with the -i argument and
the Stringmaster s4pm_pge_esdt.cfg file is specified with the -c argument.
For AIRS strings, include the -a argument.

=head1 AUTHOR

Stephen berrick, NASA GSFC, Code 610.2

=cut

################################################################################
# s4pm_old2new_stringcfg.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Safe;
use Getopt::Std;
use vars qw($opt_i $opt_c $opt_a);

getopts('i:c:a');
unless ( $opt_i ) {
    die "\nYou must specify an input with -i\n\n";
}
unless ( $opt_c ) {
    die "\nYou must specify the s4pm_pge_esdt.cfg file with -c\n\n";
}

# Based upon input file name, guess the profile for the algorithms within
my $fn = $opt_i;
my $profile;
if ( $fn =~ /^S4PM[0-9][0-9]_[A-Z][A-Z]_([A-Z][A-Z]).*$/ ) {
    if ( $1 eq "RE" or $1 eq "NU" ) { 
        $profile = "RPROC"; 
    } elsif ( $1 eq "FW" ) { 
        $profile = "FPROC"; 
    } else {
        print "\nI couldn't guess the algorithm profiles. You'll have to do this manually.\n";
        $profile = "UNKNOWN"; 
    }
}
    
# Slurp in new prolog
my $header_file = "/tools/gdaac/TS2/cfg/s4pm_stringmaker_string.cfg";
open(HEAD, $header_file) or die "Failed to open file $header_file for read-only: $!\n\n";
my @header = ();
while ( <HEAD> ) {
    next unless ( /^#/ );
    last if ( /=cut/ );
    push(@header, $_);
}
close HEAD;

# Read in input config file
open(INP, $opt_i) or die "Failed to open file $opt_i for read-only: $!\n\n";
my @input_config = ();
my $in_removable_block = 0;
while ( <INP> ) {
    if ( $in_removable_block ) {
        if ( /;\s*$/ ) {
            $in_removable_block = 0;
        }
        next;
    }
    next if ( /^\s*#/ );
    next if ( /^\s*\%esdts/ );
    next if ( /^\s*\@esdts/ );
    next if ( /^\s*\$make_ph/ );
    next if ( /product PH/ );
    next if ( /^\s*1;/ );
    if ( /^\s*\$all_triggers/ or 
         /^\s*push\s+\@input_esdts/ or 
         /^\s*\$qc_input_esdts/ ) {
        unless ( /;\s*$/ ) {
            $in_removable_block = 1;
        }
        next;
    }
    push(@input_config, $_);
}
close INP;


# Make the cut-and-paste transations

foreach my $line ( @input_config ) {
    $line =~ s/\@sort_pges/\@run_sorted_algorithms/;
    $line =~ s/\@display_sort_pges/\@display_sorted_algorithms/;
    $line =~ s/\$gear/\$instance/;
    $line =~ s/\@subscription_esdts/\@all_external_datatypes/;
    $line =~ s/\$pge_version/\$all_algorithm_versions/;
    $line =~ s/\$esdt_version/\$all_datatype_versions/;
}

# Reopen input, this time for reading in the algorithms listed

my $compartment1 = new Safe 'CFG1';
$compartment1->share('@sort_pges');
$compartment1->rdo($opt_i) or die "Failed to import file $opt_i: $!\n\n";
my $compartment2 = new Safe 'CFG2';
$compartment2->share('%pge_version');
$compartment2->rdo($opt_c) or die "Failed to import file $opt_c: $!\n\n";

my $output = "$opt_i.new";
open(OUT, ">$output") or die "Failed to open file $output for write: $!\n\n";
foreach my $line ( @header ) {
    print OUT $line;
}
print OUT "################################################################################\n";
foreach my $line ( @input_config ) {
    print OUT $line;
}

print OUT "\$algorithm_root = 'ENTER SOMETHING HERE!';\n\n";
print OUT "\%algorithm_versions = (\n";
foreach my $pge ( @CFG1::sort_pges ) {
    if ( $opt_a ) {
        print OUT "    '$pge' => \$airs_version,\n";
    } else {
        print OUT "    '$pge' => '" . $CFG2::pge_version{$pge} . "',\n";
    }
}
print OUT ");\n\n";

print OUT "\%algorithm_profiles = (\n";
foreach my $pge ( @CFG1::sort_pges ) {
    print OUT "    '$pge' => '$profile',\n";
}
print OUT ");\n\n";

print OUT "\n1;\n";

close OUT;

print "\nYour new file is $output. You must edit this file and\nset \$algorithm_root manually. You should also review the file.\n\n";
