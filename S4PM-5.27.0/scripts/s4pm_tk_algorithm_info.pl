#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_algorithm_info.pl - pop-up box displaying algorithm information

=head1 SYNOPSIS

s4pm_tk_algorithm_info.pl

=head1 DESCRIPTION

This script displays a pop-up box that displays algorithm information for the
algorithms running in this S4PM string. Information includes the algorithm 
name and version; and the input and output data types and their versions.

Note that only algorithms currently configured to run in the Run Algorithm
station are displayed. Algorithms that are installed, but not configured to
run are not shown in the display.

=head1 AUTHOR

Stephen berrick

=cut

################################################################################
# s4pm_tk_algorithm_info.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use S4PM;
use S4PM::Algorithm;
require 5.6.0;
use Safe;
use Tk;

my $main = MainWindow->new();
$main->title("Algorithm Information Box");

# Verify we're being run from the correct directory location

unless ( -e "prepare_run" ) {
    S4P::perish(1, "Could not find prepare_run directory. Are you running me from the right directory?");
}

my @candidate_config_files =  glob("prepare_run/s4pm_select_data*.cfg");
if ( scalar(@candidate_config_files) == 0 ) {
    S4P::perish(1, "Could not find any algorithm Select Data configuration files.");
}

# Now screen the list of candidate config files for algorithms that are
# actually configured to run in Run Algorithm. The rest are dropped.

my @config_files = get_operational(@candidate_config_files);

my $count = 0;

$main->Label(-padx => 5, -pady => 5, -relief => 'raised', -justify => 'center', -background => 'brown', -foreground => 'white', -text => "Algorithm Name/Version")->grid(
    $main->Label(-relief => 'raised', -justify => 'center', -background => 'brown', -foreground => 'white', -text => "Input Data Types"),
    $main->Label(-relief => 'raised', -justify => 'center', -background => 'brown', -foreground => 'white', -text => "Output Data Types"),
    -sticky => "nsew");

my $fg = "black";
my $bg;
my $pbg;

my $count = 1;
foreach my $config_file ( @config_files ) {
    my $unknown = 0;

    unless ( -e $config_file ) {
        S4P::logger("ERROR", "main: Could not find $config_file. I'm skipping it.");
        $unknown = 1;
    }

    my $algorithm;
    unless ( $unknown ) {
        $algorithm = new S4PM::Algorithm($config_file);
        unless ( $algorithm ) {
            S4P::logger("ERROR", "main: Could not create new S4PM::Algorithm object from reading $config_file. I'm skipping it.");
            $unknown = 1;
        }
    }

    if ( $count/2 == int($count/2) ) {
        $bg = "white";
        $pbg = ($unknown) ? "red" : "brown";
    } else {
        $bg = "gray";
        $pbg = ($unknown) ? "red" : "black";
    }

    my $algorithm_name_ver = ($unknown) ? "READ ERROR!!!" : $algorithm->pge_name . "  v" . $algorithm->pge_version;
    my @input_groups  = ($unknown) ? () : @{ $algorithm->input_groups };
    my @output_groups = ($unknown) ? () : @{ $algorithm->output_groups };
    my $input_str = "";
    my @input_esdts = ();
    foreach my $input_group ( @input_groups ) {
        push(@input_esdts, $input_group->data_type . "." . $input_group->data_version);
    }
    my @unique_input_esdts = uniq(@input_esdts);
    foreach my $ie ( @unique_input_esdts ) {
        $input_str .= "$ie ";
    }
    my $output_str = "";
    my @output_esdts = ();
    foreach my $output_group ( @output_groups ) {
        push(@output_esdts, $output_group->data_type . "." . $output_group->data_version);
    }
    my @unique_output_esdts = uniq(@output_esdts);
    foreach my $oe ( @unique_output_esdts ) {
        $output_str .= "$oe ";
    }
    $main->Label(-relief => 'raised', -padx => 5, -pady => 5, -anchor => 'e', -justify => 'right', -background => $pbg, -foreground => 'white', -text => $algorithm_name_ver)->grid(
        $main->Label(-justify => 'left', -wraplength => 500, -anchor => 'w', -text => $input_str, -background => $bg, -foreground => $fg),
        $main->Label(-justify => 'left', -wraplength => 500, -anchor => 'w', -text => $output_str, -background => $bg, -foreground => $fg),
        -sticky => "nsew");

    $count++;
}

$main->Button(-text => "Close", -padx => 5, -pady => 5, -command => sub { exit; })->grid("-", "-", -padx => 5, -pady => 5);

MainLoop();

sub uniq {

    my @ar = @_;

    my %seen = ();
    foreach my $item ( @ar ) {
        $seen{$item}++;
    }
    return keys %seen;
}

sub get_operational {

    my @configs = @_;
    my @running = ();

    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_commands');
    $compartment->rdo("run_algorithm/station.cfg") or 
        S4P::perish(10, "get_operational(): Failed to import run_algorithm/station.cfg in Safe mode: $!");
    
    my @algs = ();
    foreach my $alg ( keys %CFG::cfg_commands ) {
        $alg =~ s/^RUN_//;
        push(@algs, $alg);
    }

    foreach my $c ( @configs ) {
        foreach my $a ( @algs ) {
            if ( $c =~ /$a\.cfg$/ ) {
                push(@running, $c);
            }
        }
    }

    return @running;
}
