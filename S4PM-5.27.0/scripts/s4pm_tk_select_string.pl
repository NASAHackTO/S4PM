#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_select_string.pl - window in which to select a S4PM string

=head1 SYNOPSIS

s4pm_tk_select_string.pl

=head1 DESCRIPTION

B<s4pm_tk_select_string.pl> is a simple GUI that allows a user to select
an S4PM string and then write that value out to stdout. It is intended to
feed other tools such as B<s4pm_tk_install_algorithm.pl>. Only those strings
available on that machine are shown in the display.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_select_string.pl,v 1.3 2006/12/06 15:25:28 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Safe;
use Sys::Hostname;
use Tk;

my @available_strings = get_strings();

# Set up Tk stuff

my $mw = MainWindow->new();
$mw->title("Select S4PM String");

my $label_frame   = $mw->Frame->pack();
my $button_frame  = $mw->Frame->pack();

$label_frame->Label(
    -text => "Select a S4PM string from the list below:",
    -background => "white", -foreground => "red", -padx => 20, -pady => 10,
    )->pack(-expand => 1, -fill => 'both');

my $rb_value;
foreach my $str ( @available_strings ) {
    $button_frame->Radiobutton(-text => $str,
                     -value => $str,
                     -padx => 10, -pady => 5,
                     -anchor => 'w',
                     -variable => \$rb_value)->pack(-side => 'top',
                                                    -anchor => 'w');
}

$button_frame->Button(-text => 'Done', 
                      -command => sub { print "$rb_value\n"; exit 0; },
                     )->pack(-side => 'top');

MainLoop();

sub get_strings {

    my $config_file = "s4pm_configurator.cfg";

    my @ar = ();
    my $machine = hostname();

    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_string_info');
    $compartment->rdo($config_file) or S4P::perish(30, "main: Failed to import configuration file: $config_file safely: $!");
    
    foreach my $str ( keys %CFG::cfg_string_info ) {
        if ( $CFG::cfg_string_info{$str}{'machine'} eq $machine ) {
            push(@ar, $str);
        }
    }

    return @ar;       
}
