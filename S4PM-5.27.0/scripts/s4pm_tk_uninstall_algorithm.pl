#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_uninstall_algorithm.pl - uninstall an algorithm from an S4PM string

=head1 SYNOPSIS

s4pm_tk_uninstall_algorithm.pl

=head1 DESCRIPTION

B<s4pm_tk_uninstall_algorithm.pl> is intended to be run from within an existing
S4PM string via a tool available in the Configurator station. The tool
allows an algorithm to be uninstalled from S4PM.

B<s4pm_tk_uninstall_algorithm.pl> needs to know the string ID of the string in
which to uninstall the algorithm. The script gets the string ID from stdin. The 
tool B<s4pm_tk_select_string.pl> was designed to allow the user to select
a string and then write the value to stdout. Thus, this script can be run
in a piped command as: s4pm_tk_select_string.pl | s4pm_tk_uninstall_algorithm.pl

Changes are made automatically made to the string-specific configuration
file before Stringmaker is run. B<s4pm_tk_uninstall_algorithm.pl> will use the 
values of $cfg_checkout and $cfg_checkin in the B<s4pm_configurator.cfg> file 
to check out and check in the string-specific configuration file (e.g. from 
SCCS or RCS) as needed. Changes made to the string-specific configuration are 
prepended with a date/time stamp message.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_uninstall_algorithm.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use File::Basename;
use Safe;
use Sys::Hostname;
use Getopt::Std;
use S4PM::Configurator;
use Tk;
use Cwd;
use File::Copy;
use vars qw($stringid $mw);

my $machine = hostname();

my $stringid = <STDIN>;
chomp($stringid);

my $date = localtime(time);
my $algorithm_cfg;

$mw = MainWindow->new();

my @current_algorithms = S4PM::Configurator::get_current_algorithms($machine, $stringid);
unless ( @current_algorithms ) {
    S4PM::Configurator::errorbox($mw, "Configuration Error!", "Unable to import string configuration file for $stringid.");
    exit 1;
}

# Get root directory of where algorithms are installed

my $alg_root = S4PM::Configurator::get_algorithm_root("s4pm_configurator.cfg", $stringid);

# Set up Tk stuff

$mw->title("Algorithm Uninstall Tool");
my $label_frame1 = $mw->Frame->pack(-expand => 1, -fill   => 'both', -pady   => 20, -padx   => 20,);
my $label_frame2 = $mw->Frame->pack(-expand => 1, -fill   => 'both', -pady   => 20, -padx   => 20,);
my $txt1 = "S4PM Algorthm Uninstallation Tool For $stringid";
my $label1 = $label_frame1->Label(-wraplength => 800, -justify => 'left', 
                                -text => $txt1, -relief => 'flat', 
                                -background => 'white', -foreground => 'blue',
                                )->grid(-row => 0, -columnspan => 2, 
                                        -sticky => 'nsew',
);
my $txt2 = "DIRECTIONS: Below is a list of algorithms currently installed into this string. Select one to uninstall and then click on Uninstall Selected Algorithm.";
my $label2 = $label_frame2->Label(-wraplength => 800, -justify => 'left', 
                                -text => $txt2, -relief => 'groove', 
                                -pady => 10, -pady => 10,
                                )->grid(-row => 0, -columnspan => 2, 
                                        -sticky => 'nsew',
);
my $button_frame  = $mw->Frame->pack(-expand => 1, -fill => 'both');
my $choice;
foreach my $str ( @current_algorithms ) {
    my ($alg, $ver, $prof) = split(/\|/, $str);
    my $line = "$alg, Version $ver, Profile $prof";
    $button_frame->Radiobutton(-text => $line,
                     -value => $str, -padx => 10, -pady => 5, -anchor => 'w',
                     -variable => \$choice)->pack(-side => 'top',
                                                  -expand => 1,
                                                  -anchor => 'w',
                                                  -fill => 'both',
                                                  -padx => 10, -pady => 5,);
}


my $button_frame  = $mw->Frame->pack(-expand => 1, -fill => 'both');

my %exclusions = ();
my $install_button = $button_frame->Button(-text => 'Uninstall Selected Algorithm', -command => sub { 
        update_config_files($choice, $machine, $stringid);
        save_alloc_config($alg_root);
        my $stat = S4PM::Configurator::run_stringmaker($stringid, $machine, $alg_root); 
        if ( $stat ) {
            S4PM::Configurator::errorbox($mw, "Stringmaker Failure", "Stringmaker failed to modify string using $algorithm_cfg. You will need to address the failure before trying again.");
            exit 1;
        } else {
            S4PM::Configurator::infobox($mw, "Success!", "Stringmaker has completed successfully. You may want to review the configuration files in the string before using the algorithm.");
            my $station_root = $alg_root;
            $station_root =~ s/ALGORITHMS$//;
            S4PM::Configurator::reconfig_all_stations($station_root);
            S4PM::Configurator::resync_all_stations($station_root);
            goodbye();
        }
    }
)->pack(-side => 'left', -expand => 1, -fill => 'both', -padx => 10, -pady => 5);

my $exit_button = $button_frame->Button(-text => 'Exit',
    -command => sub { goodbye(); },)->pack(-side => 'left', -expand => 1,
    -fill => 'both', -padx => 10, -pady => 5);

MainLoop();

sub save_alloc_config {

    my $dir = shift;
    $dir =~ s/ALGORITHMS$//;
    my $pwd = cwd();
    chdir($dir);

    print "Copying $dir/allocate_disk/s4pm_allocate_disk_pool.cfg to $dir/allocate_disk/s4pm_allocate_disk_pool.cfg.old\n";
    copy("$dir/allocate_disk/s4pm_allocate_disk_pool.cfg", "$dir/allocate_disk/s4pm_allocate_disk_pool.cfg.old");
    
    chdir($pwd);

    return;
}

sub goodbye {

    $mw->destroy;
    exit 0;
}

sub update_config_files {

    my $choice   = shift;
    my $machine  = shift;
    my $stringid = shift;

    my ($algorithm, $version, $profile) = split(/\|/, $choice);

    my $cfg = "$machine/$stringid.cfg";

### Import the string-specific configuration file

    my $compartment = new Safe 'STR';
    $compartment->share('@run_sorted_algorithms');
    unless ( $compartment->rdo($cfg) ) {
        S4PM::Configurator::errorbox($mw, "Failed To Import", "update_config_files(): Failed to import configuration file $cfg safely: $!");
        return undef;
    }

### Open string-specific file for appending

    S4PM::Configurator::cm_checkout_file("../s4pm_configurator.cfg", $cfg);
    open(CFG, ">>$cfg") or S4P::perish(30, "update_config_files(): Failed to open file: $cfg for append: $!");

### Set string to be appended to string-specific file

    my $output_str = "";

### Write out a new @run_sorted_algorithms array without the chosen algorithm

    my @newlist = ();
    foreach my $item ( @STR::run_sorted_algorithms ) {
        unless ( $algorithm eq $item ) {
            push(@newlist, $item);
        }
    }

    $output_str .= "\@run_sorted_algorithms = (";
    foreach my $item ( @newlist ) {
        $output_str .= "'$item', ";
    }
    $output_str .= ");\n";

    my $date = localtime(time);
    $output_str = "\n# The following update was made by the Algorithm Installation Tool\n# on $date\n\n" . $output_str . "\n1;\n";
    print CFG $output_str;

    close(CFG);
    S4PM::Configurator::cm_checkin_file("../s4pm_configurator.cfg", $cfg);
        
}
