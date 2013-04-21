#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_install_algorithm.pl - install an algorithm into S4PM string

=head1 SYNOPSIS

s4pm_tk_install_algorithm.pl

=head1 DESCRIPTION

B<s4pm_tk_install_algorithm.pl> is intended to be run from within an existing
S4PM string via a tool available in the Configurator station. The tool
allows an algorithm to be installed into S4PM.

B<s4pm_tk_install_algorithm.pl> needs to know the string ID of the string in
which to install the algorithm. The script gets the string ID from stdin. The 
tool B<s4pm_tk_select_string.pl> was designed to allow the user to select
a string and then write the value to stdout. Thus, this script can be run
in a piped command as: s4pm_tk_select_string.pl | s4pm_tk_install_algorithm.pl

This script first verifies the selected algorithm configuration file against
the B<s4pm_stringmaker_datatypes.cfg> file and issues warnings or errors
as appropriate. 

Required changes are automatically made to the string-specific configuration
file (if needed), before Stringmaker is run. B<s4pm_tk_install_algorithm.pl>
will use the values of $cfg_checkout and $cfg_checkin in the 
B<s4pm_configurator.cfg> file to check out and check in the string-specific
configuration file (e.g. from SCCS or RCS) as needed. Changes made to the 
string-specific configuration are prepended with a date/time stamp message.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_install_algorithm.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
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
use vars qw($stringid $mw);

my $machine = hostname();

my $stringid = <STDIN>;
chomp($stringid);

my @current_algorithms = S4PM::Configurator::get_current_algorithms($machine, $stringid);
unless ( @current_algorithms ) {
    S4PM::Configurator::errorbox($mw, "Configuration Error!", "Unable to import string configuration file for $stringid.");
    exit 1;
}

my $date = localtime(time);
my $algorithm_cfg;

# Get root directory of where algorithms are installed

my $alg_root = S4PM::Configurator::get_algorithm_root("s4pm_configurator.cfg", $stringid);

# Set up Tk stuff

$mw = MainWindow->new();
$mw->title("Algorithm Installation Tool");
my $label_frame1 = $mw->Frame->pack(-expand => 1, -fill   => 'both', -pady   => 20, -padx   => 20,);
my $label_frame2 = $mw->Frame->pack(-expand => 1, -fill   => 'both', -pady   => 20, -padx   => 20,);
my $label_frame3 = $mw->Frame->pack(-expand => 1, -fill   => 'both', -pady   => 20, -padx   => 20,);
my $txt1 = "S4PM Algorthm Installation Tool For $stringid";
my $label1 = $label_frame1->Label(-wraplength => 800, -justify => 'left', 
                                -text => $txt1, -relief => 'flat', 
                                -background => 'white', -foreground => 'blue',
                                )->grid(-row => 0, -columnspan => 2, 
                                        -sticky => 'nsew',
);
my $txt2 = "DIRECTIONS: Enter in the full path to the algorithm configuration file for the algorithm to install either by typing in the full path or by browsing using the indicated button. Then click on Install Selected Algorithm.";
my $label2 = $label_frame2->Label(-wraplength => 800, -justify => 'left', 
                                -text => $txt2, -relief => 'groove', 
                                -pady => 10, -pady => 10,
                                )->grid(-row => 0, -columnspan => 2, 
                                        -sticky => 'nsew',
);
my $txt3 = "Below is a list of the currently installed algorithms:";
my $label3 = $label_frame3->Label(-wraplength => 800, -justify => 'left', 
                                -text => $txt3, -relief => 'flat', 
                                -pady => 10, -pady => 10,
                                )->grid(-row => 0, -column => 0,
                                        -sticky => 'w',
);
my $alg_frame  = $mw->Frame->pack(-expand => 1, -fill => 'both');
my $choice;
foreach my $str ( @current_algorithms ) {
    my ($alg, $ver, $prof) = split(/\|/, $str);
    my $line = "$alg, Version $ver, Profile $prof";
    $alg_frame->Label(-text => $line, -anchor => 'w',
                     )->pack(-side => 'top', -expand => 1, -anchor => 'w',
                             -fill => 'both', -padx => 10, -pady => 5,);
}

my $entry_frame  = $mw->Frame->pack(-expand => 1, -fill => 'both');
my $entry = $entry_frame->Entry(-textvariable => \$algorithm_cfg, 
                                -width => 60)->pack(-side => 'left', 
                                -expand => 1, -fill => 'both', -padx => 10, 
                                -pady => 5,
);
my $submit_button = $entry_frame->Button(-text => 'Browse for new algorithm...',
    -command => sub { $algorithm_cfg = get_file($alg_root) } )->pack(-side => 'left', -expand => 1, -fill => 'both', -padx => 10, -pady => 5);

my $button_frame  = $mw->Frame->pack(-expand => 1, -fill => 'both');

my %exclusions = ();
my $install_button = $button_frame->Button(-text => 'Install Selected Algorithm',
    -command => sub { 
        my $id = undef;
        my $input_uses_rf = undef;
        ($input_uses_rf, $id) = read_config($algorithm_cfg); 
        my $station_root = $alg_root;
        $station_root =~ s/ALGORITHMS$//;
        if ( $id ) {
            update_config_files($id, $machine, $stringid);
            $mw->configure(-cursor => 'watch');
            $mw->grab;
            S4PM::Configurator::stop_station("$station_root/track_data");
            my $stat = S4PM::Configurator::run_stringmaker($stringid, $machine, $alg_root);
            $mw->configure(-cursor => 'left_ptr');
            if ( $stat ) {
                S4PM::Configurator::errorbox($mw, "Stringmaker Failure", "Stringmaker failed to modify string using $algorithm_cfg. You will need to address the failure before trying again.");
                S4PM::Configurator::start_station($station_root, "track_data");
                exit 1;
            } else {
                S4PM::Configurator::reconfig_all_stations($station_root);
                S4PM::Configurator::resync_all_stations($station_root);
                track_data_update($station_root, $input_uses_rf);
                sleep(10);      # Make sure work order is there before starting
                S4PM::Configurator::start_station($station_root, "track_data");
                S4PM::Configurator::infobox($mw, "Success!", "Stringmaker has completed successfully. You may want to review the configuration files in the string before using the algorithm.");
                goodbye();
            }
        }
    },)->pack(-side => 'left', -expand => 1, -fill => 'both', -padx => 10, -pady => 5);

my $exit_button = $button_frame->Button(-text => 'Exit',
    -command => sub { goodbye(); },)->pack(-side => 'left', -expand => 1,
    -fill => 'both', -padx => 10, -pady => 5);

MainLoop();

sub read_config {

    my $algorithm_cfg = shift;

    if ( $algorithm_cfg =~ /^\s*$/ ) {
        S4PM::Configurator::errorbox($mw, "No File Selected", "You did not select an algorithm configuration file!");
        return undef;
    }
    unless ( -e $algorithm_cfg ) {
        S4PM::Configurator::errorbox($mw, "File Does Not Exist", "The file you selected or entered doesn't seem to exist!");
        return undef;
    }

    my $profile = $algorithm_cfg;
    $profile =~ s/\.cfg$//;
    $profile = reverse $profile;
    $profile =~ s/_.*$//;
    $profile = reverse $profile;

### Import the algorithm configuration file

    my $alg_compartment = new Safe 'ALG';
    $alg_compartment->share('%inputs', '%outputs', '%input_uses');
    unless ( $alg_compartment->rdo($algorithm_cfg) ) {
        S4PM::Configurator::errorbox($mw, "Failed To Import", "read_config(): Failed to import configuration file $algorithm_cfg safely: $!");
        return undef;
    }

### Import the Stringmaker data types configuration file

    my $datatype_config = "s4pm_stringmaker_datatypes.cfg";
    my $data_compartment = new Safe 'DATA';
    $data_compartment->share('%all_datatype_max_sizes',  
                             '%all_datatype_versions',
                             '%ragged_file_trap',        
                             '%register_data_offsets',
                             '%data_file_qa',            
                             '%non_hdf_datatypes',
                             '%skip_checksum_datatypes', 
                             '%qc_output',
                             '$algorithm_name',
                             '$algorithm_version',
    );
    $data_compartment->rdo($datatype_config) or S4P::perish(30, "main: Failed to import configuration file: $datatype_config safely: $!");

### Open the Stringmaker data types configuration file for possible updated

### Verify that all data types in the algorithm configuration file are listed
### in the data types configuration file. If not, get the information need.

    my $error_message = "";
    my %already_seen = ();
    my %exclusions = ();

    foreach my $input ( keys %ALG::inputs ) {
        my $dt  = $ALG::inputs{$input}{'data_type'};
        next if ( $already_seen{$dt} );
        $already_seen{$dt} = 1;
        my $ver = $ALG::inputs{$input}{'data_version'};

####### Is there a max size for this data type?

        unless ( exists $DATA::all_datatype_max_sizes{$dt} ) {
            $error_message .= "\nNo maximum size for data type $dt has been specified in Stringmaker data types config file.\n";
        }

####### Is there a version for this data type?

        unless ( $DATA::all_datatype_versions{$dt} eq $ver ) {
            $error_message .= "\nData type $dt with version $ver does not exist in Stringmaker data types config file.\n";
        }

####### Is this data type in the %ragged_file_trap and should it be?

        unless ( exists $DATA::ragged_file_trap{$dt} ) {
            push( @{$exclusions{$dt}}, '%ragged_file_trap');
        }

####### Is this data type in the %register_data_offsets and should it be?

        unless ( exists $DATA::register_data_offsets{$dt} ) {
            push( @{$exclusions{$dt}}, '%register_data_offsets');
        }

####### Is this data type in the %data_file_qa and should it be?

        unless ( exists $DATA::data_file_qa{$dt} ) {
            push( @{$exclusions{$dt}}, '%data_file_qa');
        }

####### Is this data type in the %non_hdf_datatypes and should it be?

        unless ( exists $DATA::non_hdf_datatypes{$dt} ) {
            push( @{$exclusions{$dt}}, '%non_hdf_datatypes');
        }

####### Is this data type in the %skip_checksum_datatypes and should it be?

        unless ( exists $DATA::skip_checksum_datatypes{$dt} ) {
            push( @{$exclusions{$dt}}, '%skip_checksum_datatypes');
        }

####### Is this data type in the %qc_output and should it be?

        unless ( exists $DATA::qc_output{$dt} ) {
            push( @{$exclusions{$dt}}, '%qc_output');
        }
    }

    unless ( $error_message eq "" ) {
        $error_message .= "\nTHIS ALGORITHM CANNOT BE INSTALLED UNTIL THE ABOVE ERRORS ARE FIXED.";
        S4PM::Configurator::errorbox($mw, "Configuration Errors", $error_message);
        exit 1;
    }

    my $msg = get_warning_string(%exclusions);
    $msg = "The following messages are warnings only. The algorithm may be safely installed despite them. You can choose to continue or exit to resolve these warnings.\n\n" . $msg . "\n\nDo you wish to continue?";
    my $decision = S4PM::Configurator::warningbox($mw, "Configuration Warnings", $msg);
    if ( $decision eq 'No' ) {
        goodbye();
    }

    my $input_uses_rf = \%ALG::input_uses;
    return ($input_uses_rf, "$ALG::algorithm_name|$ALG::algorithm_version|$profile");
}

sub get_warning_string {

    my %exclusions = @_;

    my $str = "";
    my $count = 0;
    foreach my $dt ( keys %exclusions ) {
        $count++;
        $str .= "\n$dt is missing from: ";
        foreach my $item ( @{$exclusions{$dt}} ) {
            $str .= "\n\t$item, ";
        }
        $str =~ s/,\s+$//;
        if ( $count > 4 ) {
            $str .= "\n\nThere were too many warnings to show all.";
            return $str;
        }
    }
    return $str;
}

sub get_file {

    my $initialdir = shift;

    my $types = [
        ['Config Files', '.cfg'],
        ['All Files',    '*',],
    ];
    my $title = "Select an algorithm configuration file";

    my $file = $mw->getOpenFile(-filetypes => $types, -title => $title,
                                -initialdir => $initialdir);
    return $file;
}

sub goodbye {

    $mw->destroy;
    exit 0;
}

sub update_config_files {

    my $id       = shift;
    my $machine  = shift;
    my $stringid = shift;

    my ($new_algorithm, $new_version, $new_profile) = split(/\|/, $id);

    my $cfg = "$machine/$stringid.cfg";

### Import the string-specific configuration file

    my $compartment = new Safe 'STR';
    $compartment->share('@run_sorted_algorithms', 
                        '%algorithm_versions', 
                        '%algorithm_profiles',
                        '%all_datatype_versions',
                        '%all_datatype_max_sizes',
    );
    unless ( $compartment->rdo($cfg) ) {
        S4PM::Configurator::errorbox($mw, "Failed To Import", "update_config_files(): Failed to import configuration file $cfg safely: $!");
        return undef;
    }

### Open string-specific file for appending

    S4PM::Configurator::cm_checkout_file("../s4pm_configurator.cfg", $cfg);
    open(CFG, ">>$cfg") or S4P::perish(30, "update_config_files(): Failed to open file: $cfg for append: $!");

### Set string to be appended to string-specific file

    my $output_str = "";

### See if algorithm is already in list, otherwise add it in

    my @newlist = ();
    my $is_there = 0;
    foreach my $item ( @STR::run_sorted_algorithms ) {
        if ( $new_algorithm eq $item ) {
            $is_there = 1;
        }
        push(@newlist, $item);
    }

    unless ( $is_there ) {
        $output_str .= "\@run_sorted_algorithms = ('$new_algorithm', ";
        foreach my $item ( @STR::run_sorted_algorithms ) {
            $output_str .= "'$item', ";
        }
        $output_str .= ");\n";
    }

### See if algorithm version is already there, otherwise add it in

    unless ( exists $STR::algorithm_versions{$new_algorithm} and
             $STR::algorithm_versions{$new_algorithm} eq $new_version ) {
        $output_str .=  "\$algorithm_versions{'$new_algorithm'} = '$new_version';\n";
    }

### See if algorithm profile is already there, otherwise add it in

    unless ( exists $STR::algorithm_profiles{$new_algorithm} and
             $STR::algorithm_profiles{$new_algorithm} eq $new_profile ) {
        $output_str .= "\$algorithm_profiles{'$new_algorithm'} = '$new_profile';\n";
    }

    my $date = localtime(time);
    unless ( $output_str eq "" ) {
        $output_str = "\n# The following update was made by the Algorithm Installation Tool\n# on $date\n\n" . $output_str . "\n1;\n";
        print CFG $output_str;
    }

    close(CFG);
    S4PM::Configurator::cm_checkin_file("../s4pm_configurator.cfg", $cfg);
        
}

sub track_data_update {

    my $station_root = shift;
    my $input_uses_rf = shift;

    my $update_wo = "DO.UPDATE.$$.wo";
    open(UPDATE, ">$update_wo") or 
        S4P::perish(10, "track_data_update(): Failed to open new work order $update_wo for write: $!");
    foreach my $input ( keys %{$input_uses_rf} ) {
        print UPDATE "FileId=\*$input* Uses=+" . $input_uses_rf->{$input} . "\n";
    }
    close(UPDATE);
    system("mv $update_wo $station_root/track_data");
}
