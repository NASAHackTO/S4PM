=head1 NAME

Configurator - common routines used in the Configurator station

=head1 SYNOPSIS

use S4PM::Configurator;

$status = run_stringmaster($string_id, $machine, $algorithm_root);

$dir = get_algorithm_root($configurator_cfg, $string_id);

$machine = get_machine_name($configurator_cfg, $string_id);

errorbox($mw, $title, $message);

$response = warningbox($mw, $title, $message);

infobox($mw, $title, $message);

$cmd = get_checkin_cmd($configurator_cfg);

$cmd = get_checkout_cmd($configurator_cfg);

$status = cm_checkin_file($configurator_cfg, $file);

$status = cm_checkout_file($configurator_cfg, $file);

reconfig_all_stations($station_root);

resync_all_stations($station_root);

@algs = get_current_algorithms($machine, $stringid);

stop_station($station_dir);

start_station($station_root, $station);

=head1 DESCRIPTION

This module contains support routines for scripts running in the Configurator
station.

=over 4

=item run_stringmaster

Given the string's string ID, machine name, and algorithm root directory
(available in the s4pm_configurator.cfg file), this function will run
Stringmaker with the -c option on the string.

=item get_algorithm_root

Given the string's string ID and the name of the Configurator station's
configuration file (s4pm_configurator.cfg by default), this function wll
return the alogorithm root directory for this string.

=item get_machine_name

Given the string's string ID and the name of the Configurator station's
configuration file (s4pm_configurator.cfg by default), this function wll
return the machine name for this string.

=item errorbox

Provides a simple way of popping up a Tk error box with a particular window
title and message. The main window object is the first argument. The interface
provides an 'Ok' button. There is no return value.

=item warningbox

Provides a simple way of popping up a Tk warning box with a particular window
title and message. The main window object is the first argument. The interface
provides 'Yes' and 'No' buttons and returns the one selected.

=item infobox

Provides a simple way of popping up a Tk information box with a particular 
window title and message. The main window object is the first argument. The
interface provides a simple 'Ok' button. There is no return value.

=item get_checkin_cmd

Internal subroutine that retrieves the $cfg_checkin value from the Configurator
station's configuration file, s4pm_configurator.cfg.

=item get_checkout_cmd

Internal subroutine that retrieves the $cfg_checkout value from the Configurator
station's configuration file, s4pm_configurator.cfg.

=item cm_checkin_file 

Given the name of the Configuration station's configuration file and the name
of a file to checkin of CM, this function checks in that file.

=item cm_checkout_file 

Given the name of the Configuration station's configuration file and the name
of a file to checkout of CM, this function checks out that file.

=item reconfig_all_stations

Given the station root directory, this function will drop RECONFIG work orders
into all station directories (except for configurator and sub_notify). It
will automatically ignore any disabled stations.

=item resync_all_stations

Given the station root directory, this function will drop RESYNC work orders
into all stations that can accept them. It will automatically ignore any
disabled stations.

=item get_current_algorithms

Retrieves list of current algorithms configurated in the string. List is made
by querying the appropriate string-specific Stringmaker configuration file.
Each item in the returned list is a string consisting of the algorithm name,
version, and profile delimited by pipe characters.

=item stop_station

Stops the station running in directory passed to the subroutine by issuing
a STOP work order. There is no return code.

=item start_station

Starts the station running in the directory passed to the subroutine. There
is no return code.

=back

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# Configurator.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::Configurator;
use strict;
use Cwd;
use File::Basename;
use Safe;
use S4P;
1;

sub run_stringmaker {

    my $stringid = shift;
    my $machine  = shift;
    my $alg_root = shift;

    my $cmd = "s4pm_stringmaker.pl -c -d $machine -s $stringid.cfg";

    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger("ERROR", "S4PM::Configurator::run_stringmaker(): command: [$cmd] failed: $errstr");
        return 1;
    }

    unlink("stringmaker.log");
    system("/bin/rm -fr tmp");

    return 0;
}

sub get_machine_name {

    my $config_file = shift;
    my $stringid = shift;

    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_string_info');
    $compartment->rdo($config_file) or S4P::perish(30, "S4PM::Configurator::get_machine_name(): Failed to import configuration file: $config_file safely: $!");
    my $machine = $CFG::cfg_string_info{$stringid}{'machine'};

    return $machine;
}

sub get_algorithm_root {

    my $config_file = shift;
    my $stringid = shift;

    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_string_info');
    $compartment->rdo($config_file) or S4P::perish(30, "S4PM::Configurator::get_algorithm_root(): Failed to import configuration file: $config_file safely: $!");
    my $root = $CFG::cfg_string_info{$stringid}{'root_dir'};

    $root .= "/ALGORITHMS";
    return $root;
}

sub errorbox {

    my ($mw, $title, $msg) = @_;

    $mw->messageBox(-title => "$title",
                    -message => "$msg",
                    -type    => "OK",
                    -icon    => 'error',
                    -default => 'ok',
                   );

}

sub warningbox {

    my ($mw, $title, $msg) = @_;

    my $answer = $mw->messageBox(-title => "$title",
                                 -message => "$msg",
                                 -type    => "YesNo",
                                 -icon    => 'question',
                                 -default => 'ok',
                                );
    return $answer;

}

sub infobox {

    my ($mw, $title, $msg) = @_;

    $mw->messageBox(-title => "$title",
                    -message => "$msg",
                    -type    => "OK",
                    -icon    => 'info',
                    -default => 'ok',
                   );
}

sub get_checkin_cmd {

    my $config_file = shift;

    my $compartment = new Safe 'CFG';
    $compartment->share('$cfg_checkin');
    $compartment->rdo($config_file) or S4P::perish(30, "S4PM::Configurator::get_checkin_cmd(): Failed to import configuration file: $config_file safely: $!");
    if ( $CFG::cfg_checkin ) {
        return $CFG::cfg_checkin;
    } else {
        return undef;
    }
}

sub get_checkout_cmd {

    my $config_file = shift;

    my $compartment = new Safe 'CFG';
    $compartment->share('$cfg_checkout');
    $compartment->rdo($config_file) or S4P::perish(30, "S4PM::Configurator::get_checkout_cmd(): Failed to import configuration file: $config_file safely: $!");
    if ( $CFG::cfg_checkout ) {
        return $CFG::cfg_checkout;
    } else {
        return undef;
    }
}

sub cm_checkout_file {

    my $config_file = shift;
    my $file = shift;

    my $pwd = cwd();   # save for later

    my $dir = dirname($file);
    my $base = basename($file);
    chdir($dir);
    my $cmd = S4PM::Configurator::get_checkout_cmd($config_file);
    if ( ! $cmd ) {
        chdir($pwd);
        return undef;
    }
    $cmd =~ s/%s/$base/g;

    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger("FATAL", "Configurator::cm_checkout_file(): Failed to check out file. This was the return string: [$errstr]");
        return 1;
    }

    chdir($pwd);

    return 0;
}

sub cm_checkin_file {

    my $config_file = shift;
    my $file = shift;

    my $pwd = cwd();   # save for later

    my $dir = dirname($file);
    my $base = basename($file);
    chdir($dir);
    my $cmd = S4PM::Configurator::get_checkin_cmd($config_file);
    if ( ! $cmd ) {
        chdir($pwd);
        return undef;
    }
    $cmd =~ s/%s/$base/g;

    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        return 1;
        S4P::logger("FATAL", "Configurator::cm_checkin_file(): Failed to check in file. This was the return string: [$errstr]");
    }

    chdir($pwd);

    return 0;
}

sub resync_all_stations {

    my $dir = shift;

    my $pwd = cwd();
    chdir($dir);

    system("echo NULL> $dir/prepare_run/DO.RESYNC.$$.wo");
    chdir($pwd);

    return;
}

sub reconfig_all_stations {

    my $dir = shift;

    my $pwd = cwd();
    chdir($dir);

    my @station_configs = glob("$dir/*/station.cfg");
    foreach my $sta ( @station_configs ) {
        next if ( $sta =~ /configurator/ );
        next if ( $sta =~ /sub_notify/ );
        next if ( $sta =~ /track_data/ );  # Handled separately
        my $compartment = new Safe 'CFG';
        $compartment->share('$cfg_disable');
        $compartment->rdo($sta) or S4P::perish(30, "S4PM::Configurator::reconfig_all_stations(): Failed to import configuration file: $sta safely: $!");
        unless ( $CFG::cfg_disable ) {
            my $station_dir = dirname($sta);
            system("touch $station_dir/DO.RECONFIG.$$.wo");
        }
    }

    chdir($pwd);

    return;
}

sub get_current_algorithms {

    my $machine  = shift;
    my $stringid = shift;

    my $cfg = "$machine/$stringid.cfg";

### Import the string-specific configuration file

    my $compartment = new Safe 'STR';
    $compartment->share('@run_sorted_algorithms', '%algorithm_versions', '%algorithm_profiles');
    unless ( $compartment->rdo($cfg) ) {
        return undef;
    }

    my @ar = ();
    foreach my $alg ( @STR::run_sorted_algorithms ) {
        my $item = $alg . "|" . $STR::algorithm_versions{$alg} . "|" . $STR::algorithm_profiles{$alg};
        push(@ar, $item);
    }

    return @ar;
}

sub stop_station {

    my $stationdir = shift;

    system("touch $stationdir/DO.STOP.$$.wo");
}

sub start_station {

    my $station_root = shift;
    my $station      = shift;

    chdir($station_root);

    if ( -f "nohup.out" ) {
        open(OUT, ">>nohup.out") or 
            S4P::perish(30, "start_station(): Failed to open: nohup.out: $!");
        print OUT "--------------------------------------------------\n";
        my $d = localtime( time() );
        print OUT "$d\n";
        print OUT "Starting up station $station\n";
    }

    system("nohup stationmaster.pl -d $station &");

}
