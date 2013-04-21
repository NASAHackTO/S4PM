#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_configurator.pl - run Stringmaker in this string

=head1 SYNOPSIS

s4pm_configurator.pl
B<-m> I<mode>
B<-r> I<root_directory>
B<-c> I<config_file>

=head1 DESCRIPTION

=head1 ARGUMENTS

=over 4

=item B<-c> I<config_file>

Specifies the fullpath to the configuration file for this station.

=item B-m> I<mode>

Specifies the type of mode or operation to perform. There is currently only
one valid choice: maxjobs

=item B<-r> I<root_directory>

For future use. Not used currently.

=back

=head1 AUTHOR

Stephen W Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_configurator.pl,v 1.3 2007/04/12 14:30:52 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Sys::Hostname;
use S4P;
use File::Basename;
use Getopt::Std;
use Cwd;
use Safe;
use vars qw($opt_c $opt_m $opt_r);

getopts('c:r:m:');

my $wo = $ARGV[0];
my %children_updates = ();

unless ( $opt_c ) {
    S4P::perish(30, "main: No configuration file specified with the -c argument.");
}

unless ( $opt_m ) {
    S4P::perish(30, "main: No mode file specified with the -m argument.");
}

### Read work order

my $compartment = new Safe 'CFG';
$compartment->share('%cfg_string_info');
$compartment->rdo($opt_c) or S4P::perish(10, "main: Failed to import config file: $opt_c: $!");

if ( $opt_m eq "disable" or $opt_m eq "enable" ) {
    sleep_and_wake($wo, $opt_r);
    exit 0;
}

if ( $opt_m eq "maxjobs" ) {

    open(WO, $wo) or S4P::perish(10, "main: Failed to open work order file: $wo: $!");
    print "Reading work order...\n\n";
    while ( <WO> ) {
        my ($string, $station, $num_jobs) = split(/\s+/);
        print "string: [$string], station: [$station], num_jobs: [$num_jobs]\n";
        $children_updates{$string}{$station} = $num_jobs;
    }
    close WO;

    my $script;
    my $tool;
    if ( -e "../s4pm_stringmaker_datatypes.cfg" and 
         -e "../s4pm_stringmaker_global.cfg" ) {
        $script = "s4pm_stringmaker.pl";
        $tool   = "Stringmaker";
    } else {
        S4P::perish(30, "main: At least one of ../s4pm_stringmaker_datatypes.cfg and ../s4pm_stringmaker_global.cfg seem to be missing: $!");
    }

### Run Stringmaker on the affected strings

    print "Running $tool on affected strings...\n\n";
    my $machine = hostname();
    foreach my $string ( keys %children_updates ) {
        my $cmd = "$script -a -d ../$machine -s ../$machine/$string.cfg";
        print "Executing this command: [$cmd]\n";
        my ($rs, $rc) = S4P::exec_system("$cmd");
        if ( $rc ) {
            S4P::perish(30, "main: $tool failed: [$rs]");
        } else {
            S4P::logger("INFO", "main: $tool succeeded!");
        }
    }
        
### Drop RECONFIG work orders into the affect stations of the affected strings
### (if the station is down, it doesn't bother)

    foreach my $string ( keys %children_updates ) {
        foreach my $station ( keys %{$children_updates{$string}} ) {
            my $dir = $CFG::cfg_string_info{$string}{'root_dir'};
            if ( S4P::check_station("$dir/$station") ) {
                my ($rs, $rc) = S4P::exec_system("touch $dir/$station/DO.RECONFIG.wo");
            }
        }
    }

}
    
unlink("stringmaker.log");
system("/bin/rm -fr tmp");

exit 0;

sub sleep_and_wake {

    my $wo   = shift;
    my $root = shift;

    my $date = localtime(time);

    open(WO, "$wo") or S4P::perish(10, "sleep_and_wake(): Failed to open work order file: $wo: $!");
    my $station = <WO>;
    chomp($station);
    close WO;
    my $file = "$root/$station/station.cfg";

    my $setting;
    if ( $opt_m eq "disable" ) {
        if ( S4P::check_station("$root/$station") ) {
#           system("touch $root/$station/DO.STOP.$$.wo");
            chdir("$root/$station");
            S4P::stop_station();
        }
#       subtract_station($station, $root);
        $setting = 1;
    } elsif ( $opt_m eq "enable" ) {
#       add_station($station, $root);
        $setting = 0;
    }

    system("chmod +w $file");
    open(FILE, ">>$file") or S4P::perish(10, "sleep_and_wake(): Failed to open work order file: $file: $!");
    print FILE "\n# The following update was made by Configurator on $date:\n\$cfg_disable = $setting;\n\$cfg_foobar = 1;\n";
    close FILE;
    system("chmod -w $file");

}

sub add_station {

    my $station = shift;
    my $root = shift;

    my $list = "$root/station.list";

    system("chmod +w $list");
    open(LIST, ">>$list") or S4P::perish(10, "add_station(): Failed to open station list file: $list: $!");
    print LIST "$station\n";
    close LIST;
    system("chmod -w $list");
}

sub subtract_station {

    my $station = shift;
    my $root = shift;

    my $list = "$root/station.list";
    my $tmplist = "$list.tmp";

    system("chmod +w $list");
    open(LIST, $list) or S4P::perish(10, "subtract_station(): Failed to open station list file: $list: $!");
    
    open(TMP, ">$tmplist") or S4P::perish(10, "subtract_station(): Failed to open station tmp list file: $tmplist: $!");

    while ( <LIST> ) {
        next if ( $_ =~ /$station/ );
        print TMP;
    }

    close LIST;
    close TMP;

    unlink($list);
    rename($tmplist, $list);
    system("chmod -w $list");
}

