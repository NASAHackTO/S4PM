#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_prepare_run_resync.pl - resync script for the Prepare Run station

=head1 SYNOPSIS

s4pm_prepare_run_resync.pl 

=head1 DESCRIPTION

The B<s4pm_prepare_run_resync.pl> takes care of resyncing the station with
the current station.cfg file. For now, this only means deleting 
Select Data configuration files whose algorithms are not being run by this
station. This resyncing is typically desired after an algorithm has been
uninstalled and Stringmaker has been run.                  

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_prepare_run_resync.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Safe;
use S4P;

# Read station.cfg file's %cfg_commands hash in Safe mode

my $compartment = new Safe 'CFG';
$compartment->share('%cfg_commands');
$compartment->rdo("../station.cfg") or S4P::perish(30, "main: Failed to import ../station.cfg file in safe mode: $!");

my %current_algorithms = ();
foreach my $wo ( keys %CFG::cfg_commands ) {
    $wo =~ s/^PREPARE_//;
    $wo = "../s4pm_select_data_" . $wo . ".cfg";
    $current_algorithms{$wo} = 1;
}

# Get current array of Select Data configuration files

my @config_files = glob("../s4pm_select_data_*.cfg");

# Now, remove any Select Data configuration files that aren't in 
# %current_algorithms hash. Also, remove the corresponding algorithm
# subdirectories.

foreach my $cfg ( @config_files ) {
    unless ( exists $current_algorithms{$cfg} ) {
        unlink($cfg) or S4P::perish(100, "main: Failed to unlink $cfg: $!");
        if ( $cfg =~ /^\.\.\/s4pm_select_data_(.*)\.cfg$/ ) {
            my $f = "../" . $1;
            system("/bin/rm -fr $f");
        }

    }
}

exit 0;
 


