#!/usr/bin/perl

=head1 NAME

s4pm_failed_qc_handler.pl - Run Algorithm station handler for jobs failing QC

=head1 SYNOPSIS

s4pm_failed_qc_handler.pl

=head1 DESCRIPTION

This script is a QC failure handler script for the Run Algorithm station. The 
script performs these steps:

=over 4

=item 1

Moves the SWEEP work order to the Sweep Data station.

=item 2

Moves the UPDATE work order to the Track Data station.

=item 3

Moves the EXPORT work order to the Export station and the EXPORT_PH work
order to the PDR_LIMBO pseudo-station. If all output products failed QC,
there will be no EXPORT work order.

=item 4

Moves the REGISTER work order and the RUN log file to the Register Local 
Data station. If all output products failed QC, there will be no REGISTER 
work order.

=back

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_failed_qc_handler.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

require 5.6.0;
use File::Copy;
use S4P;
use S4PM;
use strict;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw(
);

################################################################################

S4P::logger("DEBUG", "main: Starting s4pm_failed_qc_handler.pl.");

move_clean_data();

move_export();

move_trigger();

move_update();

sub move_clean_data {

    my @files = glob("SWEEP.*");

    if ( scalar(@files) > 1 ) {
        S4P::perish(30, "s4pm_failed_qc_handler: move_clean_data(): More than one file with pattern: SWEEP* found. There should only be one.");
    } elsif ( scalar(@files) == 0 ) {
        S4P::perish(31, "s4pm_failed_qc_handler: move_clean_data(): No file with pattern: SWEEP* found. There should be one.");
    }

    my $file = $files[0];
    rename("$file", "DO.$file") or
        S4P::perish(70, "s4pm_failed_qc_handler: move_clean_data(): Could not rename SWEEP work order to prepend 'DO': $!
");
    move("DO.$file", "../../sweep_data") or
        S4P::perish(71, "s4pm_failed_qc_handler: move_clean_data(): Moving of SWEEP work order file failed: $!");

    S4P::logger('INFO', "Moved SWEEP work order to sweep_data.");
    return;

}

sub move_export {

    my @exports = glob("EXPORT.*");
    if ( scalar(@exports) > 1 ) {
        S4P::perish(33, "s4pm_failed_qc_handler: move_export(): More than one file with pattern: EXPORT.* found. There should only be one.");
    } elsif ( scalar(@exports) == 0 ) {
        S4P::logger("WARNING", "s4pm_failed_qc_handler: move_export(): No file with pattern: EXPORT.* found. This can happen if zero output files passed QC.");
        return;
    }

    my @phs = glob("EXPORT_PH.*");
    if ( scalar(@phs) > 1 ) {
        S4P::perish(34, "s4pm_failed_qc_handler: move_export(): More than one file with pattern: EXPORT_PH.* found. There should only be one.");
    } elsif ( scalar(@phs) == 0 ) {
        S4P::logger("WARNING", "s4pm_failed_qc_handler: move_export(): No file with pattern: EXPORT_PH.* found. This can happen if zero output files passed QC.");
        return;
    }

    my $export = $exports[0];
    my $ph = $phs[0];

    rename("$export", "DO.$export") or
        S4P::perish(75, "s4pm_failed_qc_handler: move_export(): Could not rename EXPORT work order to prepend 'DO': $!
");
    move("DO.$export", "../../export") or
        S4P::perish(76, "s4pm_failed_qc_handler: move_export(): Moving of EXPORT work order file failed: $!");
    S4P::logger('INFO', "Moved EXPORT work order to export.");

    rename("$ph", "DO.$ph") or
        S4P::perish(77, "s4pm_failed_qc_handler: move_export(): Could not rename EXPORT_PH work order to prepend 'DO': $!
");
    move("DO.$ph", "../../PDR_LIMBO") or
        S4P::perish(78, "s4pm_failed_qc_handler: move_export(): Moving of EXPORT_PH work order file failed: $!");
    S4P::logger('INFO', "Moved EXPORT_PH work order to PDR_LIMBO.");

    return;

}

sub move_trigger {

    my @files = glob("REGISTER.*");

    if ( scalar(@files) > 1 ) {
        S4P::perish(36, "s4pm_failed_qc_handler: move_trigger(): More than one file with pattern: REGISTER* found. There should only be one.");
    } elsif ( scalar(@files) == 0 ) {
        S4P::logger("WARNING", "s4pm_failed_qc_handler: move_trigger(): No file with pattern: REGISTER.* found. This can happen if zero output files passed QC.");
        return;
    }

    my @logs = glob("RUN_*.log");

    if ( scalar(@files) > 1 ) {
        S4P::perish(37, "s4pm_failed_qc_handler: move_trigger(): More than one log file with pattern: RUN_*log found. There should only be one.");
    } elsif ( scalar(@files) == 0 ) {
        S4P::logger("WARNING", "s4pm_failed_qc_handler: move_trigger(): No log file with pattern: RUN_*.log found. This can happen if zero output files passed QC.");
        return;
    }

    my $file = $files[0];

    rename("$file", "DO.$file") or
        S4P::perish(79, "s4pm_failed_qc_handler: move_trigger(): Could not rename REGISTER work order to prepend 'DO': $!
");
    move("DO.$file", "../../register_local_data") or
        S4P::perish(79, "s4pm_failed_qc_handler: move_trigger(): Moving of REGISTER work order file failed: $!");
    S4P::logger('INFO', "Moved REGISTER work order to register_local_data.");

    my $log = $logs[0];
    my $newlog = $log;
    $newlog =~ s/\.([^l])/\1/;
    $newlog =~ s/RUN_/REGISTER\./;
    rename("$log", "$newlog") or
        S4P::perish(74, "s4pm_failed_qc_handler: move_trigger(): Could not rename REGISTER log file from $log to $newlog: $!
");
    move("$newlog", "../../register_local_data") or
        S4P::perish(79, "s4pm_failed_qc_handler: move_trigger(): Moving of REGISTER log file failed: $!");
    S4P::logger('INFO', "Moved REGISTER log file to register_local_data.");

    return;

}

sub move_update {

    my @files = glob("UPDATE.*");

    if ( scalar(@files) > 1 ) {
        S4P::perish(79, "s4pm_failed_qc_handler: move_update(): More than one file with pattern: UPDATE.* found. There should only be one.");
    } elsif ( scalar(@files) == 0 ) {
        S4P::perish(79, "s4pm_failed_qc_handler: move_update(): No work order file with pattern: UPDATE.log found. There should be one.");
    }

    my $file = $files[0];
    rename("$file", "DO.$file") or
        S4P::perish(79, "s4pm_failed_qc_handler: move_update(): Could not rename UPDATE work order to prepend 'DO': $!
");
    move("DO.$file", "../../track_data") or
        S4P::perish(79, "s4pm_failed_qc_handler: move_update(): Moving of UPDATE work order file failed: $!");
    S4P::logger('INFO', "Moved UPDATE work order to track_data.");

    return;

}

