#!/usr/bin/perl 

=head1 NAME

s4pm_resubmit_pdr.pl - script to fixed failed PDRs

=head1 SYNOPSIS

s4pm_resubmit_pdr.pl 
B<[-c]> 

=head1 DESCRIPTION

B<s4pm_resubmit_pdr.pl> will (1) move FIXPDR to the Export station for export 
of PDR to ECS polling directory and (2) if one exists, move the UPDATE 
work order to the Track Data station for proper cleanup of successfully 
ingested files found in LONGPAN returned from ECS.

=head1 ARGUMENTS

=item B<-c> I<s4pm_resubmit_pdr.cfg>

Specifies the configuration file. This file contains the full path 
specification for the Export and Track Data station directories.

=head1 AUTHOR

Bruce Vollmer, NASA/GSFC, Code 610.2, Greenbelt, MD 20771 

=cut

################################################################################
# s4pm_resubmit_pdr.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_c
            $filename
            $jobname
            $jobid
            $failtype
            $wo
);
use S4P;
use S4PM;
use Getopt::Std;
use Safe;

# Get resubmit_PDR.cfg token from station.cfg

  getopts('c:');

## Read the Resubmit_PDR configuration file to get directory specification
## for Track Data station (UPDATE workorder) and for Export station (FIXPDR)

my $compartment = new Safe 'CFG';
$compartment->share('$cfg_export_directory', '$cfg_grancentral_directory');
$compartment->rdo($opt_c) or
    S4P::perish(30, "main: Failed to read in configuration file $opt_c in safe mode: $!");

# Glob for the fixed PDR (FIXPDR)

my @files = <FIXPDR*>;

my $num_files = @files;

S4P::logger("INFO", "Executing ResubmitPDR: number of FIXPDRs is $num_files\n");

if ($num_files == 1) {

    $filename = $files[0];

### Parse the input workorder filename

    ($jobname, $jobid, $failtype,  $wo) = split(/\./, $filename, 4);

### Give FIXPDR full path for move to export directory 

    my $final_destination = "$CFG::cfg_export_directory/" .
        join(".","DO","EXPORT","$jobid","$failtype","$wo");

### Move FIXPDR to export directory

    my $realpath = readlink($filename) or 
        S4P::perish(1, "Cannot readlink for $filename: $!");
    my $rc = S4P::move_file($realpath, $final_destination);

    unless ($rc) {
        S4P::perish(1, "main: move of $realpath to export directory failed;
                    export directory is $CFG::cfg_export_directory");
    }

    S4P::logger("INFO", "ResubmitPDR Success: moved PDR $filename to $CFG::cfg_export_directory \n");

} else {

    S4P::perish(1,"resubmit_PDR failed, number of PDRs is $num_files \n");

}

# If LONGPAN is processed, an UPDATE workorder may exist for science files
# that were successfully archived; if UPDATE exists, process to Track Data 

my @update_files = <UPDATE*>;

my $num_update_files = @update_files;

S4P::logger("INFO","Number of UPDATE workorders to process in resubmit_PDR is $num_update_files \n");

if ($num_update_files == 1 ) {

    my $update_filename = $update_files[0];

### Create full path name for UPDATE workorder for move to Track Data station

    my $final_dest = "$CFG::cfg_grancentral_directory/" . "DO." . "$update_filename";

### Move the UPDATE workorder to Track Data

    my $rcode = S4P::move_file($update_filename,$final_dest);    

    unless($rcode) {

        S4P::perish(1,"main:move of $update_filename to Track Data failed;
                    Track Data directory is $CFG::cfg_grancentral_directory \n");

    }

} 

exit 0;
