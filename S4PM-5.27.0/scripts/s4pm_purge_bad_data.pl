#! /usr/bin/perl

=head1 NAME

s4pm_purge_bad_data.pl - failure handler to delete bad data that fails QC

=head1 SYNOPSIS

s4pm_purge_bad_data.pl
[B<-c> I<clean_data_dir>] 

=head1 DESCRIPTION

This is a Register Data failure handler to purge bad input data (e.g. 
maneuver-contaminated AM1ATTN0 or short L0 granules) from the system. 
These are detected by QC scripts such as s4pm_attitude_check.pl or 
Register Data's ragged granule trap.

Purging involves 3 steps:
(1) remove the input files and associated met file from disk
(2) remove the file_groups from the PDR and recycle
(3) deallocate the disk space in the Allocate Disk database

The last step is a little tricky because the file has not yet been renamed
to a name that the Sweep Station station script can recognize. Therefore, 
we create a work order with a "dummy" filename, e.g. 
AM1ATTN0.A1970001.0000.001.1970001000000 and drop it in the Sweep Data station. 
This is assumed to be B<../../sweep_data>, but can be overridden on the 
command line.

If the PDR work order is a mixed PDR (multiple data types), a fresh PDR
without the offending AM1ATTN0 is generated and copied up to the parent
directory for processing.

=head1 OPTIONS

=over 4

=item B<-c>

Override the Sweep Data station directory default (../../sweep_data).
For failure handlers, make sure you go up enough directories (usually ../..)
from the current failed one.

=back

=head1 EXAMPLES

 s4pm_purge_bad_data.pl -p 'DO.BAD_QA.*.wo' -c '../../sweep_data'

 s4pm_purge_bad_data.pl -p 'DO.FIX_TIME.*.wo' -c '../../sweep_data'

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_purge_bad_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_c $opt_p $opt_t);
use Getopt::Std;
use S4P::PDR;
use S4PM;
use File::Copy;
use File::Basename;
require 5.6.0;

getopts('c:p:');
my $clean_data_dir = $opt_c || '../../sweep_data';

my $pattern = $opt_p or 'DO.REGISTER.*';
my @work_order_files = glob($pattern);
my $nfiles = scalar(@work_order_files);
die "No work order files with pattern $pattern" unless ($nfiles > 0);
my $i = 0;
foreach my $infile(@work_order_files) {
    my $pdr = S4P::PDR::read_pdr($infile) or
        die "Cannot read/parse PDR from $infile";

    my $clean_data_wo = "DO.SWEEP.BAD_INPUT.$$.$i.wo";
    open SWEEP, ">$clean_data_wo" or 
        die "Cannot open output work order $clean_data_wo for SWEEP";

    # Loop through file groups, saving a list for deletion
    # And writing dummy files to the sweep_data work order
    my @delete_list;
    foreach my $file_group (@{$pdr->file_groups}) {
        # Add actual files to the list to be deleted
        my @sci_files = $file_group->science_files;
        push @delete_list, @sci_files;
        push @delete_list, $file_group->met_file;
        # Files may not be named properly for Sweep Data to recognize
        # So we just make up one with the right data type.
#       my $dummy = S4PM::make_s4pm_filename($file_group->data_type,
#           S4PM::infer_platform($file_group->data_type), '1970-01-01T00:00:00Z',
#           $file_group->data_version, '1970-01-01T00:00:00Z');
        my $dummy = S4PM::make_patterned_filename($file_group->data_type, $file_group->data_version, '1970-01-01T00:00:00Z', 0);
        my $dir = dirname($file_group->met_file);
        print SWEEP "$dir/$dummy\n";
    }
    close SWEEP;

    # Delete the files
    unlink @delete_list or die "Cannot unlink all files in $infile: $!";

    # Move the SWEEP work order to clean_data
    move($clean_data_wo, $clean_data_dir) or 
        die "Could not move $clean_data_wo to $clean_data_dir: $!";

    # Unlink input PDR file so we don't reprocess by mistake
    unlink $infile or die "Cannot unlink input file $infile: $!";
    $i++;
}
exit(0);
