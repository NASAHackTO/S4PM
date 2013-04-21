#!/usr/bin/perl

=head1 NAME

s4pm_clean_algorithm_output.pl - clean output of algorithm after a failure

=head1 SYNOPSIS

s4pm_clean_algorithm_output.pl 
I<ProcessControlFile>

=head1 DESCRIPTION

This is run in a failed run_pge directory to delete the files created by a
failed algorithm.  It is necessary because certain algorithms use the Fortran
"new", which fails if the file already exists.

Normally, it must be run from within the directory, a la cfg_failure_handler.
It derives the input work order (aka PCF) using S4P::check_job, then goes
through all the output files, deleting them if they exist.

However, it does have a manual override, i.e., specifying the Process Control
File on the command line.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_clean_algorithm_output.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use S4P::PCF;

my ($status, $pid, $owner, $pcf_name, $comment);
if ($ARGV[0]) {
    $pcf_name = $ARGV[0];
}
else {
    ($status, $pid, $owner, $pcf_name, $comment) = S4P::check_job();
    $pcf_name =~ s/\.(pcf|wo)$//;
}
my $pcf = S4P::PCF::read_pcf($pcf_name) or 
    S4P::perish(1, "Cannot read PCF $original_work_order");
my ($section, $file);
foreach $section('PRODUCT OUTPUT FILES', 'SUPPORT OUTPUT FILES',
  'INTERMEDIATE OUTPUT', 'TEMPORARY I/O') {
    foreach $file_type('fileref', 'metpath') {
        my %files = %{$pcf->product_files($section, $file_type, 0)};
        my @files = map {split ' '} values %files;
        foreach $file(@files) {
            if (-f $file) {
                unlink $file or S4P::perish(2, "Failed to unlink output $file: $!\n");
                S4P::logger('INFO', "Deleting output file $file\n");
            }
        }
    }
}
exit(0);
