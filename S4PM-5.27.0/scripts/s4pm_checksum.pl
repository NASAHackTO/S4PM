#!/usr/bin/perl

=head1 NAME

s4pm_checksum.pl - compute checksums for S4P-generated data

=head1 SYNOPSIS

s4pm_checksum.pl
[B<-p> I<percentage>]
[B<-d> I<directory>]
metadata_file
file

=head1 DESCRIPTION

B<s4pm_checksum.pl> computes a CRC checksum using the cksum utility.
By default, it writes the checksum to ../checksums/cksum.YYYYDDD.txt
where YYYYDDD is today's date.  This closes off the file to allow
loading into the ECS database. The default directory can be overridden.

Note that both metadata and data files are needed, the former to obtain the
LocalGranuleID.

The percentage of files checksummed can be specified either on the command
line, with the B<-p> option, or more commonly, in the environment
variable S4P_CHECKSUM_PERCENT.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_checksum.pl,v 1.3 2008/09/19 17:29:07 clynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use File::Basename;
use S4P::MetFile;
use S4P;
use vars qw($opt_p $opt_d $opt_v);

getopts('d:p:v');

# Check to make sure we have everything
my $met_file = $ARGV[0];
my $data_file = $ARGV[1];
usage() unless $data_file;
S4P::perish(2, "Metadata file $met_file does not exist") unless (-e $met_file);
S4P::perish(3, "Data file $data_file does not exist") unless (-e $data_file);

# See if we are sampling
my $percentage = $opt_p || $ENV{'S4P_CHECKSUM_PERCENT'};
if (defined $percentage) {
    S4P::perish (4, "Percentage must be between 0 and 100") 
        unless ($percentage >= 0 && $percentage <= 100);
    if (rand(100) > $percentage) {
        printf STDERR "Skipping $ARGV[1] in random sample\n" if $opt_v;
        exit(0);
    }
}

# Checksum directory
my $directory = $opt_d || "../checksums";
S4P::perish(5, "Checksum directory $directory does not exist\n") unless (-d $directory);

# Filename:  $dir/cksum.YYYYDDD.txt
my @time = localtime();
my $checksum_file = sprintf("%s/cksum.%04d%03d.txt", $directory, 
    $time[5]+1900, $time[7]+1);
print STDERR "Writing checksum to $checksum_file\n" if ($opt_v);

# Get LocalGranuleID from metfile
my %attrs = S4P::MetFile::get_from_met($met_file, 'LOCALGRANULEID');
my $local_granule_id = $attrs{'LOCALGRANULEID'} 
    or S4P::perish(5,"Cannot find LOCALGRANULEID in $met_file\n");
print STDERR "LocalGranuleID = $local_granule_id\n" if $opt_v;

# Compute checksum
my $cksum_txt = `cksum $data_file`;
print STDERR $cksum_txt if $opt_v;
if ($?) {
    print STDERR "PATH=$ENV{'PATH'}\n";
    S4P::perish(6, "Checksum failed on $data_file");
}
my @cksum = split(/\s+/, $cksum_txt);

# Write output
open OUT, ">>$checksum_file" or S4P::perish(7, "Cannot append to $checksum_file\n");
my $filename = basename($data_file);
print OUT $local_granule_id, "\t", $filename, "\t", $cksum[0], "\n";
close OUT;
exit(0);

sub usage {
    S4P::perish(1, "Usage: $0 [-v] [-p percentage] [-d directory] metfile data_file");
}
