#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_convert_all_old2new_selectdata.pl - s4pm_old2new_selectdatacfg.pl wrapper

=head1 SYNOPSIS

s4pm_convert_all_old2new_selectdata.pl
B<-r> I<algorithm_root_dir>
[B<-f>]

=head1 DESCRIPTION

This script is a front end to B<s4pm_old2new_selectdatacfg.pl> that allows
old style (pre-5.6.0) Select Data configuration files to be converted into
Stringmaker algorithm configuration files in a batch like mode.

The B<-r> argument specifies the algorithm root directory. When run, this
script will seek and convert all old style (pre-5.6.0) Select Data 
configuration files into the new Stringmaker configuration files for all
versions of all algorithms found under the root directory.

This script assumes that algorithm directories exist under the root directory
specified and that under each algorithm directory are one or more subdirectories
for each version. Further, this script assumes that the old style Specify 
Data configuration files are named specify_data_[algorithm_name].[profile].cfg.
For example: specify_data_MoPGE1.RPROC.cfg. The converted files are named:
[algorithm_name]_[profile].cfg, for example: MoPGE01_FPROC.cfg.

By default, if the script encounters a previously converted file, it will
prompt you whether to overwrite or not. To turn off this feature, use
the B<-f> option. This will force any preexisting files to be overwritten
without prompting.

=head1 AUTHOR

Stephen berrick, NASA GSFC, Code 610.2

=cut

################################################################################
# s4pm_convert_all_old2new_selectdata.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use File::Basename;
use Getopt::Std;
use vars qw($opt_r $opt_f);

getopts('r:f');

unless ( $opt_r ) {
    die "\nYou need to specify the root algorithm directory with the -r argument.\n\n";
}
my $root = $opt_r;

my @specify_data_files = ();

chdir $root or die "\nFailed to chdir to $root: $!\n\n";
my @pgedirs = glob('*');
foreach my $pgedir ( @pgedirs ) {
    next unless ( -d $pgedir );
#   print "Working on PGE: $pgedir\n";
    chdir $pgedir or die "\nFailed to chdir to $pgedir: $!\n\n";
    my @versions = glob('*');
    foreach my $ver ( @versions ) {
        next if ( $ver eq 'common' );
        next unless ( -d $ver );
        chdir $ver or die "\nFailed to chdir to $ver: $!\n\n";
        my @config_files = glob('specify_data*.cfg');
        foreach my $c ( @config_files ) {
#           print "File: [$root/$pgedir/$ver/$c]\n";
            push(@specify_data_files, "$root/$pgedir/$ver/$c");
        }
        chdir "..";
    }
    chdir "..";
}

foreach my $file ( @specify_data_files ) {

    my $outfile = basename($file);
    my $dir = dirname($file);
    $outfile =~ s/^specify_data_//;
    $outfile =~ s/\./_/;
    $outfile = "$dir/$outfile";
#   print "outfile: [$outfile]\n";
    print "Converting\n$file\ninto\n$outfile\n...";
    my $cmd = "s4pm_old2new_selectdatacfg.pl -i $file -o $outfile -c " . $ENV{'HOME'} . "/s4pm_cfg/s4pm_pge_esdt.cfg";
    if ( -e $outfile and ! $opt_f ) {
        print "$outfile already exists. Overwrite? (y/n) ";
        my $ans = <STDIN>;
        chomp($ans);
        if ( $ans eq "n" or $ans eq "N" ) {
            print "\nOk. Skipping.\n";
            next;
        }
    }
    system($cmd);
    print "done.\n\n";

}
