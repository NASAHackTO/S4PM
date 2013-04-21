#!/tools/gdaac/COTS/bin/perl

=head1 NAME

s4pm_s4pa_subscription.pl - convert S4PA subscription email into symlinks and output PDR

=head1 SYNOPSIS

s4pm_s4pa_subscription.pl 
[B<-f> I<config_file>]
[B<-t> I<target_dir>]

=head1 DESCRIPTION

s4pm_s4pa_subscription.pl converts an email of S4PA subscriptions into symlinks
and output a PDR for the downstream register_data station.

The target directory for the symlinks can be specified on the command line
using the B<-t> option, or in the configuration file as $cfg_target_dir
(see below).

=head1 FILES

=head2 Email File

The script is designed to be triggered by the arrival of the body of an S4PA 
email, which looks something like:

 New data and metadata files are now available online at the URLs:
 ftp://disc3.nascom.nasa.gov/data/s4pa/TRMM_ANCILLARY/GOES-10_ir2_4km/1998/240/GOES-10_ir2_4km_9808282028.xml
 ftp://disc3.nascom.nasa.gov/data/s4pa/TRMM_ANCILLARY/GOES-10_ir2_4km/1998/240/GOES-10_ir2_4km_9808282028.tar

=head2 Configuration File

The configuration file is a Perl segment with the following configuration
variables:

=over 4

=item $cfg_target_dir

The full pathname of the target directory for the symlinks that are created.

=item %cfg_datatype_pattern

This is a hash where the key is the data_type and the value is a regular
expression pattern.  This is applied to the metadata file path in order
to infer the data_type and optionally version_id.
For example, a Data Pool-type pattern would be:

'MOD08_D3' => '/(MOD08_D3)\.(\d\d\d)/'

The parentheses are used to identify the data_type and the version fields.
If parentheses are not used, the key will be used as the data_type when a
match is found and the version will default to 1.

If this hash does not exist, then the metadata file itself will be parsed
for ShortName and VersionID.

=back

=head2 PDR File

The output is a REGISTER file in PDR format.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_s4pa_subscription.pl,v 1.4 2007/11/02 14:47:12 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use Safe;
use S4P;
use S4P::PDR;
use S4P::MetFile;
use File::Basename;
use vars qw($opt_f $opt_t);
use vars qw($cfg_local_root $cfg_target_dir %cfg_datatype_pattern);

# Read config file
getopts('f:t:');
if ($opt_f) {
   my $cpt = new Safe 'CFG';
   $cpt->share(qw($cfg_local_root $cfg_target_dir %cfg_datatype_pattern));
   $cpt->rdo($opt_f) or S4P::perish(2, "Failed to rdo file $opt_f: $!");
}

my $target_dir = $opt_t || $cfg_target_dir;
S4P::perish(3, "Target directory must be specified on command line or in config file") 
    unless $target_dir;

# Read input work order
my $input_file = shift @ARGV;
open IN, $input_file or die "Cannot read from $input_file: $!";
my $metfile;
my @scifiles;
my @file_groups;
my ($data_type, $data_version);
my ($name, $pass, $uid, $gid, $quota, $com, $gcos, $ftpdir, $shell) = getpwnam("ftp");
my %symlinks;
while (<IN>) {
    chomp;
    next unless (s#^\s*ftp://##);
    my ($machine, $path) = split('\/+', $_, 2);
    my $filename = basename($path);

    # Determine source and target for file
    my $source = "$ftpdir/$path";
    S4P::perish(7, "Nonexistent or zero-size file $source") unless (-s $source);
    my $target = "$target_dir/$filename";

    # See if it is a metadata file (ends in .xml)
    my $is_met = ($filename =~ m/\.xml$/);
    if ($is_met) {
        # First flush the FILE_GROUP in progress
        push (@file_groups, mk_file_group($data_type, $data_version, 
            $metfile, @scifiles)) if ($metfile);

        $metfile = $target;
        ($data_type, $data_version) = infer_data_type($source, \%CFG::cfg_datatype_pattern);
        S4P::perish(4, "Cannot find datatype for met file $source") unless $data_type;
        S4P::logger("INFO", "Determined $filename to be data_type $data_type");
        @scifiles = ();
    }
    else {
        push @scifiles, $target;
    }
    $symlinks{$source} = $target;
}
foreach my $source(keys %symlinks) {
    my $target = $symlinks{$source};
    mk_symlink($source, $symlinks{$source});
}
# Flush last FILE_GROUP
push (@file_groups, mk_file_group($data_type, $data_version, $metfile, @scifiles)) if ($metfile);

# Finally, create PDR with accumulated FILE_GROUPS
my $output_wo = sprintf("REGISTER.%d_%d.wo", time(), $$);
my $pdr = new S4P::PDR;
$pdr->file_groups(\@file_groups);
$pdr->recount();
if ($pdr->write_pdr($output_wo) != 0){
    S4P::perish(100, "Failed to write output work order to $output_wo");
}
exit(0);


sub infer_data_type {
    my ($metfile, $rh_datatype_pattern) = @_;

    # Search for data_type by pattern
    my ($data_type, $data_version);
    my %datatype_pattern = %{$rh_datatype_pattern} if $rh_datatype_pattern;
    foreach my $dt(keys %datatype_pattern) {
        if ($metfile =~ m#$datatype_pattern{$dt}#) {
            ($data_type, $data_version) = ($1, $2);
            # If pattern did include parentheses, use key and
            # default version to 1
            $data_type ||= $dt;
            $data_version ||= 1;
            last;
        }
    }
    return ($data_type, $data_version) if $data_type;

    # OK, not found, either because it is not there or there is no pattern hash
    # Instead let's parse the metfile
    my %met = S4P::MetFile::get_from_met($metfile, 'SHORTNAME', 'VERSIONID');
    return($met{'SHORTNAME'}, $met{'VERSIONID'});
}

sub mk_file_group {
    my ($data_type, $data_version, $metfile, @scifiles) = @_;
    my $file_group = new S4P::FileGroup;

    # Create FILE_SPEC for metadata
    my $met_fs = new S4P::FileSpec;
    $met_fs->pathname($metfile);
    $met_fs->file_type('METADATA');
    my $size = (-s $metfile);
    $met_fs->file_size($size);
    my @file_specs = ($met_fs);

    # Create FILE_SPECS for metadata files
    foreach my $f(@scifiles) {
        my $fs = new S4P::FileSpec;
        $fs->pathname($f);
        $fs->file_type('SCIENCE');
        my $size = (-s $f);
        $fs->file_size($size);
        push @file_specs, $fs;
    }

    # Construct FILE_GROUP from FILE_SPECs
    $file_group->file_specs(\@file_specs);
    $file_group->data_type($data_type);
    $file_group->data_version($data_version);
    return $file_group;
}
sub mk_symlink {
    my ($source, $target) = @_;

    # Check to see if link is there
    if (-l $target) {
        my $check = readlink($target);
        # Link found:  see if it corresponds to the same source
        if ($check ne $source) {
            S4P::perish(20, "Link $target already exists but does not match $source ($check instead)");
        }
        else {
            S4P::logger("INFO", "Symlink $target already exists");
        }
        return 1;
    }

    # Create the symlink
    symlink($source, $target) or 
        S4P::perish(5, "Failed to create symlink from $source to $target: $!");
    S4P::logger("INFO", "Creating a symlink from $source to $target");
    return 1;
}
