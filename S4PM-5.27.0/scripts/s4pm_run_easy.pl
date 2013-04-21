#!/usr/bin/perl

=head1 NAME

s4pm_run_easy - easy (non-PCF) way to run algorithms within S4PM

=head1 SYNOPSIS

s4pm_run_easy.pl [B<-r> I<runtime_file>] [B<-l>] [B<-L>] [B<-v>]

=head1 DESCRIPTION

s4pm_run_easy provides a shell for PGEs to run in S4PM without
having to understand PCFs.  
Instead, s4pm_run_easy parses the PCF in the s4pm_run_algorithm
job and then generates a simpler "runtime file" for the algorithm
to use.  See "FILES" for the format of this.

It can handle both STATIC files (usually in the SUPPORT INPUT FILES
or sometimes PRODUCT INPUT FILES section of the PCF), and DYNAMIC
files (always in the PRODUCT INPUT FILES section).  
Additional runtime parameters for the algorithm are assumed to be
in one of the STATIC input files.

In order to map the S4PM input files to their proper role, it uses
a configuration file, which is referenced in the PCF with the special
LUN 411411.  See the FILES section of this man page for the format.

By default, s4pm_run_easy tars up everything left at the end of the run
as the output file.  Alternatively, if it finds a %cfg_output variable
in the configuration file, it will attempt to match filename patterns
to output files, and for each LUN, match it up with the output file.

=head1 ARGUMENTS

=over 4

=item B<-r> I<runtime_file>

Use runtime_file as the filename for the temporary runtime file.
Default is "runtime.txt".

=item B<-l>

Create local symlinks for the runtime file, using the name in the PCF.

=item B<-L>

Create local symlinks for the runtime file, using the 
LocalGranuleID in the metadata file.

=item B<-v>

Verbose mode.

=back

=head1 FILES

=head2 Runtime File

The first line of the runtime file is the number of lines following.
After that each line has 3 fields (for STATIC files) or 4 fields.

The first field is either STATIC or DYNAMIC.

The second field is an ESDT ShortName for DYNAMIC files.  
For STATIC files, it is a mnemonic assigned by the user/developer when
uploading/integrating the algorithm, e.g. RADIANCE_LUT.

The third field is the full pathname of the (data) file.

For DYNAMIC files, the fourth field is the full pathname of the metadata file.

=head2 Configuration File

The configuration file is a Perl code segment, with two (or three) hashes.

%cfg_static maps LUNs to Mnemonic values.

%cfg_dynamic maps LUNs to ESDTs.

$cfg_use_midtime_in_name causes the LocalGranuleID to be formed using the
midtime between start and stop time.  (This will result in the file being
renamed as such when staged for pickup, inserted in the data pool, 
or distributed via DCLI.)

=over 4 

=item TO DO


=back

For example,
  %cfg_static_input = (
    999990 => 'RUNTIME_PARAMETERS',
  );
  %cfg_dynamic_input = (
    700050 => 'MOD02SSH',
  );
  %cfg_output  = (
    700002 => 'MOD021KM.hdf'
  );

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_run_easy.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use Safe;
use Cwd;
use File::Basename;
use File::Copy;
use vars qw($opt_l $opt_L $opt_r $opt_v);
use S4PM;
use S4P::PCF;
use S4P::PCFEntry;
use S4P::MetFile;
use S4P::TimeTools;

getopts('lLr:v');

my @cmd = @ARGV or die "No algorithm specified for run";

# Read in PCF file
my $pcf_path = $ENV{'PGS_PC_INFO_FILE'} or 
    die "No PCF specified in environment PGS_PC_INFO_FILE";

my $pcf = S4P::PCF::read_pcf($pcf_path) or 
    die "Cannot read or parse PCF $pcf_path";
print STDERR "Read in PCF $pcf_path.\n";


# Get the hash representing the section for the product input files
my %support_files = %{$pcf->sections->{'SUPPORT INPUT FILES'}};
my %support_entries = %{$support_files{'entries'}};

my %product_files = %{$pcf->sections->{'PRODUCT INPUT FILES'}};
my $default_path = $product_files{'default_path'};
my %product_entries = %{$product_files{'entries'}};

# Make a pass through PCF:
#   (1) Look for config file (LUN=411411)
#   (2) Store paths for science and metadata files
#   (3) Store LUNs for later lookup of data_type in config file

my ($config, %file_lun, %file_met);
foreach my $r_entry(values %support_entries) {
    my $lun = $r_entry->lun;
    if ($lun == 411411) {
        $config = S4P::PCF::pcf_pathname($r_entry, 'fileref', $default_path);
    }
    else {
        my $path = S4P::PCF::pcf_pathname($r_entry, 'fileref', $default_path);
        $file_lun{$path} = $lun;
    }
}
foreach my $r_entry(values %product_entries) {
    my $lun = $r_entry->lun;
    my $met = S4P::PCF::pcf_pathname($r_entry, 'metpath', $default_path);
    my $science = S4P::PCF::pcf_pathname($r_entry, 'fileref', $default_path);
    $file_lun{$science} = $lun;
    $file_met{$science} = $met;
}
my ($start_time, $stop_time) = S4P::PCF::get_shell_parms(10258,10259);

# Read config file
my $cpt = new Safe 'CFG';
$cpt->share('%cfg_dynamic_input', '%cfg_static_input', '%cfg_output');
$cpt->rdo($config) or die "Cannot import config file $config: $!\n";
print STDERR "Read in configuration file $config.\n";

# Add inputs to runtime lists
my (@dynamic, @static, @parameters, @symlinks);
foreach my $file(sort keys %file_lun) {
    my $lun = $file_lun{$file};
    if (exists $CFG::cfg_dynamic_input{$lun}) {
        my $short_name = $CFG::cfg_dynamic_input{$lun};

        # Create local symlinks for dynamic files
        if ($opt_l || $opt_L) {
            my ($local_file, $local_met);
            if ($opt_L) {
                $local_file = local_granule_id($file_met{$file}) || basename($file);
                $local_met = ($file_met{$file} =~ /\.xml/) 
                               ? "$local_file.xml" : "$local_file.met";
            }
            else {
                $local_file = basename($file);
                $local_met = basename($file_met{$file});
            }
            symlink($file, $local_file) or 
                die "Cannot symlink $file to $local_file: $!";
            symlink($file_met{$file}, $local_met) or 
                die "Cannot symlink $file_met{$file} to $local_met: $!";
            push @dynamic, "DYNAMIC $short_name $local_file $local_met\n";
            push @symlinks, $local_file, $local_met;
            print STDERR "Created symlink to $local_file\n" if $opt_v;
        }
        else {
            push @dynamic, "DYNAMIC $short_name $file $file_met{$file}\n";
        }
    }
    elsif (exists $CFG::cfg_static_input{$lun}) {
        my $mnemonic = $CFG::cfg_static_input{$lun};
        if ($opt_l || $opt_L) {
            my $local_file = basename($file);
            symlink($file, $local_file) or 
                die "Cannot symlink $file to $local_file: $!";
            push @static, "STATIC $mnemonic $local_file\n";
            push @symlinks, $local_file;
            print STDERR "Created symlink to $local_file\n" if $opt_v;
        }
        else {
            push @static, "STATIC $mnemonic $file\n";
        }
    }
    else {
        warn "Cannot find input LUN $lun in config file $config";
    }
}
push (@parameters, "PARAMETER START_TIME $start_time\n") if $start_time;
push (@parameters, "PARAMETER STOP_TIME $stop_time\n") if $stop_time;

# Write out runtime list
my @runtime;
push (@runtime, @parameters, sort(@static), sort(@dynamic));
my $runtime_file = $opt_r || 'runtime.txt';
open RUNTIME, ">$runtime_file" or 
    die "Cannot write to runtime file $runtime_file: $!\n";
print RUNTIME (scalar(@runtime), "\n");
print STDERR  (scalar(@runtime), "\n") if $opt_v;
print RUNTIME join('', @runtime);
print STDERR  join('', @runtime) if $opt_v;
close RUNTIME;
print STDERR "Done writing runtime file $runtime_file.\n";

# Run the algorithm
push @cmd, $runtime_file;
print STDERR "Running command: ", join(' ', @cmd), "\n";
my ($rc, $errstr) = S4P::exec_system(@cmd);
if ($rc != 0) {
    print STDERR "$errstr received from ", join(' ', @cmd), "\n";
    exit(10);
}
print STDERR "Command succeeded.\n";

# Cleanup local symlinks first
foreach my $link(@symlinks) {
    unlink($link) or warn("Warning: failed to unlink $link: $!\n");
}
print STDERR "Done with local input symlinks\n" if ($opt_l && $opt_v);

# Get output file:  should be only one unless cfg_output is specified
my %output_files = %{$pcf->product_output_files()};
my %cleanup;
my @files = glob('*');
if (%CFG::cfg_output) {
    foreach my $lun(keys %CFG::cfg_output) {
        my $current_file = $CFG::cfg_output{$lun};
        if (! -f $current_file) {
            die "Cannot find output file $current_file for LUN $lun";
        }
        if (! exists $output_files{$lun}) {
            die "\%cfg_output refers to LUN that is not in PCF: $lun";
        }
        my $output_files = $output_files{$lun};
        # $output_files is whitespace separated list of output files for
        # this LUN--except there should be only one.
        my ($output_file, $extra) = split(/\s+/, $output_files);
        die "More than one output file version in PCF for LUN $lun" if ($extra);

        # Move file to S4PM-approved location and delete from cleanup list
        move($current_file, $output_file) or 
            die "Cannot move $current_file to $output_file: $!";
        delete $cleanup{$current_file};
        print STDERR "LUN $lun: moved $current_file to $output_file\n";

        # Try to keep the same extension in the Local Granule ID
        my ($extension) = ($current_file =~ m#\.(\w+)#);
        write_metadata($output_file, $extension, $start_time, $stop_time,
            $CFG::cfg_use_midtime_in_name);
    }
}
# Create a tar file of the files left in the directory
else {
    my @output_luns = values %output_files;
    my $nluns = scalar(@output_luns);
    die "Wrong number of product output file LUNs ($nluns != 1) in $pcf_path" 
        if ($nluns != 1);
    my @output_files = split(/\s+/, $output_luns[0]);
    my $nfiles = scalar(@output_files);
    die "Wrong number of product output file versions ($nfiles != 1) in $pcf_path" 
        if ($nfiles != 1);
    my $output_file = $output_files[0];
    print STDERR "Output file will be $output_file.\n";

    # Create a tar file of the output
    # Use a temporary directory so files are unique when untarred.
    my $tmpdir = basename($output_file);
    $tmpdir =~ s/\.hdf$//i;
    mkdir($tmpdir) or die "Cannot mkdir $tmpdir for final tar file: $!";
    print STDERR "Created temporary directory $tmpdir for tarring.\n";

    # Create symlinks for tarring
    my @links;
    foreach my $f(@files) {
        my $link = "$tmpdir/$f";
        symlink("../$f", $link) or die "Cannot make symlink from $f to $link: $!";
        print STDERR "Created symlink $link for tarfile\n" if $opt_v;
        push @links, $link;
    }
    S4PM::tar_links($output_file, $tmpdir) or 
        die "Failed to tar temporary directory $tmpdir";
    print STDERR "Completed tar file $output_file\n";

    # Cleanup temporary directory

    unlink(@links) or die "Failed to unlink symlink: $!";
    rmdir($tmpdir) or die "Failed to rmdir $tmpdir: $!";
    print STDERR "Completed cleanup of temporary directory.\n";

    # Make metadata file

    my $metfile = write_metadata($output_file, 'tar', $start_time, $stop_time);
    print STDERR "Wrote metadata file $metfile.\n";
    %cleanup = map{($_,1)} @files;
    delete $cleanup{$output_file};
    # N.B.:  Output file will look like HDF, following S4PM conventions
    # but have LGID of .tar
}
# Cleanup files
delete $cleanup{$pcf_path};
my $logfile = basename($pcf_path) . ".log";
$logfile =~ s/^DO.//;
delete $cleanup{$logfile};
foreach my $f(keys %cleanup) {
    print STDERR "Cleaning up $f\n" if ($opt_v);
    unlink($f) or die "Failed to cleanup file $f: $!";
}
if ( -e $runtime_file ) {
    unlink($runtime_file) or die "Cannot delete runtime file $runtime_file: $!";
}

print STDERR "Done.\n";

exit(0);

sub local_granule_id {
    my $metfile = shift;
    die "Cannot find metadata file $metfile" unless (-f $metfile);
    my %found = S4P::MetFile::get_from_met($metfile,'LOCALGRANULEID');
    return $found{'LOCALGRANULEID'};
}
sub write_metadata {
    my ($output_file, $extension, $start_datetime, $stop_datetime, 
        $use_midtime_in_lgid) = @_;
    my $output_base = basename($output_file);

    # Parse filename for basic metadata
    my ($shortname, $version, $begin, $prod_ccsds, undef)
          = S4PM::parse_patterned_filename($output_base);
#   my $prod_ccsds = S4P::TimeTools::yyyydddhhmmss2CCSDSa($prod_time);

    my $filesize = (-s $output_file) / 1000000;
    my ($begin_date, $begin_time, $end_date, $end_time, $dummy);
    my $begin_datetime = $start_datetime 
        || S4P::TimeTools::yyyydddhhmmss2CCSDSa($begin);
    ($begin_date, $begin_time, $dummy) = split(/[TZ]/, $begin_datetime);

    my $end_datetime = $stop_datetime || $begin_datetime;
    ($end_date, $end_time, $dummy) = split(/[TZ]/, $end_datetime);
    
    # Form LocalGranuleID
    my $lgid;
    if ($use_midtime_in_lgid) {
        my $diff = S4P::TimeTools::CCSDSa_Diff($begin_datetime, $end_datetime);
        my $midtime = S4P::TimeTools::CCSDSa_DateAdd($begin_datetime, $diff/2.);
#       $lgid = S4PM::make_s4pm_filename($shortname, $platform, $midtime, 
#           $version, $prod_ccsds, ($extension || 'dat'));
        $lgid = S4PM::make_patterned_filename($shortname, $version, $midtime, 0);
    }
    else {
        $lgid = $output_base;
    }
    $lgid =~ s/hdf$/$extension/i if (defined $extension);

    # Clean extraneous characters from prod. time for .xml file
    $prod_ccsds =~ s/T/ /;
    $prod_ccsds =~ s/Z//;
    # Use begin date/time for both Begin Date and End Date
    # Should switch this to SingleDateTime
    my $xml =<< "EOF";
<?xml version="1.0" encoding="UTF-8"?>
<GranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="NonEcsGranuleMetadata.xsd">
    <SchemaVersion>1.0</SchemaVersion>
    <DataCenterId>GSF</DataCenterId>
            <GranuleMetaData>
                <CollectionMetaData>
                    <ShortName>$shortname</ShortName>
                    <VersionID>$version</VersionID>
                </CollectionMetaData>
                <DataFilesContainer>
                  <File type="Science">$output_base</File>
                </DataFilesContainer>
                <DataGranule>
                    <SizeMB>$filesize</SizeMB>
                    <LocalGranuleID>$lgid</LocalGranuleID>
                    <ProductionDateTime>$prod_ccsds</ProductionDateTime>
                </DataGranule>
                <RangeDateTime>
                    <RangeEndingTime>$end_time</RangeEndingTime>
                    <RangeEndingDate>$end_date</RangeEndingDate>
                    <RangeBeginningTime>$begin_time</RangeBeginningTime>
                    <RangeBeginningDate>$begin_date</RangeBeginningDate>
                </RangeDateTime>
            </GranuleMetaData>
</GranuleMetaDataFile>
EOF
    my $met_file = "$output_file.xml";
    open MET, ">$met_file" or die "Cannot write to metadata file $met_file";
    print MET $xml;
    close MET;
    return $met_file;
}
