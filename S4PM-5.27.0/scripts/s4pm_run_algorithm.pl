#!/usr/bin/perl

=head1 NAME

s4pm_run_algorithm - run a science algorithm within S4PM

=head1 SYNOPSIS

s4pm_run_algorithm.pl 
[B<-c>]
[B<-f> I<config_file>] 
[B<-q> I<config_file>]
[B<-s>]
[B<-l> I<logdir>]
[B<-m> I<msgdir>] 
[B<-e> I<expiration_time>] 
[B<-p> I<pgedir>] 
[B<-t> I<toolkit_home>] 
[B<-v> I<PGEversion>] 
[B<-F> I<FAILPGE_met_template>] 
[B<-o> I<originating_system>]
[B<-a> I<node_name>]
[B<-O>]
[B<-A>]
[B<-R>]
executable process_control_file

=head1 DESCRIPTION

This runs a Product Generation Executable within S4P.
The penultimate argument is the full path to the executable, and the last 
argument is the full path the PCF (process control file).
In S4P, the PCF is the input work order.

On completion, s4pm_run_algorithm.pl also gathers up the production history 
information, and outputs a number of work orders:

=over 4

=item EXPORT

Product Delivery Record file with output data.

=item UPDATE

UPDATE work order for Track Data, decrementing uses for input data.

=item REGISTER

Product Delivery Record file with output data for Register Data. This is output
unless the B<-q> option is specified to send data to the qc station.

=back

=head1 OPTIONS

=over 4

=item B<-A>

A temporary flag, indicates that s4pm_run_algorithm.pl should use Aqua 
behaviour for decrementing uses, specifically -1 for all input granules.
If not set, it uses Terra behaviour, where input L0 is decremented according
to the number of MOD01 granules created.

When this has been thoroughly tested, the -A option will be removed and all
input granules will be decremented by 1.

=item B<-a> I<node_name>

By default, the node name added to the output PDRs going to the Export station
are taken from the machine name using S4P::PDR::gethost(). To explictly set
the node name to something else, include this option.

=item B<-c>

Add checksums to the EXPORT work order.

=item B<-f> I<config_file>

Configuration file.  This is really necessary only if you have a browse
map that needs to be represented, in which case the map should be in
%cfg_data_browse_map, with data lun as key and browse lun
as value.  There can be only one browse file for a given data file

In for a penny, in for a pound, so all of the other arguments listed
below can also be specified in the configuration file.  The variable name is
simply $cfg_argument, where argument is the argument listed below.
However, anything specified on the command line will override anything in
the configuration file.

=item B<-q> I<config_file>

Apply Quality Control scripts found in config_file.

=item B<-e> I<expiration_time>

Expiration time for output PDR work orders.  This should be specified as
<n><units>, e.g. 2days.  Valid units are seconds, minutes, hours, 
days, weeks, months and years.

The default is 3days.

=item B<-o> I<originating_system>

Originating system to use in output PDR work orders.
The default is 'S4PM'.

=item B<-s>

Symlinks:  creates local symbolic links to input and output directories
to aid in debugging.

=item B<-m> I<msgdir>

Message directory for PGE.  The default is '../MSGS'.

=item B<-p> I<pge_dir>

Directory for algorithm executables.  Default is '../'.
This is needed for nested executables that expect to be in the same directory.

=item B<-t> I<toolkit_dir>

Directory where Toolkit lives, used for directories starting with '~'.

=item B<-l> I<logdir>

Directory to save runlogs (output by rusage program).

=item B<-v> I<pge_version>

Version of the algorithm.  If this is not specified, it will attempt to obtain 
a version from the B<-p> argument.  This is used only for metadata for the 
FAILPGE, you can get by without it so long as the algorithm doesn't fail 8-).

=item B<-F> I<failpge_met_template>

Metadata template file for FAILPGE data type.
Default is ../FAILPGE_met_template.cfg.

=item B<-o> I<originating_system>

Specify alternate originating system for output PDR work orders.

=item B<-O>

Run algorithm in on-demand setting.
This modifies the name of the output EXPORT work order to not include
the algorithm.

=item B<-R>

Re-read PCF after execution. 
This is useful if the PGE tweaks the PCF to put in the "real"
output filenames.

=item B<-H>

Data Handles are in effect.  This means that the information about the data
and metadata file locations is stored in the .ur file.
Because RegisterData moves the files in this scenario, no EXPORT is done from
Run Algorithm, which does not "know" where the files will be when EXPORT gets
around to them.
Instead, we add a STATUS=EXPORT or STATUS=EXPORT_ONLY to the FILE_GROUP in
the REGISTER_DATA PDRs.  
EXPORT status tells Register Data to export and register that granule.  
EXPORT_ONLY says to export but do not register it (in order to support the
various QC combinations supported by s4pm_run_algorithm.pl).

=back

=head1 SEE ALSO

QC(3)

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=cut

################################################################################
# s4pm_run_algorithm.pl,v 1.18 2008/04/15 18:18:13 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4P::PCF;
use S4P::TimeTools;
use S4P::MetFile;
use S4PM;
use S4PM::QC;
use POSIX;
use File::Basename;
use File::Copy;
use Getopt::Std;
use Safe;
require 5.6.0;
use vars qw($opt_a $opt_c $opt_e $opt_f $opt_l $opt_m $opt_o $opt_O  $opt_H
            $opt_p $opt_P $opt_q $opt_R $opt_s $opt_t $opt_v $opt_F );
use vars qw($originating_system $expiration);

# Parse command line

getopts('a:Ace:f:F:Hl:m:o:Op:Pq:Rst:v:');
my %data_browse_map;

my $compartment;
if ($opt_f) {
    $compartment = new Safe 'CFG';
    $compartment->share('%cfg_data_browse_map', 
                        '$cfg_msgdir', 
                        '$cfg_pgedir', 
                        '$cfg_logdir', 
                        '$cfg_fail_pge_met_template', 
                        '$cfg_pge_version', 
                        '$cfg_originating_system', 
                        '$cfg_expiration',
                        '%cfg_production_summary',
    );
    $compartment->rdo($opt_f) or
        S4P::perish(30, "main: Failed to read in configuration file $opt_f in safe mode: $!");
    %data_browse_map = %CFG::cfg_data_browse_map;
}

my $msgdir = $opt_m || $CFG::cfg_msgdir || '../MSGS';
my $pgedir = $opt_p || $CFG::cfg_pgedir || '..';
my $logdir = $opt_l || $CFG::cfg_logdir;
my $pge_version = $opt_v || $CFG::cfg_pge_version || basename($opt_p);
my $failpge_met_template = $opt_F || $CFG::cfg_fail_pge_met_template ||
    '../FAILPGE_met_template.cfg';
$originating_system = $opt_o || $CFG::cfg_originating_system || "S4PM";
$expiration = $opt_e || $CFG::cfg_expiration || "3days";

# Setup environment

S4P::logger("INFO", 
    "main: Setting environment: PATH='$pgedir', PGS_PC_INFO_FILE='$ARGV[1]', PGSMSG='$msgdir'..., PGSHOME='$opt_t'");
$ENV{'PATH'} = join(':', $pgedir, $ENV{'PATH'});
$ENV{'PGS_PC_INFO_FILE'} = $ARGV[1];
$ENV{'PGSMSG'} = $msgdir;
$ENV{'PGSHOME'} = $opt_t if ($opt_t);

# Read PCF

my $pcf_name = $ARGV[1];
my $pcf = S4P::PCF::read_pcf($pcf_name) or
              S4P::perish(1, "main: Cannot read/parse PCF $pcf_name: $!");
my ($prefix, $job_type, $job_id, $rest) = split('\.', $pcf_name);
$job_type =~ s/^RUN_//;

# Create symbolic links to input and output directories

S4P::logger("INFO", "main: Setting up local subdirectories and symlinks...");
my (@files, @product_input_files);
my (@support_output_files, @support_input_files);
my (@intermediate_output_files, @intermediate_input_files);

my ($ra_product_output_files, $rh_browse_data_map, $rh_output_metfiles) =
    get_output_files($pcf, \%data_browse_map);
my @product_output_files = @{$ra_product_output_files};
my @product_browse_files = keys %{$rh_browse_data_map};

map {push(@product_input_files, split)} values %{$pcf->product_input_files};
map {push(@support_output_files, split)} values %{$pcf->support_output_files};
map {push(@support_input_files, split)} values %{$pcf->support_input_files};
map {push(@intermediate_output_files, split)} values %{$pcf->intermediate_output_files};
map {push(@intermediate_input_files, split)} values %{$pcf->intermediate_input_files};

# Go through all the referenced files and get their directories

push @files, @product_browse_files, @product_input_files,
             @support_output_files, @support_input_files,
             @intermediate_output_files, @intermediate_input_files;
foreach my $f(@product_output_files) {
    # Handle multi-file (ref $f) and single-file granules
    my @out = (ref $f) ? @$f : ($f);
    push(@files, @out);
}
my @dirs = unique_dirs(@files);

# For multi-file granules, create directories on-the-fly

foreach my $ofile ( @product_output_files ) {
    next unless (ref $ofile);
    my $dir = dirname($ofile->[0]);
    unless ( -d $dir ) {
        mkdir($dir, 0775) or 
            S4P::perish(2, "main: Cannot make local dir $dir: $!");
    }
}

# Make local directories or symlinks to non-local directories
my @local_dirs = make_local_dirs(@dirs, $opt_s);

my $runlog_file = "$pcf_name.Log";;
$runlog_file =~ s/^DO\.//;
$runlog_file =~ s/^RUN_//;

# Start up production log file...

my $cmd = $ARGV[0];
open RUNLOG, ">$runlog_file" or 
    S4P::perish(113, "main: Cannot open runlog file $runlog_file for writing: $!");
printf RUNLOG << "EOF";
# DPR_ID: $job_type.$job_id
# Resource collection for command:
# $cmd
# Resource Usage Information
EOF
close RUNLOG;
open STDOUT, ">>$runlog_file" or
    S4P::perish(113, "main: Cannot redirect STDOUT to runlog file $runlog_file for appending: $!");

# And we're off and running...

S4P::logger("INFO", "main: Starting up command $cmd...");
my $err_number = S4PM::rusage($cmd);

# Reread PCF in cases where output filenames were tweaked by algorithm
if ($opt_R) {
    S4P::logger('INFO',"Re-reading PCF $pcf_name for new output filenames");
    $pcf = S4P::PCF::read_pcf($pcf_name) or
              S4P::perish(1, "main: Cannot read/parse PCF $pcf_name: $!");
    $rh_output_metfiles = ();
    ($ra_product_output_files, $rh_browse_data_map, $rh_output_metfiles) =
        get_output_files($pcf, \%data_browse_map);
    @product_output_files = @{$ra_product_output_files};
    @product_browse_files = keys %{$rh_browse_data_map};
}

# Move STDOUT back to the real STDOUT so we can close out the RUNLOG file
open STDOUT, ">-" or S4P::perish(113, "main: Cannot redirect STDOUT back to stdout: $!");
# HACK for DPREP exit code 216
if ($err_number == 216) {
    S4P::logger("WARN", "main: Exit 216 from $cmd, setting to 0...");
    $err_number = 0;
}

# If successful, first do the production history
# Then write the output work orders
# Finally clean the log files and temporary stuff
#=====================================================================

my $node_name = $opt_a || S4P::PDR::gethost();

# Generate output UPDATE work order
# N.B.:  Generated for both successes and failures.  In the latter case, it is
# picked up by the "punt" failure handler, as we will no longer be needing the
# input file for this purpose.
# Also, note we are just looking for MODIS/AIRS product input files, not all the
# ancillary files, dynamic or otherwise

# All S4PM-tracked input files have absolute paths
my @input_files = grep(/^\//, @product_input_files);
S4P::perish(130, "main: Cannot find any product input files for UPDATE work order in $ARGV[0]") if (! @input_files);
update_work_order($job_type. '_' . $job_id, \@input_files);

# Finish runlog file no matter what exit code
finish_runlog($runlog_file, $pcf_name, [@product_output_files, @product_browse_files],
    \@support_output_files, \@product_input_files, \@support_input_files);
save_runlog($logdir, $runlog_file) if ($logdir);

#Created Production summary file
if ( %CFG::cfg_production_summary ) {
  unless ( S4PM::QC::production_summary(
					$pcf, 
					\%CFG::cfg_production_summary, 
					$CFG::cfg_originating_system, 
					$runlog_file 
				       )  
	 ) {
      S4P::perish(1, "main: Failed to create production summary file: $!");
    }
}

if ($err_number == 0) {

    # Remove zero-length files (e.g. MOD07_QC) from @product_output_files
    prune_empty_files(\@product_output_files, $job_type . '_' . $job_id);

    if (scalar(@product_output_files) == 0) {
        S4P::logger('WARN', "main: No product output files left after pruning empty ones");
        log2stderr($runlog_file);
    }
    else {
        # Make PDRs for output work orders
        S4P::logger("INFO", "main: Making output work orders...");
        my $catch_pdr = make_pdr('S4P', $node_name, $rh_output_metfiles, 
            @product_output_files);
        my $export_pdr = make_pdr('ECS', $node_name, $rh_output_metfiles, 
            @product_output_files);

        $export_pdr->checksum if ($opt_c);

        # Apply Quality Control scripts
        my ($fatal, $ra_qc_clean, $ra_block_export);
        if ($opt_q) {
            ($fatal, $ra_qc_clean, $ra_block_export) = 
                S4PM::QC::apply_qc($opt_q, $export_pdr, $catch_pdr);
            # Write out SWEEP work order if any are files returned by 
            # function
            clean_work_order($job_id, 'QC_BLOCK', @$ra_qc_clean) if (@$ra_qc_clean);
        }
    
        my ($ph_file, $linkage_file);
        if ($opt_P && $export_pdr->total_file_count) {
            # Make the production history
            ($ph_file, $linkage_file) = production_history($pcf_name, $pcf, 0, 
                $export_pdr, $rh_output_metfiles, $runlog_file);
            S4P::logger("INFO", "main: algorithm successful, production history in $ph_file...");
        }
        else {
            # No production history:  instead cat runlog to STDERR so it ends up in chain log
            log2stderr($runlog_file);
        }

        # Write output PDR work orders
        # Concatenate input job_type with job_id to make output job_id more unique
        # Do a PDR recount() just for good measure...
        $catch_pdr->recount();
        my $new_job_id = ($opt_O) ? $job_id : ($job_type . '_' . $job_id);
        $export_pdr->recount();
        if ($linkage_file) {
            pdr_work_order('ECS', 'EXPORT_PH', $job_type . 'PH_' . $job_id, 
                $node_name, $rh_output_metfiles, $ph_file, $linkage_file);
        }
        elsif ($opt_P) {
            S4P::logger('WARN', "main: No linkage file, so PH will not be exported.");
        }
        export_browse($rh_browse_data_map, $rh_output_metfiles, 
            $ra_block_export, $job_type , 'BR_' . $job_id, $node_name, 
            $rh_output_metfiles, $export_pdr);
        $export_pdr->recount();

        # Multi-file granules where each file is a different version in the LUN:
        # move the metadata file up a level to be even with the directory
        map {elevate_metfiles($_)} ($catch_pdr, $export_pdr);

        # Multi-file granules where individual versions are directories 
        # containing multiple files: expand FILE_SPECs to include contents
        # Expand for EXPORT work order, but not for REGISTER (which would
        # cause problems at the find_data step)
        filespec_dir2files($export_pdr);

        merge_pdrs($catch_pdr, $export_pdr) if ($opt_H);

        # Output Export PDR work order
        if ($export_pdr->total_file_count) {
            pdr_work_order('ECS', 'EXPORT', $new_job_id, $node_name, $rh_output_metfiles, $export_pdr);
        }
        elsif (! $opt_H) {
            S4P::logger('INFO', "No files left for EXPORT PDR after pruning and QC, skipping PDR generation.");
        }

        # Output Register Data PDR work order
        if ($catch_pdr->total_file_count) {
            pdr_work_order('S4P', 'REGISTER', $new_job_id,
                $node_name, $rh_output_metfiles, $catch_pdr);
        }
        else {
            S4P::logger('INFO', "No files left for REGISTER PDR after pruning and QC, skipping PDR generation.");
        }

        # Quit if a fatal error condition was encountered in QC
        S4P::perish(121, "main: Fatal QC error condition encountered.") if $fatal;
    }
    # Cleanup

    S4P::logger("INFO", "main: Cleaning log files and temporary dirs/symlinks...");

    # Collect scratch files, including GetAttr.temp and MCFWrite.temp
    my (@scratch_files);
    my $mcf_temp = $pcf->product_input_files->{'10254.1'};
    my $getattr_temp = $pcf->product_input_files->{'10252.1'};
    if ($mcf_temp) {
        my @mcf_temp = glob($mcf_temp . '_*');
        push (@mcf_temp, $mcf_temp) if (-f $mcf_temp);
        push (@scratch_files, @mcf_temp) if @mcf_temp;
        push (@scratch_files, $getattr_temp);
    }
    map {push(@scratch_files, $_)} values %{$pcf->temporary_i_o};
    push @scratch_files, @support_output_files;

    # Clean scratch and temporary files, then temporary directories
    unlink @scratch_files or S4P::logger('WARN', "main: Cannot unlink scratch files " . 
        join(', ', @scratch_files) .  ": $!");

    # Clean those pesky toolkit temporary files:  all pcNNNNNNN... pattern
    opendir DIR, '.';
    map {unlink $_ or S4P::logger('ERROR', "main: Cannot delete $_: $!")} 
        grep(/^pc\d+(\.met)?$/, readdir DIR);
    closedir DIR;

    # Clean temporary directories
    clean_tmp_dirs(@local_dirs) if @local_dirs;
    exit(0);
}
else {
    S4P::logger("ERROR", "main: Algorithm failed with code=$err_number");
    my ($failpge) = production_history($pcf_name, $pcf, $err_number);

    # Use $job_type for pge name in metfile
    failpge_metfile($failpge_met_template, $failpge, $job_type, $pge_version, 
        $job_id, $err_number) or 
        S4P::perish(120, "main: Failed to make metfile for FAILPGE tar file");
    pdr_work_order('ECS', 'EXPORT_FAILPGE', $job_type . 'FAILPGE_' . $job_id, 
        $node_name, $rh_output_metfiles, $failpge);
    exit($err_number);
}

##########################################################################
# Subroutines
##########################################################################
# clean_tmp_dirs(@dirs)
#   @dirs - list of directories to be removed
#   This unlinks symlinks and recursively removes local subdirectories
#=========================================================================
sub clean_tmp_dirs {
    my ($dir, $file);
    foreach $dir(@_) {

        # Aaack! Don't remove current or parent!
        next if ($dir eq '.' || $dir eq '..');
        next if ($dir =~ m/^\s*$/);
        next if ($dir =~ m/^\//);

        # If it is not just a symlink, clean out the files
        if (! -l $dir) {
            foreach $file(glob("$dir/*")) {
                unlink($file) or 
                    S4P::perish(102, "clean_tmp_dirs(): Cannot unlink local file $file: $!");
            }
        }

        # Remove the symlink or directory
        unlink $dir or 
            S4P::perish(103, "clean_tmp_dirs(): Cannot unlink local directory $dir: $!");
    }
    return 1;
}
# Output SWEEP work order
sub clean_work_order {
    my ($job_id, $clean_type, @files) = @_;
    # Write out SWEEP work order for the actual deletion and deallocation
    my $clean_work_order = "SWEEP.$clean_type" . "_$job_id.wo";
    open SWEEP_WORK_ORDER, ">$clean_work_order" or
        S4P::perish(137, "clean_work_order(): Cannot open $clean_work_order to write: $!");
    foreach my $file(@files) {
        print SWEEP_WORK_ORDER "$file\n";
    }
    close SWEEP_WORK_ORDER;
    return $clean_work_order;
}
sub elevate_metfiles {
    my $pdr = shift;
    foreach my $fg(@{$pdr->file_groups}) {
        my ($met_fs, $n_sci);

        # Loop through the FILE_SPECS, looking for met and science files
        foreach my $fs(@{$fg->file_specs}) {
            if ($fs->file_type =~ /^MET/i) {
                $met_fs = $fs;
            }
            elsif ($fs->file_type =~ /^(SCI|HDF)/i) {
                # We only care if there is more than 1
                $n_sci++;
            }
        }
        # If multi-file granule, move the metadata file up one level
        # and modify PDR accordingly
        if ($n_sci > 1) {
            my $oldmet = $met_fs->pathname;
            my $newdir = dirname($met_fs->directory_id);
            my $newpath = "$newdir/" . basename($oldmet);
            if (-f $newpath && ! -f $oldmet) {
                S4P::logger('INFO', "Metfile $oldmet already moved to $newdir, modifying PDR");
            }
            else {
                move($oldmet, $newdir) or 
                    S4P::perish(180, "Failed to move $oldmet to $newdir: $!");
            }
            $met_fs->directory_id($newdir);
        }
    }
}
sub export_browse {
    my ($rh_browse_data_map, $rh_met, $ra_block_export, $job_type, $job_id, 
        $node_name, $rh_met, $export_pdr) = @_;
    my %browse_data_map = %{$rh_browse_data_map} or return;
    my $browse_file;
    my $n_pdr;
    my %block_export = map {($_, 1)} @$ra_block_export;
    # Loop through browse files
    my @export_files;
    foreach $browse_file(keys %browse_data_map) {
        my $browse_dir = dirname($browse_file);
        # Handle phantom browse so no failure due to missing browse
        if (! -f $browse_file) {
            S4P::logger('INFO', "export_browse(): Browse file $browse_file not generated");
            next;
        }
        # Screen out data files that we are not sending due to QC issues
        my @data_files = grep {! exists $block_export{$_}} 
            @{$browse_data_map{$browse_file}};

        my $n_data = scalar(@data_files);
        if ($n_data == 0) {
            S4P::logger('WARN', "No data files left for browse $browse_file, skipping...");
            next;
        }
        elsif ( $n_data == 1 && (dirname($data_files[0]) eq $browse_dir) ) {
            # Only 1 data file: put it in the same PDR
            # N.B.:  ECS also requires Browse to be in same directory
            my $fs = new S4P::FileSpec('file_type' => 'BROWSE', 
                'directory_id' => $browse_dir, 
                'file_id' => basename($browse_file),
                'file_size' => (-s $browse_file));
            my $found = 0;
            # Loop through FILE_GROUPS
            foreach my $fg(@{$export_pdr->file_groups}) {
                # N.B.:  Does not work with multi-file granules!
                my @scifiles = $fg->science_files;

                if ($scifiles[0] eq $data_files[0]) {
                    # MATCH:  add to this FILE_GROUP
                    push @{$fg->file_specs}, $fs;
                    $found = 1;
                    S4P::logger('INFO', "export_browse(): Added $browse_file to FILE_GROUP for $data_files[0]");
                    last;
                }
            }
            S4P::logger('ERROR', "Cannot find matching data file for $browse_file") unless ($found);
        }
        else {
            # Linkage file is named same as browse file, but with "LINKFILE" as
            # pseudo-data-type, and .pvl at the end
            my ($base, $dir, $ext) = fileparse($browse_file, '\.hdf');
            my $linkage_file_name = "$dir/LINKFILE.$base.pvl";

            # Create the linkage file
            linkage_file($linkage_file_name, $rh_met, @data_files);
            push @export_files, $browse_file, $linkage_file_name;
            S4P::logger('INFO', "export_browse(): Created linkage file $linkage_file_name");
        }
    }
    unless ($#export_files < 0) {
        return ( pdr_work_order('ECS', 'EXPORT_BROWSE',
            $job_type . '_' . $job_id, $node_name, $rh_output_metfiles, 
            @export_files) );
    } else {
        return(1) ;
    } 
}
##############################################################################
# fail_pge_metfile($template, $failpge, $pge, $version, $job_id, $exit_code)
#   $template - metadata template
#   $failpge - Failed algorithm tar file
#   $pge - Name of the pge (for met file)
#   $version - Algorithm Version (for met file)
#   $job_id - Job identifier (for met file - half of DPR_ID)
#   $exit_code - Algorithm exit code (for met file)
#------------------------------------------------------------------------------
#   This creates the metadata file for the FAILPGE.
#==============================================================================
sub failpge_metfile {
    my ($template, $failpge, $pge, $version, $job_id, $exit_code) = @_;
    my $outfile = "$failpge.met";
    if (! $version) {
        S4P::logger('ERROR', "failpge_metfile(): Cannot create FAILPGE metfile without algorithm version (-v): $!");
        return 0;
    }
    if (! open IN, $template) {
        S4P::logger('ERROR', "failpge_metfile(): Cannot open input metfile template $template: $!");
        return 0;
    }
    if (! open OUT, ">$failpge.met") {
        S4P::logger('ERROR', "failpge_metfile(): Cannot open output metfile $failpge.met: $!");
        return 0;
    }
    my $size = (-s $failpge) / (1024. * 1024);
    if (! $size) {
        S4P::logger('ERROR', "failpge_metfile(): $failpge is zero size");
        return 0;
    }
    my $insert_time = S4P::TimeTools::CCSDSa_Now();
    # SSS cannot handle T and Z in times
    $insert_time =~ s/[TZ]/ /g;
    while (<IN>) {
        s/INSERT_SIZE_HERE/$size/ ||
        s/INSERT_PGEVERSION_HERE/$version/ ||
        s/INSERT_PGE_HERE/$pge/ ||
        s/INSERT_EXIT_CODE_HERE/$exit_code/ ||
        s/INSERT_JOB_ID_HERE/$pge.$job_id/ ||
        s/INSERT_TIME_HERE/$insert_time/;
        print OUT;
    }
    close IN;
    close OUT;
    return 1;
}
##############################################################################
# finish_runlog($file, $pcf_file, $ra_product_output, $ra_support_output,
#               $ra_product_input, $ra_support_input)
#   $file - output file for runlog
#   $pcf_file - Process Control File for algorithm run
#   $ra_product_output - reference to array of product output files
#   $ra_support_output - reference to array of support output files (e.g. logs)
#   $ra_product_input - reference to array of product input files
#   $ra_support_input - reference to array of support input files (including
#                       leapsec.dat and utcpole.dat)
#------------------------------------------------------------------------------
#   This finishes off the production log in an effort to look just like
#   the ECS production logs.  It includes:
#   o  Algorithm Completion time
#   o  First line of leapsec.dat and utcpole.dat
#   o  Output filenames
#   o  Input filenames
#==============================================================================
sub finish_runlog {
    my ($file, $pcf_file, $ra_product_output, $ra_support_output, 
        $ra_product_input, $ra_support_input) = @_;
    if (!open LOG, ">>$file") {
        S4P::logger("WARN", "finish_runlog(): Cannot open runlog file $file for append: $!");
        return 0;
    }

    # algorithm Completion time

    print LOG "# Algorithm Completion time (GMT) in Toolkit format\n";
    my ($sec,$min,$hour,$mday,$mon,$year) = gmtime(time);
    $year += 1900;
    printf LOG 
        "PGE_COMPLETION_TIME_TOOLKIT_FORMAT=\"%04d-%02d-%02dT%02d:%02d:%02d\"\n",
        $year, $mon+1, $mday, $hour, $min, $sec;
    print LOG "# Algorithm Completion time (GMT) in understandable format\n";
    print LOG "# Format : Year[Number of years since 1900]:Month[0,11]:Day[1,31]:Hour[0,23]:Min [0,59]:Sec[0,61]\n";
    print LOG "PGE_COMPLETION_TIME_YEAR=$year\n";
    print LOG "PGE_COMPLETION_TIME_MONTH=$mon\n";
    print LOG "PGE_COMPLETION_TIME_DATE=$mday\n";
    print LOG "PGE_COMPLETION_TIME_HOUR=$hour\n";
    print LOG "PGE_COMPLETION_TIME_MIN=$min\n";
    print LOG "PGE_COMPLETION_TIME_SEC=$sec\n\n";

    # First line of leapsec.dat and utcpole

    my @ancillary = grep /(leapsec|utcpole)/, @$ra_support_input;
    if (@ancillary) {
        map {$_ =~ s/^~/$ENV{'PGSHOME'}/} @ancillary if ($ENV{'PGSHOME'});
        print LOG "ANCILLARY_DATA:\n";
        map { printf LOG "%s=%s\n", 
            (/leapsec/) ? 'LEAPSECONDSFILE' : 'ORBITPOLARMOTION', `head -1 $_`}
            @ancillary;
    }

    # Output filenames
    print LOG "File names and actual sizes after algorithm completed\n";
    foreach my $f(@$ra_product_output) {
        my @out = (ref $f) ? @$f : ($f);
        map {printf LOG "FILE=%s=%d\n", $_, -s $_} @out;
    }

    # Input filenames:  all input filenames with absolute paths
    # This screens out static files (relative paths), toolkit files
    # (~ paths or no paths), temporary files (. paths)
    map {printf LOG "RUNTIME_FILE=%s\n", $_} grep /^\//, @$ra_product_input;
    close LOG;
}
sub filespec_dir2files {
    my $pdr = shift;
    # Loop through FILE_GROUPs
    foreach my $fg(@{$pdr->file_groups}) {
        # Loop through FILE_SPECs
        my (@new_fs, $modify);
        foreach my $fs(@{$fg->file_specs}) {
            my $path = $fs->pathname;
            # Only necessary for FILE_SPECs that point to directories
            if ($fs->file_type eq 'SCIENCE' && -d $path) {
                 $modify = 1;
                 # Read files in the directory
                 opendir(DIR, $path) or 
                     S4P::perish(179, "Cannot open granule directory $path: $!");
                 my @files = grep !/^\./, readdir(DIR);

                 # Create new FILE_SPECs pointing to the files
                 foreach my $file(@files) {
                     my $new_fs = S4P::FileSpec->new(
                         'directory_id' => $path,
                         'file_id' => $file,
                         'file_size' => (-s "$path/$file"),
                         'file_type' => 'SCIENCE'
                     );
                     push(@new_fs, $new_fs);
                }
            }
            else {
                push(@new_fs, $fs);
            }
        } # End of FILE_SPEC loop
        $fg->file_specs(\@new_fs) if ($modify);
    } # End of FILE_GROUP loop
    $pdr->recount();
}
           

##########################################################################
# get_esdt_version_lgid($pathname)
#   $pathname - pathname of file for which to get LGID
#     if this does not end with .met, it looks in $pathname.met
#-------------------------------------------------------------------------
# LGID specification from Volume 0 of the ECS-SIPS specification:
# LGID:<ESDT>:<ESDTVersion>:<filename>
#=========================================================================
sub get_esdt_version_lgid {
    my $metfile = shift;
    my %attr = S4P::MetFile::get_from_met($metfile, 'SHORTNAME', 'VERSIONID', 
        'LOCALGRANULEID');
    S4P::perish(91, "get_esdt_version_lgid(): Cannot find SHORTNAME in $metfile") if !$attr{'SHORTNAME'};
    S4P::perish(92, "get_esdt_version_lgid(): Cannot find VERSIONID in $metfile") if !$attr{'VERSIONID'};
    my $lgid = sprintf("LGID:%s:%03d:%s", $attr{'SHORTNAME'}, 
        $attr{'VERSIONID'}, $attr{'LOCALGRANULEID'}) if $attr{'LOCALGRANULEID'};
    return ( $attr{'SHORTNAME'}, $attr{'VERSIONID'}, $lgid);
}
##############################################################################
# linkage_file($pathname, $rh_met, @files)
#   $pathname - output pathname for linkage file
#   $rh_met - hash of metfiles, keyed on output product files
#   @files - data files to point to
# ----------------------------------------------------------------------------
# This creates a linkage file for linking production histories and browse
# with the output granules they refer to.  The link is done via Local
# Granule ID, as per Volume 0 of the ECS-SIPS ICD.
#=============================================================================
sub linkage_file {
    my ($pathname, $rh_met, @files) = @_;
    my $file;
    my $n_files = 0;
    my ($file, @linkage);
    # Loop through files, using only those which:
    # have non-zero size and LocalGranuleID
    foreach $file(@files) {
        # Filter out zero-length files
        next unless (-s $file);
        my ($esdt, $version, $lgid) = get_esdt_version_lgid(find_metfile($file, $rh_met));
        # Filter out those which have no lgid
        if ($lgid) {
            $n_files++;
            push(@linkage, sprintf("  GRANULE_POINTER_%03d=\"%s\"", $n_files,$lgid));
        }
    }
    if (! $n_files) {
        S4P::logger('WARN', "linkage_file(): No files to link in linkage file $pathname");
        return;
    }
    open LINKAGE, ">$pathname" or 
        S4P::perish(123, "linkage_file(): Cannot open linkage file $pathname to write: $!");
    print LINKAGE "GROUP=LINKAGE_POINTERS\n";
    printf LINKAGE "  NUM_POINTERS=%d\n", $n_files;
    print LINKAGE join("\n", @linkage);
    print LINKAGE "\nEND_GROUP=LINKAGE_POINTERS\nEND\n";
    close(LINKAGE);
    return $pathname;
}
##########################################################################
# log2stderr($runlog_file)
#   $runlog_file - pathname of run-log file
# Cats the run-log file to STDERR and deletes it
##########################################################################
sub log2stderr {
    my $runlog_file = shift;
    open RUNLOG, $runlog_file;
    while (<RUNLOG>) {
        print STDERR;
    }
    close RUNLOG;
    unlink $runlog_file;
}
##########################################################################
# make_local_dirs(@dirs)
#   @dirs - list of local directories to either symlink or make
# ----------------------------------------------------------------------------
# This makes local subdirectories for paths that start with './' and
# symlinks for those that do not.
# It is used to make all of the input and output directories for a algorithm run
# available "locally", within the job directory, for easy troubleshooting.
#=============================================================================
sub make_local_dirs {
    my ($dir, $use_symlinks);
    my (@dirs, $link);
    foreach $dir(@_) {
        # Skip if it's just the current or parent directory
        next if ($dir eq '.' || $dir eq '..');

        # Make a subdirectory if it starts with ./
        if ($dir =~ m#^\./#) {
            mkdir($dir, 0775) or 
                S4P::perish(2, "make_local_dirs(): Cannot make local dir $dir: $!");
            push @dirs, $dir;
        }

        # Otherwise, create a symbolic link to the directory
        elsif ($use_symlinks) {
            $link = $dir;
            $link =~ s#\.\.#+#g;
            $link =~ s#/#_#g;
            $link =~ s/^_//;
            # Screen out circular links and non-existent directories
            # We don't warn of these, as they can be common for unused LUNs
            # in a pcf.
            next if ($dir eq $link);
            next if (! -d $dir);
            symlink($dir, $link) or 
                S4P::logger('WARN', "make_local_dirs(): Cannot create symlink $link to $dir: $!");
            push @dirs, $link;
        }
    }
    return @dirs;
}
##########################################################################
#=============================================================================
sub make_pdr {
    my ($pdr_type, $node_name, $rh_met, @files) = @_;
    $expiration =~ m/(\d+)\s*(\w+)/;
    my $exp_number = $1;
    my $exp_units = $2;
    my $pdr = S4P::PDR::start_pdr('originating_system' => $main::originating_system,
        'expiration_time' => S4P::PDR::get_exp_time($exp_number, $exp_units));

    # Loop through each filename in the PDR
    my ($i, $n_files, $penultimate);
    $n_files = scalar(@files);
    $penultimate = $n_files - 1; 
    for ($i = 0; $i < $n_files; $i++) {
        my ($filename, undef) = fileparse($files[$i]);
        my $granule;

        # Production history must be followed by its linkage file in the arglist
        # Regarless of S4PM file name pattern, PH files will always be named
        # PH.*

        my $esdt;
        if ( $filename =~ /^PH/ ) {
            $esdt = 'PH';
        } else {
            ($esdt, undef, undef, undef, undef) =
                S4PM::parse_patterned_filename($files[$i]);
        }
        S4P::logger("INFO", "make_pdr(): esdt: [$esdt]");

        # If this comes before a LINKFILE, it is either Browse or 
        # PH (deprecated)
        if ($i < $penultimate && ($files[$i+1] =~ /LINKFILE\..*pvl$/)) {
            my $version = '001';
            # Will have a .met if this is Browse
            ($esdt, $version) = get_esdt_version_lgid(find_metfile($files[$i], $rh_met))
                unless ($filename =~ /^PH/);
            $granule = $pdr->add_granule(
                'data_type' => $esdt, 
                'data_version' => $version,
                'node_name' => $node_name,
                'files'=>[$files[$i], $files[$i+1]]);
            if ($filename !~ /^PH/) {
                $granule->file_specs->[0]->file_type('BROWSE');
                $granule->data_type('Browse');
            }
            $i++;
        }
        # Normal case
        else {
            my $metfile = find_metfile($files[$i], $rh_met);
            my ($esdt, $version, $lgid) = get_esdt_version_lgid($metfile);
            # Multi-file or single-file
            my @granfiles = (ref $files[$i]) ? @{$files[$i]} : ($files[$i]);
            push @granfiles, $metfile;

            $granule = $pdr->add_granule(
                'data_type' => $esdt,
                'data_version' => $version,
                'node_name' => $node_name,
                'files'=>\@granfiles);

            # Add LGID as UR if the output PDR is a non-ECS-standard one
            # This is so it can be saved in the database and later used for
            # InputGranulePointer

            $granule->ur($lgid) if ($pdr_type ne 'ECS');
        }
    }
    $pdr->resolve_paths;
    return $pdr;
}
############################################################################
# Find metadata file
############################################################################
sub find_metfile {
    my ($data_file, $rh_met) = @_;

    # (1) Look for the metfile in the hash constructed from the PCF
    #     This is where it is SUPPOSED to be
    my $metfile = $rh_met->{$data_file} if $rh_met;
    return ($metfile) if (-f $metfile);
    S4P::logger('WARN', "Could not find metadata file $metfile, will try to guess");

    # (2) Look for file passed in as argument if it has a .met or .xml
    # (3) If argument is a data file, first try adding .met
    my $root = (ref $data_file) ? $data_file->[0] : $data_file;
    my $met_file = ($root =~ /\.(met|xml)/) ? $root : "$root.met";
    return $met_file if (-f $met_file);

    # (3) Assume multi-file granule, try removing version tag
    $met_file =~ s/:[0-9]+//;
    return $met_file if (-f $met_file);
    
    # (4) If no .met file exists, then try adding .xml
    $met_file =~ s/\.met$/.xml/;
    return $met_file if (-f $met_file);

    # (5) All attempts failed
    warn "Cannot find either metadata file $data_file.met or $data_file.xml";
    return;
}

sub get_output_files {
    my ($pcf, $rh_data_browse_map) = @_;
    my %data_browse_map = %{$rh_data_browse_map};

    # Configuration map is based on luns, with data lun as key and browse lun
    # as value.  There can be only one browse file for a given data file
    # Browse-data map reverses this pattern, using files:
    # Key is pathname of browse file, value is anonymous array of data files
    my %browse_data_map;

    # Get output files indexed on LUN for browse mapping
    my %output = %{$pcf->product_output_files};

    # Also get output GRANULES indexed on metfile
    # This allows us to keep files for a given granule together
    my %output_granules = %{$pcf->product_granules('PRODUCT OUTPUT FILES')};

    my %input = %{$pcf->product_input_files};
    my %met = %{$pcf->output_met_files('PRODUCT OUTPUT FILES')};
    my ($data_lun, $browse_lun, %delete_browse_met);

    # Loop through each browse lun in the browse map
    # N.B.:  Can have only ONE version per LUN for browse
    while ( ($data_lun, $browse_lun) = each %data_browse_map) {
        # No need to split browse files in that hash value, can only be 1
        my $browse_file = $output{$browse_lun};
        my @data_files = split('\s', $output{$data_lun});
    
        # Add array of data files for this LUN to the hash
        push @{$browse_data_map{$browse_file}}, @data_files;

        S4P::logger('INFO', "Mapping data to browse for lun $browse_lun");
        # Browse files are kept separate so they go into separate EXPORT
        # work order
        my $metfile = $met{$browse_lun} or 
            S4P::perish(179, "Cannot find metfile for Browse LUN $browse_lun");
        S4P::logger('INFO', "Setting aside $metfile from list of output granules");
        # Delete browse from product output so they don't go in the main EXPORT
        # work order
        delete $output_granules{$metfile};

        # Now check input files for possible links
        # This allows linking to output files from upstream algorithms
        if ($input{$data_lun}) {
            @data_files = split('\s', $input{$data_lun});
            push @{$browse_data_map{$browse_file}}, @data_files;
        }
    }

    # Make an array of product_output_files from the %output_granules hash
    my (@product_output_files, %output_met);
    foreach my $met(keys %output_granules) {
        my $ra_scifiles = $output_granules{$met};
        # If multi-file granule, stash as an anonymous array ref
        my $outfile = (scalar(@$ra_scifiles) == 1) ? $ra_scifiles->[0] 
                                                   : $ra_scifiles;
        push @product_output_files, $outfile;
        $output_met{$outfile} = $met;
        # If its an array reference, also hash the output files individually
        # for later lookup
        if (ref $outfile) {
            map {$output_met{$_} = $met} @$outfile;
        }
    }
    return (\@product_output_files, \%browse_data_map, \%output_met);
}

sub merge_pdrs {
    my ($register_pdr, $export_pdr) = @_;

    S4P::logger('INFO', "Merging REGISTER_DATA and EXPORT PDRs");
    # Put FILE_GROUPS into hashes for easy finding
    my %register_fg = map {($_->met_file(), $_)} @{$register_pdr->file_groups};
    my %export_fg = map {($_->met_file(), $_)} @{$export_pdr->file_groups};

    # First overwrite the REGISTER FILE_GROUPs with corresponding
    # EXPORT FILE_GROUPs, because they have checksum info
    foreach my $gran(keys %register_fg) {
        if ($export_fg{$gran}) {
            $register_fg{$gran} = $export_fg{$gran};
            $register_fg{$gran}->status('EXPORT');
        }
    }
    # Add in EXPORT FILE_GROUPs that were not in REGISTER owing to their
    # QC status (i.e., block registering but not exporting)
    # Set status to NO_REGISTER so s4pm_register_data.pl ignores it
    foreach my $gran(keys %export_fg) {
        if (! $register_fg{$gran}) {
            $register_fg{$gran} = $export_fg{$gran};
            $register_fg{$gran}->status('EXPORT_ONLY');
        }
    }
    # Replace the REGISTER file_groups with those in the hash
    my @register_fg = values %register_fg;
    $register_pdr->file_groups(\@register_fg);
    $register_pdr->recount();
    
    # Zero out the EXPORT PDR to suppress writing
    my @null_fg;
    $export_pdr->file_groups(\@null_fg);
    $export_pdr->total_file_count(0);

    return $register_pdr;
}

##########################################################################
# pdr_work_order($pdr_type, $job_type, $job_id, $node_name, $rh_met, @files)
# ----------------------------------------------------------------------------
#   Creates a PDR work order (actually most of the work is done in make_pdr()).
#=============================================================================
sub pdr_work_order {
    my ($pdr_type, $job_type, $job_id, $node_name, $rh_met, @files) = @_;

    my $filename = "$job_type.$job_id.wo";
    my $pdr = ref($files[0]) ? $files[0] : make_pdr($pdr_type, $node_name, $rh_met, @files);
    S4P::perish(110, "pdr_work_order(): Failed to make PDR for output work order $filename") 
        unless $pdr;

    # pdr->write_pdr has weird error return:  non-zero if failure
    if ($pdr->errors) {
        S4P::perish(111, "pdr_work_order(): Errors in PDR for output work order $filename");
    }
    elsif ($pdr->write_pdr($filename)) {
        S4P::perish(112, "pdr_work_order(): Failed to write output work order $filename");
    }
}

##########################################################################
# production_history($pcf_name, $pcf, $err_number, $export_pdr, @files) 
#   $pcf_name - name of Process Control File
#   $pcf - parsed PCF object
#   $err_number - exit code of algorithm
#   @files - all files to be included in production history (more will be added)
# ----------------------------------------------------------------------------
# Makes both Production History and FAILPGE tar files, depending on what 
# the error code was.
# The two are very similar, the main difference (aside from the ESDT and name)
# being whether a linkage file is made to point to the output data granules.
# (FAILPGE has no linkage file.)
#=============================================================================
sub production_history {
    my ($pcf_name, $pcf, $err_number, $export_pdr, $rh_met, @files) = @_;

    # N.B.:  Filename is PH.[AP]AYYYYDDDHHMM.VVV.YYYYDDDHHMMSS.tar
    # Linkage file is LINKFILE.[AP]AYYYYDDDHHMM.VVV.YYYYDDDHHMMSS.pvl
    # .Log includes rusage info, LEAPSECONDSFILE, ORBITPOLARMOTION
    # .Tk{User,Report,Status} = Log{User,Report,Status} file

    my ($prefix, $root) = split('\.', $pcf_name, 2);
    my $chain_log = "$root.log";
    if (-f $chain_log) {
        my $logname = "$chain_log.chain";
        copy $chain_log, $logname;
        push(@files, $logname) if (-f $logname);
    }
    $root =~ s/^RUN_//;

    # Add symlinks to log files
    my %log_files = %{ $pcf->log_files };
    my ($err, $ftype);
    foreach $ftype('status', 'report', 'user') {
        if (! -f $log_files{$ftype}) {
            $err++;
            S4P::logger('WARN', 
                "production_history(): Cannot find $ftype logfile $log_files{$ftype}");
        }
        else {
            my $suffix = ucfirst $ftype;
            my $filename = "$root.Tk$suffix";
            symlink ($log_files{$ftype}, $filename) if ($filename ne $log_files{$ftype}) ;
            push @files, $filename;
        }
    }

    # Add core file if there
    if ($err_number && -f "core") {
        rename ("core", "$root.core");
        push (@files, "$root.core");
    }

    # Add PCF
    symlink ($pcf_name, "$root.pcf");
    push (@files, "$root.pcf");

    # Create tar file, following symbolic links
    my ($type, $tarfile, $linkage_file);

    # If it failed, make a FAILPGE package
    if ($err_number) {
        $tarfile = "FAILPGE.$root.tar";
    }

    # Successful Algorithm:  will make a production history and linkage file
    else {
        my @output_files = $export_pdr->files('SCIENCE');

        # The PH directory is normally in the same tree as the output files,
        # but in a subdirectory named PH
        my $dir = dirname($output_files[0]) if ($output_files[0]);
        my $ph_dir = ($dir) ? (dirname($dir) . '/PH') : '../../DATA/PH';

        # But if that subdirectory doesn't exist, put it in the same directory
        # as the first output file you come to
        if (! -d $ph_dir) {
            S4P::logger('WARN', "production_history(): Normal production history directory $ph_dir not found; using $dir instead");
            $ph_dir = $dir;
        }
        $tarfile = "$ph_dir/PH.$root.tar";
        $linkage_file = linkage_file("$ph_dir/LINKFILE.$root.pvl", $rh_met, @output_files);
    }

    # Tar up the appropriate files in the directory
    my $osname = `/bin/uname -s`;
    my $tar_args = ($osname =~ /IRIX64/) ? "-Lcf" : "-hcf";
    my ($errstr, $errnum) = S4P::exec_system('tar', $tar_args, $tarfile, @files);
    if ($errnum) {
        S4P::logger("ERROR", "production_history(): Cannot make tar file $tarfile: $errstr");
        return undef;
    }

    # Remove all of the symlinks and the core
    unlink @files;
    return ($tarfile, $linkage_file);
}

##########################################################################
# prune_empty_files(\@product_output_files, $job_id)
#   \@product_output_files - pointer to product_output_files
#----------------------------------------------------------------------------
# This generates a SWEEP work order for clean_data so it can be 
# deallocated.  It also removes them from the product_output_files
# array so that they don't show up in PDRs.
#=============================================================================
sub prune_empty_files {
    my ($ra_product_output, $job_id) = @_;
    my ($i, $n, $file);
    $n = scalar(@{$ra_product_output});
    my @empty_files = ();
    # Loop through all files in array
    # in reverse order so as not to clobber indices with splice
    for ($i = $n-1; $i >= 0; $i--) {
        $file = $ra_product_output->[$i];
        # Multi-file granule case
        if (ref $file) {
            next if (grep {-s $_} @$file);
            # Use directory for CLEAN work order
            push @empty_files, dirname($file->[0]);
            splice(@{$ra_product_output}, $i, 1);
        }
        # If zero-length or non-existent, then delete (unlink) and 
        # remove from array (splice)
        elsif (! -s $file) {
            $file =~ s/\n$//;  # Try to eliminate mystery blank line
            push (@empty_files, $file) if $file;
            splice(@{$ra_product_output}, $i, 1);
        }
    }
    return "No empty files" if (! @empty_files);
    return clean_work_order($job_id, 'EMPTY', @empty_files);
}
sub save_runlog {
    my ($dir, $file) = @_;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday) = localtime(time);
    my $dest = sprintf("%s/%04d%03d_%02d", $dir, $year+1900, $yday+1, $hour);
    if (! -d $dest && !mkdir($dest, 0775)) {
        S4P::logger('WARN', "save_runlog(): Failed to create log directory $dest: $!");
        return 0;
    }
    if (!copy($file, "$dest/$file")) {
        S4P::logger('WARN', "save_runlog(): Failed to copy runlog $file to $dest: $!");
        return 0;
    }
    return 1;
}
##########################################################################
# unique_dirs(@pathnames)
#   @pathnames - list of pathnames
#----------------------------------------------------------------------------
# Outputs a unique list of the directories holding the pathnames in the input
# arguments.
#=============================================================================
sub unique_dirs {
    my %dirs;

    # Replace ~ with PGSHOME if it is set
    if ($ENV{'PGSHOME'}) {
        map {$_ =~ s/^~/$ENV{'PGSHOME'}/} @_;
    }
    # Put all the directories in a hash, then return the keys
    map { $dirs{dirname($_)} = 1; } @_;
    return (keys %dirs);
}

##########################################################################
# update_work_order($job_id, $ra_input_files)
#   $job_id - job identifier
#   $ra_input_files - reference to an array of input files
#----------------------------------------------------------------------------
# Creates the UPDATE work order, decrementing the Uses for each input file.
#=============================================================================
sub update_work_order {
    my ($job_id, $ra_input_files) = @_;

    # Open UPDATE work order output file
    my $filename = "UPDATE.UPDATE_" . "$job_id.$$.wo";
    open (UPDATE, ">$filename") or 
        S4P::perish(120, "update_work_order(): Cannot open output work order $filename");

    # Multi-file entities have to be stripped back to their dirname

    # Update uses for files
    my %granule_ids;
    foreach my $file( @{$ra_input_files}) {
        # Save multi-file guys for later
        if ( ($file =~ /RM[AT]000\./) || ($file =~ /PDS$/) ) {
            $granule_ids{dirname($file)} = 1;
        }
        else {
            printf UPDATE "FileId=%s Uses=-1\n", $file;
        }
    }
    # Multi-file granules (MODIS L0, AIRS L0...)
    foreach my $granule(keys %granule_ids) {
        printf UPDATE "FileId=%s Uses=-1\n", $granule;
    }
    close(UPDATE);

    return $filename;
}
