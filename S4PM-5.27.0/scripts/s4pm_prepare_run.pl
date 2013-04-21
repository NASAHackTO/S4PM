#!/usr/bin/perl

=head1 NAME

s4pm_prepare_run.pl - build a runtime PCF and trigger the run of an algorithm

=head1 SYNOPSIS

s4pm_prepare_run.pl 
B<-c[onfig]> I<Config_file> 
B<-p[ge]> I<algorithm>
[B<-p[arse]>]
I<input_workorder>

=head1 DESCRIPTION

The B<Prepare Run> station is responsible for triggering the run of an algorithm
within the S4PM. The input PREPARE work order contains actual file names
and locations of input data that can be used by the run of a algorithm. The
B<Prepare Run> uses that input work order along with a PCF template (specific
to each algorithm) to construct a runtime PCF to be used for a single run of the
algorithm.

The output work order is a ALLOCATE work order, essentially a runtime PCF 
but with the directory locations for the output files unspecified. These 
ALLOCATE work orders are then sent to the B<Allocate Disk> station for 
the determination and setting of these directory locations. At this point, the 
PCFs are fully usable by the algorithm.

The B<Prepare Run> station makes use of the B<Select Data> station algorithm
configuration files (the same ones used by B<Select Data> itself). These
are assumed to be in the station directory (or, links to them).

The B<Prepare Run> station also uses a station unique configuration file,
I<s4pm_prepare_run.cfg> which, at present, contains the settings for very few
parameters. The location to this file is specified in the argument B<-c>.

To support AIRS algorithms, the B<Prepare Run> station includes the subroutine
rectify_AIRS_L0_entries() to further modify the PCF generated.

To support on-demand processing, if -p is included B<Prepare Run> will 
check the input work order for the data type PSPEC. When it sees this 
data type, it knows to use the file indicated to retrieve any specialized 
criteria. Using the specialized criteria from this file along with the 
LUN mapping in the Select Data configuration file, it places the 
specialized criteria in the output PCF as runtime parameters. If -p is
omitted, there will be no parsing of the PSPEC file although the PCF
will still contain an entry for it (as any other data type).

=head1 OPTIONS

=over 4

=item B<-c[onfig]> I<Config_file>

Configuration file for the Prepare Run station (Optional).

=item B<-p[ge]> I<algorithm>

This is the name of the algorithm.

=item B<-p[arse]>

If set, the PSPEC file will be the source of parameters to be passed to
the algorithm and set in the PCF via this script. This almost always means
that we're running in an on-demand string.

=back

=head1 PCF TEMPLATE

B<s4pm_prepare_run.pl> uses a Process Control File (PCF) template based on the
PCF template provided by the SDP Toolkit from ECS. Unlike the ECS Plannning
and Data Processing System (PDPS), the PCF template used in S4PM is tailored
for each algorithm. Each version of every algorithm must be associated with a 
PCF template. The path to the specific PCF template is one of the items 
specified in the Select Data Configuration file (s4pm_select_data(1)).

The PCF template must contain all possible input and output logical unit
numbers (or LUNs, also referred to as logical identifiers or LIDs). Further,
LUNs for all needed runtime parameters and their values need to be included
as well as LUNs for support files (e.g. log and temporary files).

=head2 PRODUCT INPUT FILES

For dynamic input files, the file names and directory paths within the PCF
entries are irrelevant and only serve as placeholders for the 
B<s4pm_prepare_run.pl> script. Only the LUNs are significant. All possible 
input LUNs must be included and must be uncommented.

For input LUNs which will or may contain multiple files (same LUN, different
versions), only one entry need be in the PCF template with the version set
to 1 in the last field of the entry. B<s4pm_prepare_run.pl> will flesh out
the remaining versions correctly depending on the number of files dynamically
determined. Do not, however, only include a single LUN where the version
is set to 2 or higher as this will confuse B<s4pm_prepare_run.pl>.

Static files in S4PM are permanently resident on the S4PM file system. 
Therefore, PCF entries for static files must have actual file names and
directory locations (unlike with dynamic files). Such static files include
Metadata Configuration Files (MCFs).

The default directory location is not used in this section of the PCF.

=head2 PRODUCT OUTPUT FILES

The default directory for production output files must be set to ./ (that
is, the current directory). Further, this default must not be overriden
by specifying directory locations in the individual PCF entries in this
section.

As with files in the PRODUCT INPUT FILES section, only the LUNs are 
significant. The file names and directory locations are merely placeholders.

=head2 SUPPORT OUTPUT FILES

The default directory for support output files must be set to ./ (that
is, the current directory). Further, this default must not be overriden
by specifying directory locations in the individual PCF entries in this
section.

Standard file names for the Toolkit log files are recommended: LogUser,
LogStatus, and LogReport.

=head2 TEMPORARY I/O

The default directory for temporary I/O files must be set to ./ (that
is, the current directory). There should be no PCF entries in this section.

=head2 USER DEFINED RUNTIME PARAMETERS

Make sure that all required parameters are included and properly set. They
will be accessed by the algorithm as set here.

=head1 AUTHOR

Bryan Zhou, NASA/GSFC, Code 610.2
Stephen Berrick, NASA/GSFC, Code 610.2

=head1 CREATED

08/15/2000

=head1 TO DO

=over 4

=item 1.

Whether metadata should be read from the metadata file or the HDF file itself 
should be made on an ESDT-by-ESDT basis. Right now it's global. This needs to
be fixed.

=item 2.

Currently, the temporal coverage of all output products is assumed to be the 
same. In fact, this is true for MODIS. But it may not be that way always.
Consequently, this needs to be handled better.

=back

=head1 REVISION HISTORY

=cut

################################################################################
# s4pm_prepare_run.pl,v 1.6 2008/03/13 14:22:49 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use File::Basename;
use S4P::PDR;
use S4P::PCF;
use S4P::PCFEntry;
use S4PM::Algorithm;
use S4PM;
use S4P::TimeTools;
use S4P::OdlTree;
use Time::gmtime;
use Getopt::Long;
use POSIX;
use strict;
use Env;
use Safe;

################################################################################
# Global variables                                                             #
################################################################################

use vars qw($OUTPUTWORKORDER
            $PARSE
            $PGE
);

$PARSE = undef;

$PGE = undef;

################################################################################
# Local variables                                                              #
################################################################################

my ($InputWorkorder, $ConfigFile);

################################################################################

# Read in configuration file
 
GetOptions("config=s" => \$ConfigFile,
           "parse"    => \$PARSE,
           "pge=s"    => \$PGE,
);
 
unless ( $PGE ) {
    S4P::perish(30, "main: No algorithm specified with the -pge argument.");
}

my $cfile = ( $ConfigFile ) ? $ConfigFile : "../s4pm_prepare_run.cfg";
if (-f $cfile) {
    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_process_lun', '@cfg_AIRS_L0_luns', '%cfg_process_criteria');
    $compartment->rdo($cfile) or 
        S4P::perish(30, "main: Cannot read configuration file $cfile in safe mode: $!");
    S4P::logger("INFO", "main: Starting s4pm_prepare_run.pl with this configuration file: $cfile");
}
else {
    S4P::logger('WARN', "main: Configuration file $cfile not found");
}

if ( $PARSE ) {
    S4P::logger("INFO", "main: Parsing of PSPEC input files is ENABLED.");
} else {
    S4P::logger("INFO", "main: Parsing of PSPEC input files is DISABLED.");
}

# Verify that an input work order (last command-line argument) has been
# specified and that it exists. Note that GetOptions (above) will remove
# from ARGV only those arguments it recognizes and leave the remaining in
# place. Since the input work order is not "recognized", it is guaranteed
# to be the only remaining argument and will thus be the first: $ARGV[0]
 
if ( !defined $ARGV[0] ) {
    S4P::perish(10, "main: No input work order specified! Must be first argument and file name of the input work order");
} else {
    $InputWorkorder = $ARGV[0];
    unless ( -e $InputWorkorder ) {
        S4P::perish(21, "main: Work order $InputWorkorder doesn't seem to exist!");
    }
    if ( $InputWorkorder =~ /^DO\.(.+)_$PGE\./ ) {
        $OUTPUTWORKORDER = $InputWorkorder;
        $OUTPUTWORKORDER =~ s/$1/ALLOCATE/;
        $OUTPUTWORKORDER =~ s/^DO\.//;
        $OUTPUTWORKORDER .= ".pcf";
    } else {
        S4P::perish(22, "main: Failed to parse input work order name: [$InputWorkorder]!");
    }
}

S4P::logger("INFO", "********** s4pm_prepare_run.pl starting for algorithm $PGE **********");

# Read in algorithm configuration data into a Algorithm object. The 
# configuration information comes from the Select Data configuration files.

my $specifydata_config = "../s4pm_select_data_" . $PGE . ".cfg";

my $algorithm = new S4PM::Algorithm($specifydata_config);
if ( ! $algorithm ) {
    S4P::perish(30, "main: Could not create new S4PM::Algorithm object from reading $specifydata_config");
}

# Read in input PDR (work order)

S4P::logger("INFO", "main: Work order: $InputWorkorder for algorithm: $PGE");
my $pdr;
if ($InputWorkorder) {
    S4P::logger("INFO", "main: Reading input work order: $InputWorkorder");
    $pdr = new S4P::PDR('text' => S4P::read_file($InputWorkorder));
    S4P::perish(23, "main: Input PDR work order is empty: $InputWorkorder") if ! $pdr;
} else {
    S4P::logger("FATAL", "main: Cannot read work order: $InputWorkorder");
}
S4P::logger("INFO", "main: Finish reading work order: $InputWorkorder");

my $data_missing = 0;   # Flag for if required data is missing
my @file_missing = ();	# Array to hold missing input data
my %lun_map      = ();  # Hash storing PCF entry fields for each LUN and each start 
                        # time within that LUN

my $now = S4P::TimeTools::CCSDSa_Now;
my $prod_time = S4P::TimeTools::CCSDSa2yyyydddhhmmss($now);

S4P::logger("INFO", "main: Time: $prod_time");
S4P::logger("INFO", "main: Processing_start: ". $pdr->processing_start);
S4P::logger("INFO", "main: Processing_stop: " . $pdr->processing_stop);

my $pspec_file = undef;

# Change algorithm info if needed

$algorithm->update_info($pdr->processing_start) ;

# Loop through input file groups in the PDR

S4P::logger("INFO", "main: Beginning scan of input PDR...");
foreach my $file_group ( @{ $pdr->file_groups } ) {
    my @fields;
    my $metfile;

### Loop over each file spec

    foreach my $file_spec ( @{ $file_group->file_specs } ) {

        S4P::logger("INFO", "main: directory: " . $file_spec->directory_id . ", file name: " . $file_spec->file_id);

        my ($file, $filename, $field);

        $filename = $file_spec->directory_id . "\/" . $file_spec->file_id;

        if ( $file_group->data_type eq "PSPEC" ) {
            $pspec_file = $filename;
        }

####### If data was NOT found by Find Data ...

        if ( $file_spec->directory_id =~ /INSERT/i or $file_spec->file_id =~ /(INSERT|NOT_AVAILABLE)/i ) {
            if ( $file_group->need =~ /REQ/ ) {
                $data_missing = 1;
                push(@file_missing, $filename);
                S4P::logger("ERROR", "main: Required input file: $filename for LUN: " . $file_group->lun . " is missing. ACTION: Verify file is missing. Check to see if it was cleaned out by clean_expire.data.pl. Look for a DELETE of this granule in the Track Data transaction log.");
            } elsif ( $file_group->need =~ /OPT/ ) {
                S4P::logger("INFO", "main: Optional input file: $filename for LUN: " . $file_group->lun . " is missing.");
            } else {
                S4P::perish(32, "main: I do not recognize a need of " . $file_group->need ." for LUN: " . $file_group->lun . ". Valid needs are REQn and OPTn, where n = 1, 2, 3,...");
            }

        } else { # If data WAS found by Find Data ...

            if ($filename =~ /\.met$/) {
                $metfile = $filename;
            } else {

############### Note that $field below is constructed with pipe (|) characters, 
############### but don't confuse these pipe characters with those used in the 
############### actual PCF. Here, they're just a matter of convenience. In 
############### other words, the $field string will not be copied into the 
############### output PCF directly.

                $field = $file_spec->file_id . "|" . $file_spec->directory_id . "|" . $file_group->ur;
                if ( ! -e $filename ) {
                    if ( $file_group->need =~ /REQ/ ) {
                        $data_missing = 1;
                        push(@file_missing, $filename);
                        S4P::logger("ERROR", "main: Required input file: $filename for LUN: " . $file_group->lun . " is missing. ACTION: Verify file is missing. Check to see if it was cleaned out by clean_expire.data.pl. Look for a DELETE of this granule in the Track Data transaction log.");
                    } else {
                        S4P::logger("INFO", "main: LUN: " . $file_group->lun . " -- Optional input file: $filename is missing.");
                    }
                } else {
                    push(@fields, $field);
                    S4P::logger("INFO", "main: LUN: " . $file_group->lun . " -- Input file: $filename exists.");
                }
            } # End if file is a metadata file or not
        }  # End if data was or was not found by Gran Find
    }  # End loop over each file spec of this input file group

### Does this mean that metadata files are pushed "bare" onto the @fields array?

    push(@fields, $metfile) if ($metfile);
    $lun_map{$file_group->lun}{$file_group->data_start} = \@fields;

} # End loop over file groups in input PDR

S4P::logger("INFO", "main: Scan of input PDR is now complete.");

# If any required data were missing (somehow deleted between Find Data and 
# here), then list the missing data and bail out.

S4P::logger("FATAL", "main: THESE DATA WERE MISSING: $data_missing") if $data_missing;
if ( $data_missing ) {
    my $file_string = join(/ /, @file_missing);
    S4P::perish(35, "main: Missing required input file(s): $file_string");
}

# Read in PCF template

my $pcf_template = $algorithm->pcf_path;
S4P::logger("INFO", "main: Reading template PCF file: $pcf_template");

my $pcf = S4P::PCF::read_pcf($pcf_template);
if ( ! $pcf ) {
    S4P::perish(104, "main: Cannot read PCF template: $pcf_template");
}
$pcf->read_file;
$pcf->parse;

# Start with PRODUCT INPUT FILES section

foreach my $lun (keys %lun_map) {
    my @data_starts = sort keys %{$lun_map{$lun}};
    my $num_versions = scalar(@data_starts);

### First version we deal with is the largest one numerically, then we work backwards

    my $version_num = $num_versions;

    foreach my $data_start (@data_starts) {
        my @fields = @{$lun_map{$lun}{$data_start}};
        my $metfile;
        my $num_fields;
        if ( $fields[$#fields] =~ /\.met$/ ) {
            $metfile = $fields[$#fields];
            $num_fields = $#fields;
        } else {
            $num_fields = $#fields + 1;
        }

        my $text = $pcf->text;

####### For multifile granules such as MOD000 (same LUN, different versions), 
####### i.e., if we have only one version from the incoming work order
####### delete entries greater than $num_fields
####### For multi-version LUNs with single-file granules (e.g. AM1EPHN0),
####### delete everything above $num_versions

        if ($num_versions == $version_num) {
            my $prune = ($num_versions > 1) ? $num_versions : $num_fields;
            if ($prune) {
                S4P::logger("INFO", "main: LUN: $lun -- Deleting all version number(s) greater than $prune in PCF entry");
                $text = S4P::PCFEntry::delete('text'    => $text, 
                                         'lun'     => $lun, 
                                         'version' => $prune
                                        );
            }
        }

####### Replace and/or add new entry to PCF if not in template already.

        foreach (sort @fields) {
            next if (/\.met$/);    # The metfile will always be the last element
            my ($fileref, $dir, $ur) = split(/\|/, $_);
            S4P::logger("INFO", "main: LUN: $lun -- Replacing/Adding version $num_fields");
            my $version_id = ($num_versions > 1) ? $version_num : $num_fields;
            my ($base, undef) = fileparse($metfile) if ( $metfile ) ;
            my $use_met = ($algorithm->metadata_from_metfile) ? $base : $fileref;
            $text = S4P::PCFEntry::replace('text'      => $text, 
                                      'lun'       => $lun, 
                                      'fileref'   => $fileref, 
                                      'directory' => $dir, 
                                      'ur'        => $ur, 
                                      'metpath'   => $use_met, 
                                      'version'   => $version_id);
            $num_fields--;
        }
        $version_num--;
        $pcf->text($text);
        $pcf->parse;
    }
}

# Change the product input information:
#
# If a LUN exists in the PCF template, but we have no file in the work order
# for it, its entry is deleted from the PCF we are building.
#
# Static input LUNs must be excluded from the algorithm config file so they
# are not removed at this step

# Go through dynamic input LUNs in the config file

my ($lun);
my @input_luns = $algorithm->get_input_luns;
foreach $lun ( @input_luns ) {

### See if we found that LUN in the input work order

    if (! exists $lun_map{$lun}) {
        S4P::logger("INFO", "main: No files in work order for LUN: $lun, deleting...");

####### Delete entry from the PCF

        my $text = S4P::PCFEntry::delete('text' => $pcf->text, 'lun' => $lun);
        $pcf->text($text);
        $pcf->parse;
    }
}

# Change the product output information

my %output_files = %{$pcf->output_data_files};
my $pgeversion = $algorithm->pge_version;

my $time_interval = $algorithm->product_coverage;

# Determine the number of versions (PCF entries with the same LUN) needed
# This will be equal to the processing period divided by the temporal coverage
# of the output granules (assumed to be all the same).

# NOTE: We assume that this same number applies to all output data types

# For multi-file granules, we apply this as an additional factor in computing
# the number of entries needed. 

my $processing_period = $algorithm->processing_period || 
    S4P::TimeTools::CCSDSa_Diff($pdr->processing_start, $pdr->processing_stop);
    
# Determine number of versions, i.e. number of output granules need to cover
# the PRODUCT_COVERAGE from the select data config file for this algorithm. We
# assume that all output products have the same temporal coverage. This 
# doesn't include (yet) any accounting for multi-file graules.

my $num_versions = ceil( $processing_period/$time_interval);
S4P::logger("INFO", "main: algorithm: $PGE Version: $pgeversion Product Coverage: $time_interval No. PCF Versions (assuming single-file granules only): $num_versions");

# Insert PRODUCT OUTPUT FILES

my %cfg_output = $algorithm->get_datatype_by_output_lun;

my %used_filerefs = ();    # Keep track of used file names
while (my ($lun, $file) = each (%output_files)) {
    S4P::logger("INFO", "main: LUN: $lun -- Replacing file name and directory");

### The template PCF used to be the way to tell S4PM whether or not a 
### particular data type was multi-file. This was done by simply having the
### correct number of entries in the template for that LUN. But now, this 
### is specified in the files_per_granule attribute in the %outputs hash
### in the Stringmaker algorithm configuration file. Thus, we only use
### the number of entries in the template if it is larger than the number
### of entries computed (just in case the algorithm knows what it's doing!)

    my @ofiles = split (/\s+/, $file);
    my $num_ofiles = scalar(@ofiles);

### For a particular LUN, we can only support either a single multi-file 
### granule or multiple granules for the same LUN (i.e. to cover the processing
### period), but not both.

#   if ( $num_ofiles > 1 and $num_versions > 1 ) {
#       S4P::perish(40, "main: Number of files for multi-file granule is $num_ofiles and number needed to cover the processing period is $num_versions for LUN $lun. Both types of multiplicity cannot be supported at the same time.");
#   }
    my $text = $pcf->text;
    my $processing_start = $pdr->processing_start;
    my $shortname = $cfg_output{$lun};
    my @mf = $algorithm->get_parm_by_lun('files_per_granule', $lun);
    my $multifile_factor = ( defined $mf[0] ) ? $mf[0] : 1;
    S4P::logger("INFO", "main: LUN: $lun -- Algorithm: $PGE data type: $shortname");

### Foreach VERSION (in a PCF sense, not data type version) of a given LUN...

    my @dv = $algorithm->get_parm_by_lun('data_version', $lun);
    S4P::logger("DEBUG", "main: lun: [$lun], dataversion: [@dv]");
    my $dataversion = $dv[0];
    my $nf = ( $num_ofiles > $num_versions * $multifile_factor ) ? $num_ofiles : $num_versions * $multifile_factor;
    for (my $i = $nf; $i >= 1; $i--) {

####### Create a file name for the output file. But before using it, we make
####### sure it hasn't already been used. If it has, we simply add 1 to the 
####### production time field. This might give some "strange" times in that
####### field, but it will guarantee that the same file name won't be used
####### more than once.

        my @output_needs = $algorithm->get_parm_by_lun('need', $lun);

####### If the need is not REQ or OPT, then they are tags that need to be 
####### included in the file names since they keep the file names unique. 
####### Below, we assume that if there are multiple files for the same LUN, 
####### they all have the need parameter set to the same value. 

        my $file_tag;
        if ( defined $output_needs[0] and $output_needs[0] !~ /^REQ/ and $output_needs[0] !~ /^OPT/ ) {
            $file_tag = ":" . $output_needs[0];
        } elsif ( $num_ofiles > 1 ) {
            $file_tag = ":" . $i;
        } else {
            $file_tag = "";
        }

####### Get file name for this output file

####### If there is a file tag, it is attached to the data type name 
####### separated by a colon, as in MOD021KM:FLX

        my $fileref;
        my $is_unique = 0;
        until ( $is_unique ) {
            S4P::logger("INFO", "shortname: [$shortname], file_tag: [$file_tag], dataversion: [$dataversion], processing_start: [$processing_start]");
            $fileref = S4PM::make_patterned_filename($shortname . $file_tag, $dataversion, $processing_start, 0);
            S4P::logger("INFO", "Candidate file name is: [$fileref]");
            unless ( exists $used_filerefs{$fileref} ) {
                S4P::logger("INFO", "Candidate file name IS unique.");
                $is_unique = 1;
            } else {
                S4P::logger("INFO", "Candidate file name is NOT unique. I will try another one...");
                sleep(2);
            }
        }
        $used_filerefs{$fileref} = 1;

        S4P::logger("DEBUG", "main: fileref: [$fileref]");
        my $metfile = "$fileref.met";
        if ( $num_ofiles > 1 ) {
            $metfile =~ s/:[0-9]+//;
        }
        S4P::logger("INFO", "\tmain: LUN: $lun -- file: $fileref");
        my $dir_tag = ( $num_ofiles > 1 ) ? "INSERT_MULTIDIRECTORY_HERE" : "INSERT_DIRECTORY_HERE";
        $text = S4P::PCFEntry::replace(
            'text'      => $text, 
            'lun'       => $lun, 
            'fileref'   => $fileref, 
            'directory' => $dir_tag,
            'ur'        => "LGID:$fileref", 
            'metpath'   => $metfile, 
            'version'   => $i,
        );
        $processing_start = S4P::TimeTools::CCSDSa_DateAdd($processing_start, $time_interval) if ( $num_ofiles == 1 );
    }

    $pcf->text($text);
}
$pcf->parse;

my $text = $pcf->text;

# If there is a script associated with this algorithm to process the specialized
# criteria, we commit the current changes to the output work order and then run
# the script on the saved work order. Otherwise, we run the default 
# add_runtime_parms() subroutine.

if ( $PARSE and $pspec_file and exists $CFG::cfg_process_criteria{$PGE} ) {
    S4P::write_file($OUTPUTWORKORDER, $pcf->text);
    my $cmd = $CFG::cfg_process_criteria{$PGE} . " $OUTPUTWORKORDER $pspec_file";
    my ($err_string, $err_number) = S4P::exec_system($cmd);
    if ( $err_string ) {
        S4P::perish(30, "main: Process criteria script: " . $CFG::cfg_process_criteria{$PGE} . " failed with exit code: [$err_number] with this error message: [$err_string]");
    }

} else {

    $text = add_runtime_parms($text, $pspec_file, $algorithm);

    $pcf->text($text);
    $pcf->parse;

# Write output work order

    S4P::logger("INFO", "main: Writing output PCF file: $OUTPUTWORKORDER");
    S4P::write_file($OUTPUTWORKORDER, $pcf->text);
}

# If we are dealing with an AIRS algorithms, we may need to modify the PCF for 
# the peculiarities of the L1A and L2 processing involved. We recognize such 
# algorithms by the algorithm name. All AIRS algorithms begin with 'Ai'.  Or 'Ah'.
# And maybe even 'Aa' in the future.

S4P::logger("DEBUG", "main: This algorithm is [$PGE]");
if ( $PGE =~ /^AiL1A/ ) {
    S4P::logger("INFO", "main: This algorithm is an AIRS L1A algorithm which requires PCF post-processing.");
    rectify_AIRS_L0_entries();
} elsif ( $PGE =~ /^A.L2/ ) {
    S4P::logger("INFO", "main: This algorithm is an AIRS L2 algorithm which requires PCF post-processing.");
    assign_avn_luns();
}

S4P::logger("INFO", "********** s4pm_prepare_run.pl completed successfully! **********");

sub add_runtime_parms {

    my $text = shift;
    my $pspec = shift;
    my $algorithm = shift;

    while (my ($name, $lun) = each (%CFG::cfg_process_lun)) {
        if ($name eq "START") {
            S4P::logger("INFO", "add_runtime_parms(): LUN: $lun -- Replacing start time to " . $pdr->processing_start);
            $text = S4P::PCFEntry::replace('text'  => $text, 
                                      'lun'   => $lun, 
                                      'value' => $pdr->processing_start);
        } elsif ($name eq "STOP") {
            S4P::logger("INFO", "add_runtime_parms(): LUN: $lun -- Replacing stop time to " . $pdr->processing_stop);
            $text = S4P::PCFEntry::replace('text'  => $text, 
                                      'lun'   => $lun, 
                                      'value' => $pdr->processing_stop);
        }
    }
    
    if ( $PARSE and $pspec ) {

####### Read specialized criteria LUN mappings from select data config file

        S4P::logger("INFO", "add_runtime_parms(): Processing specialized criteria...");

        my %special = ();
        if ( defined $algorithm->specialized_criteria ) {
            %special = %{$algorithm->specialized_criteria};
        } else {
            S4P::logger("WARNING", "add_runtime_parms(): No specialized criteria are defined in the select data config file. I'm assuming that this algorithm is getting them directly from the PSPEC file.");
            return $text;
        }

        unless ( -e $pspec ) {
            S4P::perish(10, "add_runtime_parms(): PSPEC file: [$pspec] doesn't seem to exist!");
        }
        my $OdlTree = S4P::OdlTree->new(FILE => $pspec);

        my %criteria_hash = S4P::criteria_hash($OdlTree);
        foreach my $name_and_esdt ( keys %criteria_hash ) {
            my $lun = undef;
            foreach my $key ( keys %special ) {
                if ( is_match($special{$key}, $name_and_esdt) ) {
                    $lun = $key;
                }
            }

            unless ( $lun ) {
                S4P::logger('WARNING', "add_runtime_parms(): No LUN configured for CRITERIA_NAME/data type combination: [$name_and_esdt] in select data config file.");
            }
            my $c_value = $criteria_hash{$name_and_esdt};
            $text = S4P::PCFEntry::replace('text' => $text, 'lun' => $lun, 'value' => $c_value);
        }
    }

    return $text;
}

sub rectify_AIRS_L0_entries {

#  A perl script to rectify AIRS L0 primary/secondary LID entries
#    (workaround to PDPS shortcomings wrt combining Optional + Auxiliary)
#
 
# Operation:
#   Pass list of primary LIDs on command line
#   Determine corresponding secondary LIDs either by calculation or look-up
#   Foreach primary LID:
#    -  do nothing if (PCF contains primary) and (PCF contains secondary)
#    -  do nothing if (PCF contains primary) and !(PCF contains secondary)
#    -  re-assign secondary to primary if !(PCF contains primary) and 
#       (PCF contains secondary)

    S4P::logger("DEBUG", "rectify_AIRS_L0_entries(): Entering rectify_AIRS_L0_entries()");

### Verify we have at least one argument, perish otherwise

    if ($#CFG::cfg_AIRS_L0_luns < 0) {
        S4P::perish(30, "rectify_AIRS_L0_entries(): Specify at least one primary LUN in \@cfg_AIRS_L0_luns in configuration file");
    }

### List of allowed primaries

    my $allowedLIDs = " 404 405 406 407 414 415 416 417 257 259 260 261 262 288 289 290 342 4007 ";

### Rectify PCF entries

    my $Primary   = "" ;
    my $Secondary = "" ;

    S4P::logger("DEBUG", "rectify_AIRS_L0_entries(): List of LUNs from configuration file: @CFG::cfg_AIRS_L0_luns");
    foreach $Primary (@CFG::cfg_AIRS_L0_luns) {
        if ($allowedLIDs !~ /\s$Primary\s/) { 
            S4P::perish(30, "rectify_AIRS_L0_entries(): Rectification of primary LUN $Primary is not allowed");   
        }
    
        if ($Primary == 4007) {
            $Secondary = 4008;
        } else {
            $Secondary = "9". $Primary;
        }

####### Only modify PCF if both LUNs (the xxx and the 9xxx) are there

        my $Pcommand = "grep \"^$Primary\|\" $OUTPUTWORKORDER > /dev/null";
        my $Pstatus = system($Pcommand);

        my $Scommand = "grep \"^$Secondary\|\" $OUTPUTWORKORDER > /dev/null";
        my $Sstatus = system($Scommand);

        if ( ($Pstatus != 0) and ($Sstatus == 0) ) {
            S4P::logger("INFO", "rectify_AIRS_L0_entries(): Moving LUN $Secondary entries to LUN $Primary");
    
            my $NEW_PCF = $OUTPUTWORKORDER . ".tmp";
            my $OLD_PCF = $OUTPUTWORKORDER;

            open(NEWPCF, ">$NEW_PCF") or 
                S4P::perish(110, "rectify_AIRS_L0_entries(): Could not open $NEW_PCF for writing: $!");
            open(OLDPCF, "$OLD_PCF") or
                S4P::perish(100, "rectify_AIRS_L0_entries(): Could not open $OLD_PCF for reading: $!");

            while(<OLDPCF>) {
                s/^$Secondary\|/$Primary\|/;
                print NEWPCF;
            }

            close(NEWPCF) or
                S4P::perish(110, "rectify_AIRS_L0_entries(): Could not close $NEW_PCF: $!");
            close(OLDPCF) or
                S4P::perish(100, "rectify_AIRS_L0_entries(): Could not close $OLD_PCF: $!");

            my $status = system("/bin/mv -f $NEW_PCF $OLD_PCF");
            unless ($status == 0) { 
                S4P::perish(70, "rectify_AIRS_L0_entries(): Could not move modified PCF work order in place of unmodified PCF work order."); 
            }
        }
    }

    return;
}

sub assign_avn_luns {

# A routine to assign AVN granules to PCF LUNs according to
# AIRS L2 algorithm production rule.
#
# Dr. Mike Theobald, Emergent, GDAAC
#
# 12/7/2001  - Initial Version
#

    require "timelocal.pl" ;

    my $inPCF = $OUTPUTWORKORDER ;
    my $outPCF = $inPCF.".new" ;

### Use the PCF Collection DateTime as the basis for the Year/Day-of-Year

    my $DateTime = `grep "^10258|" $inPCF|cut -f3 -d"|"` ;
    chomp($DateTime) ;
    my ($Today, $Yesterday) = get_year_doy($DateTime) ;

    open(PCF,"$inPCF") || die "Failure opening $inPCF." ;
    open(NEWPCF,">$outPCF") || die "Failure opening $outPCF." ;
 
    while(<PCF>)
    {
  
      unless ( /^999040\|/ || /^999041\|/ || /^999042\|/ )
      {
        print NEWPCF ;
        next ;
      }

      chomp() ;

      (my $lun, my $filename) = split /\|/ ;
      (my $esdt, my $ydoy, my $hhmm) = split /\./, $filename ;

      s/^[0-9]+\|/\|/ ;
      s/\|[0-9]+$/\|1/ ;
      my $entry = $_ ;

      $ydoy =~ s/^[a-zA-Z]// ;

      $lun = "" ;

      if ($esdt eq "AVI3_ANH")
      {
        if (($ydoy eq $Yesterday) && ($hhmm eq "1500")) { $lun = 2203 ; }
        if (($ydoy eq $Yesterday) && ($hhmm eq "2100")) { $lun = 2213 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0300")) { $lun = 2223 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0900")) { $lun = 2233 ; }
        if (($ydoy eq $Today) && ($hhmm eq "1500")) { $lun = 2243 ; }
      }
    
      if ($esdt eq "AVI6_ANH")
      {
        if (($ydoy eq $Yesterday) && ($hhmm eq "1500")) { $lun = 2206 ; }
        if (($ydoy eq $Yesterday) && ($hhmm eq "2100")) { $lun = 2216 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0300")) { $lun = 2226 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0900")) { $lun = 2236 ; }
        if (($ydoy eq $Today) && ($hhmm eq "1500")) { $lun = 2246 ; }
      }

      if ($esdt eq "AVI9_ANH")
      {
        if (($ydoy eq $Yesterday) && ($hhmm eq "1500")) { $lun = 2209 ; }
        if (($ydoy eq $Yesterday) && ($hhmm eq "2100")) { $lun = 2219 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0300")) { $lun = 2229 ; }
        if (($ydoy eq $Today) && ($hhmm eq "0900")) { $lun = 2239 ; }
        if (($ydoy eq $Today) && ($hhmm eq "1500")) { $lun = 2249 ; }
      }

##### Skip if we haven't found a usable AVN file

      next if ($lun eq "" ) ;

      $entry = $lun.$entry ;
      print NEWPCF "$entry\n" ;

    }

    close(PCF) ;
    close(NEWPCF) ;

    `mv -f $outPCF $inPCF` ;
 
    exit(0) ;

}

sub get_year_doy {

    my $DateTime = shift;

    (my $Date,my $Time) = split /T/,$DateTime ;
    (my $YY,my $MM,my $DD) = split /-/,$Date ;

    $YY = sprintf("%2.2d",$YY-1900) ;
    $MM = sprintf("%2.2d",$MM-1) ;
    $DD = sprintf("%2.2d",$DD) ;

    (my $hh, my $mm, my $Secs) = split /:/, $Time ;

    $hh = sprintf("%2.2d",$hh) ;
    $mm = sprintf("%2.2d",$mm) ;
    my $ss = sprintf("%2.2d",int($Secs+0.5)) ;

    my @mytime = split / /, "$ss $mm $hh $DD $MM $YY" ;

    my $secTAI70 = timegm(@mytime) ;

### Pad 100 seconds to correct for any leapseconds

#    $secTAI70+= 100 ;
    $secTAI70 = 86400 * int ($secTAI70/86400) ;

# Get Year/Day-of-Year, and format as required.
    ($ss, $mm, $hh, $DD, $MM, $YY, my $wday, my $DOY, my $isdst) = CORE::gmtime($secTAI70) ;
    my $today = sprintf("%4.4d%3.3d",$YY+1900,$DOY+1) ;

### Do the same, but for previous day.

    ($ss, $mm, $hh, $DD, $MM, $YY, my $wday, my $DOY, my $isdst) = CORE::gmtime($secTAI70 - 86400) ;
    my $yesterday = sprintf("%4.4d%3.3d",$YY+1900,$DOY+1) ;

    return($today, $yesterday) ;
}

sub is_match {

    my ($spec, $actual) = @_;

    my @spec_parts   = split(/\|/, $spec);
    my @actual_parts = split(/\|/, $actual);

### First, check to see if the criteria names match. If they don't even
### match, bail out now.

    if ( $spec_parts[0] ne $actual_parts[0] ) { return 0; }

    my @esdt_list = split(/,\s*/, $spec_parts[1]);
    foreach my $esdt ( @esdt_list ) {
        if ( $actual_parts[1] =~ /$esdt/ ) {
            return 1;
        }
    }
 
    return 0;


}
