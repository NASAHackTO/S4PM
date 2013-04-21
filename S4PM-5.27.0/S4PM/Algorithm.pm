=head1 NAME

Algorithm.pm - object implementing a algorithm configuration

=head1 SYNOPSIS

=for roff
..nf

use S4PM::Algorithm;

$algorithm = S4PM::Algorithm->new($filename);

$algorithm->update_info($start_time);

$algorithm_name = $algorithm->algorithm_name;
$algorithm->pge_name($algorithm_name);

$pge_name = $algorithm->pge_name;
$algorithm->pge_name($pge_name);

$pge_version = $algorithm->pge_version;
$algorithm->pge_version($pge_version);

$algorithm_version = $algorithm->algorithm_version;
$algorithm->pge_version($algorithm_version);

$processing_period = $algorithm->processing_period;
$algorithm->processing_period($processing_period);

$processing_offset = $algorithm->processing_offset;
$algorithm->processing_offset($processing_offset);

$post_processing_offset = $algorithm->post_processing_offset;
$algorithm->post_processing_offset($post_processing_offset);

$pre_processing_offset = $algorithm->pre_processing_offset;
$algorithm->pre_processing_offset($pre_processing_offset);

$processing_start = $algorithm->processing_start;
$algorithm->processing_start($processing_start);

$trigger_coverage = $algorithm->trigger_coverage;
$algorithm->trigger_coverage($trigger_coverage);

$pcf_path = $algorithm->pcf_path;
$algorithm->pcf_path($pcf_path);

$product_coverage = $algorithm->product_coverage;
$algorithm->product_coverage($product_coverage);

$metadata_from_metfile = $algorithm->metadata_from_metfile;
$algorithm->metadata_from_metfile($metadata_from_metfile);

$apply_leapsec_correction = $algorithm->apply_leapsec_correction;
$algorithm->apply_leapsec_correction($apply_leapsec_correction);

$leapsec_datatypes = $algorithm->leapsec_datatypes;
$algorithm->leapsec_datatypes($leapsec_datatypes);

$trigger_datatype = $algorithm->trigger_datatype;

$trigger_version = $algorithm->trigger_version;

@input_groups = @{ $algorithm->input_groups };

@output_groups = @{ $algorithm->output_groups };

@input_luns = $algorithm->get_input_luns;

@output_luns = $algorithm->get_output_luns;

%input_luns = $algorithm->get_datatype_by_input_lun;

%output_luns = $algorithm->get_datatype_by_output_lun;

@parms = $algorithm->get_parm_by_lun($parm, $lun);

%specialized_criteria = %{$algorithm->specilized_criteria};

%file_accumulation_parms = %{$algorithm->file_accumulation_parms};

$start = $algorithm->accumulation_start($data_start);

$coverage = $algorithm->get_coverage_of_trigger;

=head1 DESCRIPTION

The Algorithm object contains configuration information about a algorithm. 
The information includes the algorithm name and version; the processing 
period, the processing offset, the trigger data type, and one or
more AlgorithmIO. Each AlgorithmIO is an object containing information 
about an input or output data type. @input_groups is an array
of input AlgorithmIO; @output_groups is an array of output AlgorithmIO.

=over 4

=item new

Creates a new B<Algorithm> object. The input $filename is the full 
pathname of the Stringmaker algorithm configuration file. Some valids 
checking is done on reading the configuration file. Returns a 
B<Algorithm> object or undef for failure.

=item update_info

Adjusts algorithm info when processing_boundary is either START_OF_MONTH
or START_OF_AIRS_PENTAD:  $processing_period, $product_coverage, and
$trigger_coverage, as these vary by month or pentad.

=item pge_name

Gets or sets the algorithm name. The algorithm name is initially set in the 
Stringmaker algorithm configuration file. 

=item pge_version

Gets or sets the algorithm version. The algorithm version is initially set 
in the Stringmaker algorithm configuration file. 

=item processing_period

Gets or sets the processing period in seconds. The algorithm's processing 
period is initially set in the Stringmaker algorithm configuration file. 

The default is zero. Note that when both the processing period and the
trigger coverage values are set to zero, the number of work orders output and 
the start and end times of the processing period written into the output work 
orders are determined by the start and end times of the data in the SELECT 
input work order and the $product_coverage.

=item post_processing_offset, processing_offset

Gets or sets the post processing offset. The algorithm's post processing 
offset is initially set in the Stringmaker algorithm configuration file. This 
offset is "post" since it is applied AFTER all data times are determined 
against the processing period without the offset. Only when the processing 
start and stop times are written to the output PDR is the post processing 
offset applied. See pre_processing_offset to have the offset applied BEFORE
data times are determined against the processing period.

The default is zero.

=item pre_processing_offset

Gets or sets the pre processing offset. The PGE's pre processing offset is 
initially set in the Stringmaker algorithm configuration file. This offset is 
"pre" since it is applied BEFORE all data times are determined against the 
processing period without the offset. See post_processing_offset to have 
the offset applied AFTER data times are determined against the processing 
period.

The default is zero.

=item processing_start

Gets or sets the processing start. Valids are the same as used with data
boundary (START_OF_DAY, START_OF_HOUR, etc. with or without an offset).
The default is to set the processing start to the start of the trigger
input granule with any offsets included (see pre_processing_offset and
post_processing_offset). This option overrides that default.  

=item trigger_datatype

Gets the trigger data type. The PGE's trigger data type is initially set in 
the Stringmaker algorithm configuration file. 

=item trigger_version

Gets the trigger data type version. The PGE's trigger data type is initially 
set in the Stringmaker algorithm configuration file. 

=item trigger_coverage

Gets or sets the coverage of the trigger data type. The trigger data type's
coverage is initially set in the Stringmaker algorithm configuration file. 

=item pcf_path

Gets or sets the full path to the PCF template. The PCF template path
is initially set in the Stringmaker algorithm configuration file. 

=item product_coverage

Gets or sets the product coverage (in seconds) of all PGE products. The 
product coverage is initially set in the Stringmaker algorithm configuration 
file. 

=item metadata_from_metfile

Gets or sets the flag indicating whether the PGE will read metadata from
the metadata files accompanying input data or the input HDF data files
themselvs. This setting is global, i.e. it applies to all input product
for this PGE. It is initially set in the Stringmaker algorithm configuration 
file.

=item apply_leapsec_correction

Gets or sets the flag indicating whether a leap second correction is to
be applied to the start and stop times (LUNs 10258 and 10259) in the
runtime PCF. It is initially set in the Stringmaker algorithm configuration 
file.

=item leapsec_datatypes

Gets or sets the list of data types in which the leap second and AIRS instrument
corrections are to be applied to the start and stop data times. It is initially
set in the Stringmaker algorithm configuration file.

=item input_groups

Returns an array AlgorithmIO for input data types.

=item output_groups

Returns an array AlgorithmIO for output data types.

=item get_input_luns

Returns an array of the input LUNs. Duplicate LUNs are removed from list
first.

=item get_output_luns

Returns an array of the output LUNs. Duplicate LUNs are removed from list
first.

=item get_datatype_by_input_lun

Returns a hash where the keys are input LUNs and the values are the
corresponding data types.

=item get_datatype_by_output_lun

Returns a hash where the keys are output LUNs and the values are the
corresponding data types.

=item get_parm_by_lun

Returns an array containing the values of parameter $parm for LUN $lun.
Valid values of $parm are those that can be set or retrieved from the
B<AlgorithmIO> object and now include: data_type, data_version, timer, need,
currency, boundary, flow, coverage, and files_per_granule. For example,

@parms = $algorithm->get_parm_by_lun('need', '10501');

will return an array of need values for all inputs/outputs having the
LUN 10501.

=item specialized_criteria

Returns a hash where the keys are LUNs and the values are the specialized
criteria names and data types to which they are associated with separated
by a pipe chcaracter. If no specialized criteria are specified, the return 
value is undef.

=item file_accumulation_parms

Returns a hash where the keys are one of several parameters used to define
the file accumulation production rule and the hash values are the values 
of those parameters. If no file accumulation parameters are specified, the
return value is undef.

=item accumulation_start

Given the start time of some data file, this function uses the trigger
coverage to determine the start time of a data file accumulation window.
The function assumes that any accumulation window is aligned with a minute,
hour, day, or week boundary. This function only has meaning when the
file accumuation production rule is invoked. If the accumulation rule
has not been invoked, this function returns undef.

=item get_coverage_of_trigger

Returns the temporal coverage of the input data type whose need is set
to 'TRIG'. It fails if no such input is found.

=back

=head1 SEE ALSO

L<AlgorithmIO (3)>

=head1 AUTHORS

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# Algorithm.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::Algorithm;
use Cwd;
use S4PM::AlgorithmIO;
use S4P;
use S4PM;
use Safe;
1;

sub new {

################################################################################
#                                     new                                      #
################################################################################
# PURPOSE: To create a new S4PM::Algorithm object and return a reference to it #
################################################################################
# DESCRIPTION: new creates a new S4PM::Algorithm object, an object used to     #
#       hold algorithm configuration information. A reference to the new       #
#       object is returned.                                                    #
################################################################################
# RETURN: $r_pgeconfig - Reference to the new S4PM::Algorithm object           #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4PM::read_pev_config_file                                            #
#        S4PM::S4PM::AlgorithmIO::new                                          #
################################################################################

    my $pkg      = shift;
    my $filename = shift;

    if ( select_data_cfg_type($filename) eq "old" ) {
        S4P::perish(10, "Configuration file $filename appears to be the old-style Select Data type. You need to use the new style Stringmaker Algorithm configuration file instead.");
    }

    my @input_groups  = ();
    my @output_groups = ();
    my %specialized_criteria = ();
    my %file_accumulation_parms = ();
    my %parms = ();
    my %config_parms = ();
    my $compartment = new Safe 'CFG';

    $compartment->share('$algorithm_name',          '$algorithm_version', 
                        '$processing_period',       '$product_coverage',
                        '$pcf_path',                '$metadata_from_metfile',
                        '$post_processing_offset',  '$pre_processing_offset',
                        '%specialized_criteria',    '$apply_leapsec_correction',
                        '$leapsec_datatypes',       '$processing_start',
                        '%inputs',                  '%outputs',
                        '%file_accumulation_parms', '$trigger_coverage',
                        '%production_summary_parms',
    );
    $compartment->rdo($filename) or die "Cannot import config file $cfg: $!\n";

    if ( defined $CFG::algorithm_name and $CFG::algorithm_name !~ /^\s*$/ ) {
        $config_parms{'algorithm_name'} = $CFG::algorithm_name;
        $config_parms{'pge_name'} = $CFG::algorithm_name;
    } else {
        S4P::logger("FATAL", "S4PM::Algorithm::new: No \$algorithm_name found in config file: $filename");
        return undef;
    }
    if ( defined $CFG::algorithm_version and $CFG::algorithm_version !~ /^\s*$/ ) {
        $config_parms{'algorithm_version'} = $CFG::algorithm_version;
        $config_parms{'pge_version'} = $CFG::algorithm_version;
    } else {
        S4P::logger("FATAL", "S4PM::Algorithm::new: No \$algorithm_version found in config file: $filename");
        return undef;
    }
    if ( $CFG::processing_period ) {
        $config_parms{'processing_period'} = $CFG::processing_period;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$processing_period found in config file: $filename. I'm assuming it is zero.");
        $config_parms{'processing_period'} = 0;
    }
    if ( $CFG::pcf_path ) {
        $config_parms{'pcf_path'} = $CFG::pcf_path;
    } else {
        S4P::logger("FATAL", "S4PM::Algorithm::new: No \$pcf_path found in config file: $filename");
        return undef;
    }
    if ( defined $CFG::trigger_coverage ) {
        $config_parms{'trigger_coverage'} = $CFG::trigger_coverage;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$trigger_coverage found in config file: $filename. I'm assuming it I can get this from an INPUT entry.");
    }
    if ( $CFG::product_coverage ) {
        $config_parms{'product_coverage'} = $CFG::product_coverage;
    } else {
        S4P::logger("FATAL", "S4PM::Algorithm::new: No \$product_coverage found in config file: $filename");
        return undef;
    }
    if ( defined $CFG::metadata_from_metfile ) {
        $config_parms{'metadata_from_metfile'} = $CFG::metadata_from_metfile;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$metadata_from_metfile found in config file: $filename. I'm assuming it is zero.");
        $config_parms{'metadata_from_metfile'} = 0;
    }
    if ( defined $CFG::pre_processing_offset ) {
        $config_parms{'pre_processing_offset'} = $CFG::pre_processing_offset;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$pre_processing_offset found in config file: $filename. I'm assuming it is zero.");
        $config_parms{'pre_processing_offset'} = 0;
    }
    if ( defined $CFG::post_processing_offset ) {
        $config_parms{'post_processing_offset'} = $CFG::post_processing_offset;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$post_processing_offset found in config file: $filename. I'm assuming it is zero.");
        $config_parms{'post_processing_offset'} = 0;
    }
    if ( defined $CFG::processing_start ) {
        $config_parms{'processing_start'} = $CFG::processing_start;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$processing_start found in config file: $filename. I'm assuming it is unset.");
        $config_parms{'processing_start'} = undef;
    }
    if ( defined $CFG::apply_leapsec_correction ) {
        $config_parms{'apply_leapsec_correction'} = $CFG::apply_leapsec_correction;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$apply_leapsec_correction found in config file: $filename. I'm assuming it is zero.");
        $config_parms{'apply_leapsec_correction'} = 0;
    }
    if ( defined $CFG::leapsec_datatypes ) {
        $config_parms{'leapsec_datatypes'} = $CFG::leapsec_datatypes;
    } else {
        S4P::logger("WARNING", "S4PM::Algorithm::new: No \$leapsec_datatypes found in config file: $filename. I'm assuming it is unset.");
        $config_parms{'leapsec_datatypes'} = undef;
    }

    foreach my $key ( keys %inputs ) {

        if ( $inputs{$key}{'need'} =~ /^REQIF/ ) {
            unless ( exists $inputs{$key}{'test'} ) {
                S4P::perish(30, "new(): Data type " . $inputs{$key}{'data_type'} . ", version" . $inputs{$key}{'data_version'} . " has a need of " . $inputs{$key}{'need'} . ", yet no test attribute has been defined.");
            }
        }

        my $aio = new S4PM::AlgorithmIO;
        $aio->data_type( $inputs{$key}{'data_type'} );
        if ( $inputs{$key}{'data_version'} =~ /^[0-9]+$/ ) {
            $aio->data_version( $inputs{$key}{'data_version'} );
        } else {
            $aio->data_version( $inputs{$key}{'data_version'}, "%s" );
        }
        $aio->need( $inputs{$key}{'need'} );
        $aio->timer( $inputs{$key}{'timer'} );
        $aio->lun( $inputs{$key}{'lun'} );
        $aio->currency( $inputs{$key}{'currency'} );
        $aio->coverage( $inputs{$key}{'coverage'} );
        $aio->boundary( $inputs{$key}{'boundary'} );
        $aio->test( $inputs{$key}{'test'} );
        $aio->flow('INPUT');
        push(@input_groups, $aio);
        if ( $inputs{$key}{'need'} eq 'TRIG' ) {
            $config_parms{'trigger_datatype'} = $inputs{$key}{'data_type'};
            unless ( exists $config_parms{'trigger_coverage'} ) {
                $config_parms{'trigger_coverage'} = $inputs{$key}{'coverage'};
            }
        }
    }

    unless ( exists $config_parms{'trigger_datatype'} ) {
        S4P::logger("FATAL", "S4PM::Algorithm::new: No input found with a need of 'TRIG' in config file: $filename");
        return undef;
    }

### Handling for the optional production summary output

    if ( exists $CFG::production_summary_parms{'runlog_file'} or
         exists $CFG::production_summary_parms{'pcf_file'} or 
         exists $CFG::production_summary_parms{'logstatus_file'} ) {
        my $aio = new S4PM::AlgorithmIO;
        if ( exists $CFG::production_summary_parms{'data_type'} ) {
            $aio->data_type( $CFG::production_summary_parms{'data_type'} );
        } else {
            $aio->data_type($CFG::algorithm_name . "_PS");
        }
        if ( exists  $CFG::production_summary_parms{'data_version'} ) {
            if ( $CFG::production_summary_parms{'data_version'} =~ /^[0-9]+$/ ) {
                $aio->data_version( $CFG::production_summary_parms{'data_version'} );
            } else {
                $aio->data_version( $CFG::production_summary_parms{'data_version'}, "%s" );
            }
        } else {
            $aio->data_version("001");
        }
        if ( exists $CFG::production_summary_parms{'lun'} ) {
            $aio->lun( $CFG::production_summary_parms{'lun'} );
        } else {
            $aio->lun("90909");
        }
        $aio->currency("CURR");
        $aio->coverage( $CFG::product_coverage );
        $aio->boundary("START_OF_DAY");
        $aio->timer(0);
        $aio->need('REQ');
        $aio->flow('OUTPUT');
        push(@output_groups, $aio);
    }

    foreach my $key ( keys %outputs ) {
        my $aio = new S4PM::AlgorithmIO;
        $aio->data_type( $outputs{$key}{'data_type'} );
        if (  $outputs{$key}{'data_version'} =~ /^[0-9]+$/ ) {
            $aio->data_version( $outputs{$key}{'data_version'} );
        } else {
            $aio->data_version( $outputs{$key}{'data_version'}, "%s" );
        }
        $aio->need( $outputs{$key}{'need'} );
        $aio->timer( $outputs{$key}{'timer'} );
        $aio->lun( $outputs{$key}{'lun'} );
        $aio->currency( $outputs{$key}{'currency'} );
        $aio->coverage( $outputs{$key}{'coverage'} );
        $aio->boundary( $outputs{$key}{'boundary'} );
        if (  exists $outputs{$key}{'files_per_granule'} and 
               $outputs{$key}{'files_per_granule'} > 1 ) {
            $aio->files_per_granule( $outputs{$key}{'files_per_granule'} );
        } else {
            $aio->files_per_granule(1);
        }
        $aio->flow('OUTPUT');
        push(@output_groups, $aio);
    }

    foreach my $key ( keys %config_parms ) {
       $parms{$key} = $config_parms{$key};
    }

    $parms{'input_groups'}  = \@input_groups;
    $parms{'output_groups'} = \@output_groups;
    if ( %CFG::specialized_criteria ) {
        $parms{'specialized_criteria'} = \%CFG::specialized_criteria;
    } else {
        $parms{'specialized_criteria'} = undef;
    }
    if ( %CFG::file_accumulation_parms ) {
        $parms{'file_accumulation_parms'} = \%CFG::file_accumulation_parms;
    } else {
        $parms{'file_accumulation_parms'} = undef;
    }

    my $r_pgeconfig = \%parms;
    bless $r_pgeconfig, $pkg;

    return $r_pgeconfig;
}

sub update_info {
    my $this = shift ;
    my $start = shift ;

    if ($this->{'processing_start'} eq "START_OF_MONTH") {
        my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($start,32*86400) ;
        $nextmonth = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
        my $numdays = S4P::TimeTools::CCSDSa_Diff($start,$nextmonth) ;
        $this->{'processing_period'} = $numdays ;
        $this->{'product_coverage'} = $numdays ;
        $this->{'trigger_coverage'} = $numdays ;
    }
    elsif ($this->{'processing_start'} eq "START_OF_AIRS_PENTAD") {
        my $nstart = S4P::TimeTools::CCSDSa_DateRound($start,86400) ;
        my ($year, $month, $day, $hour, $min, $sec, $error) = S4P::TimeTools::CCSDSa_DateParse($nstart) ;
        my $numdays = 5*86400 ;
        if ($day >= 25) {

            my $nextmonth = S4P::TimeTools::CCSDSa_DateAdd($nstart,7*86400) ;
            $nextmonth = S4P::TimeTools::CCSDSa_DateFloor($nextmonth,'month',1) ;
            my $thismonth = S4P::TimeTools::CCSDSa_DateAdd($nextmonth,-86400) ;
            my ($tyear, $tmonth, $tday, $thour, $tmin, $tsec, $terror) = S4P::TimeTools::CCSDSa_DateParse($thismonth) ;
            $numdays = 86400*($tday-25) ;

        }
        $this->{'processing_period'} = $numdays ;
        $this->{'product_coverage'} = $numdays ;
        $this->{'trigger_coverage'} = $numdays ;
    }
}

sub file_accumulation_parms      {my $this = shift; @_ ? $this->{'file_accumulation_parms'} = shift
                                           : $this->{'file_accumulation_parms'}}

sub specialized_criteria      {my $this = shift; @_ ? $this->{'specialized_criteria'} = shift
                                           : $this->{'specialized_criteria'}}

sub input_groups      {my $this = shift; @_ ? $this->{'input_groups'} = shift
                                           : $this->{'input_groups'}}

sub output_groups      {my $this = shift; @_ ? $this->{'output_groups'} = shift
                                           : $this->{'output_groups'}}

sub get_input_luns {

    my $this = shift;
    my @files = @{ $this->input_groups };
    my @luns;
    my %seen = ();
    foreach my $file (@files) {
        push(@luns, $file->lun);
    }

### Remove duplicate LUNs

    my @unique_luns = grep { ! $seen{$_} ++ } @luns;

    return @unique_luns;
}

sub get_output_luns {

    my $this = shift;
    my @files = @{ $this->output_groups };
    my @luns;
    my %seen = ();
    foreach my $file (@files) {
        push(@luns, $file->lun);
    }

### Remove duplicate LUNs

    my @unique_luns = grep { ! $seen{$_} ++ } @luns;

    return @unique_luns;
}

sub get_datatype_by_output_lun {

    my $this = shift;
    my @files = @{ $this->output_groups };
    my %lun;

    foreach my $file (@files) {
        $lun{$file->lun} = $file->data_type;
    }

    return %lun;
}

sub get_datatype_by_input_lun {

    my $this = shift;
    my @files = @{ $this->input_groups };
    my %lun;

    foreach my $file (@files) {
        $lun{$file->lun} = $file->data_type;
    }

    return %lun;
}

sub get_parm_by_lun {

    my $this = shift;
    my ($parm, $lun) = @_;
    my @parms = ();

    my @input_files  = @{ $this->input_groups };
    my @output_files = @{ $this->output_groups };

### Note that we need to have the () after in the lines:
###
###  push(@parms, $io->$parm());
###
### so that Perl recognizes that $parm is to refer to a method

### Also, this does not handle incorrect $parm values sent to it well

    foreach my $io (@input_files) {
        if ( $io->lun eq $lun ) {
            push(@parms, $io->$parm());
        }
    }
    foreach my $io (@output_files) {
        if ( $io->lun eq $lun ) {
            push(@parms, $io->$parm());
        }
    }

    return @parms;
}

################################################################################
#                          Attribute get/set routines                          #
################################################################################
# PURPOSE: To set or get an attribute associated with the Algorithm object     #
################################################################################
# DESCRIPTION: These get and set routines get or set the attribute indiciated  #
#              by the sub name.                                                #
################################################################################
# RETURN: returns the attribute value if used as a get routine                 #
################################################################################
# CALLS: none                                                                  #
################################################################################

sub algorithm_name {my $this=shift; @_ ? $this->{'algorithm_name'}=shift
                                    : $this->{'algorithm_name'}}
sub pge_name {my $this=shift; @_ ? $this->{'pge_name'}=shift
                                    : $this->{'pge_name'}}
sub algorithm_version {my $this=shift; @_ ? $this->{'algorithm_version'}=shift
                                    : $this->{'algorithm_version'}}
sub pge_version {my $this=shift; @_ ? $this->{'pge_version'}=shift
                                    : $this->{'pge_version'}}
sub processing_period {my $this=shift; @_ ? $this->{'processing_period'}=shift
                                    : $this->{'processing_period'}}
sub post_processing_offset {my $this=shift; @_ ? $this->{'post_processing_offset'}=shift
                                    : $this->{'post_processing_offset'}}
sub pre_processing_offset {my $this=shift; @_ ? $this->{'pre_processing_offset'}=shift
                                    : $this->{'pre_processing_offset'}}
sub processing_start {my $this=shift; @_ ? $this->{'processing_start'}=shift
                                    : $this->{'processing_start'}}
sub trigger_coverage {my $this=shift; @_ ? $this->{'trigger_coverage'}=shift
                                    : $this->{'trigger_coverage'}}
sub pcf_path {my $this=shift; @_ ? $this->{'pcf_path'}=shift
                                    : $this->{'pcf_path'}}
sub product_coverage {my $this=shift; @_ ? $this->{'product_coverage'}=shift
                                    : $this->{'product_coverage'}}
sub metadata_from_metfile {my $this=shift; @_ ? $this->{'metadata_from_metfile'}=shift
                                    : $this->{'metadata_from_metfile'}}
sub apply_leapsec_correction {my $this=shift; @_ ? $this->{'apply_leapsec_correction'}=shift
                                    : $this->{'apply_leapsec_correction'}}
sub leapsec_datatypes {my $this=shift; @_ ? $this->{'leapsec_datatypes'}=shift
                                    : $this->{'leapsec_datatypes'}}
sub select_data_cfg_type {

    my $cfg = shift;

    open(CFG, $cfg) or S4P::perish(30, "select_data_cfg_type(): Failed to open Select Data configuration file for read: $cfg: $!");

    $/ = undef;
    my $slurp = <CFG>;
    $/ = "\n";
    if ( $slurp =~ /PGE_NAME/ ) {
        return "old";
    } else {
        return "new";
    }
    close(CFG);
}

sub trigger_datatype {

    my $this = shift;

    my @input_groups = @{ $this->input_groups };
    foreach my $group ( @input_groups ) {
        if ( $group->need eq "TRIG" ) {
            return ($group->data_type);
        }
    }

    S4P::perish(100, "Algorithm::trigger_datatype(): Could not find input data type with need of 'TRIG'");
}

sub trigger_version {

    my $this = shift;

    my @input_groups = @{ $this->input_groups };
    foreach my $group ( @input_groups ) {
        if ( $group->need eq "TRIG" ) {
            return ($group->data_version);
        }
    }

    S4P::perish(100, "Algorithm::trigger_version(): Could not find input data type with need of 'TRIG'");
}

sub get_coverage_of_trigger {

    my $this = shift;

    my @input_groups = @{ $this->input_groups };
    foreach my $group ( @input_groups ) {
        if ( $group->need eq "TRIG" ) {
            return $group->coverage;
        }
    }

    S4P::perish(100, "Algorithm::get_coverage_of_trigger(): Could not find input data type with need of 'TRIG'");
}

sub accumulation_start {

    my $this = shift;
    my $data_start = shift;

    my %intervals = (
        'START_OF_WEEK'   => 604088,
        'START_OF_DAY'    =>  86400,
        'START_OF_12HOUR' =>  43200,
        'START_OF_6HOUR'  =>  21600,
        'START_OF_4HOUR'  =>  14400,
        'START_OF_3HOUR'  =>  10800,
        'START_OF_2HOUR'  =>   7200,
        'START_OF_HOUR'   =>   3600,
        'START_OF_MIN'    =>     60,
        'START_OF_SEC'    =>      1,
    );

### First, test whether the data accumulation rule is being used. The old way
### is if the trigger coverage is less than the coverage set in the input
### entry for that data type (normally, they should be equal).

### The new way is to see if the %file_accumulation_parms hash was set. Here,
### we'll test to see if one of the parameters in that hash exists.

    my %file_accum = %{$this->file_accumulation_parms};
    if ( $this->trigger_coverage > $this->get_coverage_of_trigger or
         exists $file_accum{'window_width'} ) {

####### If we're using the newer %file_accumulation_parms hash, than the
####### the width of the accumulation window is specified as the 'window_width'
####### attribute. Otherwise, it is the regular trigger_coverage parameter.
####### Similarly, with proccessing start. 

        my $triggercoverage;
        my $processingstart;
        my $start = "ERROR" ;

        if ( defined $this->file_accumulation_parms and
            exists ${$this->file_accumulation_parms}{'window_width'} ) {
            $triggercoverage = ${$this->file_accumulation_parms}{'window_width'};
            $processingstart = ${$this->file_accumulation_parms}{'window_boundary'};
        } else {
            $triggercoverage = $this->trigger_coverage;
            $processingstart = $this->processing_start;
        }

        if ( $processingstart eq "START_OF_MONTH" ) {
            $start = S4P::TimeTools::CCSDSa_DateFloor($data_start, "month", 1);
        } elsif ( $processingstart eq "START_OF_AIRS_8DAY" ) {
            my $epoch = "1993-01-01T00:00:00.0Z" ;
            my $diff = S4P::TimeTools::CCSDSa_Diff($epoch,$data_start) ;
            my $ndays = int($diff/86400) - 1 ;
            $start = S4P::TimeTools::CCSDSa_DateAdd($epoch,86400*($ndays+2-($ndays % 8))) ;
        } elsif ( $processingstart eq "START_OF_AIRS_PENTAD" ) {
            my $nstart = $data_start ;
            #my $nstart = S4P::TimeTools::CCSDSa_DateRound($data_start,86400) ;
            #my $offset = S4P::TimeTools::CCSDSa_Diff($data_start,$nstart) ;
            my $nmonth = S4P::TimeTools::CCSDSa_DateFloor($nstart,"month",1) ;
            my $diff = S4P::TimeTools::CCSDSa_Diff($nmonth,$nstart) ;
            my $npent = int($diff/(5*86400)) ;
            #$start = S4P::TimeTools::CCSDSa_DateAdd($nmonth,$npent*5*86400-$offset) ;
            $start = S4P::TimeTools::CCSDSa_DateAdd($nmonth,$npent*5*86400) ;
            $start = S4PM::leapsec_correction($start,1,1) ;
        } else {
            my $steps = 1000000000;
            foreach my $key ( keys %intervals ) {
                if ( $triggercoverage >= $intervals{$key} ) {
                    my $s = $triggercoverage / $intervals{$key};
                   if ( $s < $steps and int($s) == $s ) {
                        $steps = $s;
                    }
                }
            }
            $start = S4P::TimeTools::getNearestTimeBoundary($data_start, $processingstart, $triggercoverage, $steps);
        }


        if ( $start eq "ERROR" ) {
            S4P::perish(30, "Algorithm::accumulation_start(): Failed to determine start accumulation start time.");
        } else {
            return $start;
        }
    } else {
        return undef;
    }

}

