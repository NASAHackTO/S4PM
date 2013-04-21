=head1 NAME

S4PM::AlgorithmIO.pm - Perl module implementing AlgorithmIO object of Algorithm object

=head1 SYNOPSIS

=for roff
.nf

use S4PM::AlgorithmIO;
$aio = new S4PM::AlgorithmIO('text'=>$text);

$aio->data_type($data_type); 
$data_type = $aio->data_type;

$aio->data_version($data_version, [$format]); 
$data_version  = $aio->data_version;

$aio->lun($lun);
$lun = $aio->lun;

$aio->need($need);
$need = $aio->need;

$aio->timer($timer);
$timer = $aio->timer;

$aio->currency($currency);
$currency = $aio->currency;

$aio->boundary($boundary);
$boundary = $aio->boundary;

$aio->coverage($coverage);
$coverage = $aio->coverage;

$aio->test($test);
$test = $aio->test;

$aio->files_per_granule($files_per_granule);
$files_per_granule = $aio->files_per_granule;

$aio->flow($flow);
$flow = $aio->flow;

=head1 DESCRIPTION

B<AlgorithmIO.pm> implements an B<AlgorithmIO> object of a B<Algorithm> object.
An B<AlgorithmIO> is a group of attributes about a particular input or output 
data type for an algorithm. An B<Algorithm> object will contain one or more 
AlgorithmIO objects.

The B<AlgorithmIO> attributes are flow (either INPUT or OUTPUT), data type,
data_version, lun, timer, need, currency, boundary, and coverage.

=head1 SEE ALSO

Algorithm(3)

=head1 AUTHORS

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# AlgorithmIO.pm,v 1.5 2008/03/13 15:36:43 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::AlgorithmIO;

    # The first three are standard for SIPS
    my @attributes = ('coverage', 'need', 'lun', 'timer','currency',
                      'boundary', 'flow', 'data_type', 'data_version',
                      'test', 'files_per_granule');
    1;

sub new {

################################################################################
#                                     new                                      #
################################################################################
# PURPOSE: To create a new S4PM::AlgorithmIO object and return a reference to  #
#          it                                                                  #
################################################################################
# DESCRIPTION: new creates a new S4PM::AlgorithmIO object, typically to be     #
#              used as a component of a Algorithm object. A reference to the   #
#              new object is returned.                                         #
################################################################################
# RETURN: $r_iogroup - Reference to the new S4PM::AlgorithmIO object           #
################################################################################
# CALLS: none                                                                  #
################################################################################

    my $pkg = shift;
    my %params = @_;
    my $value;

    # If text is set, parse it for a AlgorithmIO structure
    if ($params{'text'}) {
        my $text = $params{'text'};
        # Parse IO_GROUP-level attributes
        foreach my $attr (@attributes) {
            $parms{$attr} = 1;
        }
    }
    my $r_iogroup = \%params;
    bless $r_iogroup, $pkg;
    return $r_iogroup;
}

################################################################################
#                          Attribute get/set routines                          #
################################################################################
# PURPOSE: To set or get an attribute associated with the AlgorithmIO object   #
################################################################################
# DESCRIPTION: These get and set routines get or set the attribute indiciated  #
#              by the sub name.                                                #
################################################################################
# RETURN: returns the attribute value if used as a get routine                 #
################################################################################
# CALLS: none                                                                  #
################################################################################

sub data_type        {my $this = shift; @_ ? $this->{'data_type'} = shift
                                           : $this->{'data_type'}}
sub data_version     {my $this = shift;  my ($version, $format ) = @_;
                         $format = "%03d" unless defined $format;
                         (defined $version) ? $this->{'data_version'} = sprintf($format, $version) : $this->{'data_version'}}
sub lun              {my $this = shift; @_ ? $this->{'lun'} = shift
                                           : $this->{'lun'}}
sub flow             {my $this = shift; @_ ? $this->{'flow'} = shift
                                           : $this->{'flow'}}
# Note that timer could be set to 0
sub timer            {my $this = shift; (defined($_[0])) 
                                           ? $this->{'timer'} = shift 
                                           : $this->{'timer'}}
sub need             {my $this = shift; @_ ? $this->{'need'} = shift
                                           : $this->{'need'}}
sub currency         {my $this = shift; @_ ? $this->{'currency'} = shift
                                           : $this->{'currency'}}
sub boundary         {my $this = shift; @_ ? $this->{'boundary'} = shift
                                           : $this->{'boundary'}}
sub coverage         {my $this = shift; @_ ? $this->{'coverage'} = shift
                                           : $this->{'coverage'}}
sub test             {my $this = shift; @_ ? $this->{'test'} = shift
                                           : $this->{'test'}}
sub files_per_granule             {my $this = shift; @_ ? $this->{'files_per_granule'} = shift
                                           : $this->{'files_per_granule'}}
