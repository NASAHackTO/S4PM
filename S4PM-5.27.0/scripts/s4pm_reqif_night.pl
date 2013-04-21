#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_reqif_night.pl - test on whether file is a night file or not

=head1 SYNOPSIS

s4pm_reqif_night.pl
I<datafile>

=head1 DESCRIPTION

The B<s4pm_reqif_night.pl> script is intended to be used with the required-if
production rule. The script examines the metadata file associated with the
data file passed in as an argument. If the attribute DayNightFlag is set to
"Night", the script returns 1. Otherwise, it returns 0. 

In order to use this script to support the required-if production rule, the
Stringmaker Algorithm configuration file must be set up first. The data type
on which the test is to be run needs to have the 'need' attribute in the
%inputs hash set to 'REQIF' and the 'test' attribute must be set to point
to this script. Include the path to the script if the script is not locatable
via the PATH environment variable.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_reqif_night.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;

my $datafile = $ARGV[0];
my $metfile = $datafile . ".met";

my $tree = S4P::OdlTree->new(FILE => $metfile);
my @nodes1 = $tree->search(NAME => 'DAYNIGHTFLAG');
my @nodes2 = $tree->search(NAME => 'DayNightFlag');
my @nodes = (@nodes1, @nodes2);
unless ( @nodes ) { exit 0; }
foreach my $node ( @nodes ) {
    my $n = $node->getAttribute('VALUE');
    $n =~ s/"//g;	# Remove quotes
    if ( $n =~ /^night$/i ) {
        exit 1;
    }
}
exit 0;


