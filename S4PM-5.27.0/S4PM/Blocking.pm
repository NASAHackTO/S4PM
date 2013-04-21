=head1 NAME

Blocking.pm - Perl objects for manipulating Blocks

=head1 SYNOPSIS

use S4P::Blocking;
use S4P::TimeTools;

$status = S4PM::Blocking::MakeBlock($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime) ;

$status = S4PM::Blocking::CheckBlocks($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime) ;

$status = S4PM::Blocking::ClearBlocks($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime) ;

=head1 DESCRIPTION

This module contains tools for manipulating PGE blocks. Currently,
blocks are used in the local catcher station to allow only a single
SELECT_DATA out to daily summary PGEs (e.g., PGE55 and AIRS Browse).
The routines in this module are used in the scripts that local catcher
calls.
A block is a directory of the form: $BlockDir/$CCSDSaStartTime."_".$CCSDSaStopTime/

Examples:  ../Blocks/AiBr_AIRS/2002-07-14T00:00:00Z_2002-07-15T23:59:59Z
           ../Blocks/MoPGE01/2002-07-04T10:00:00Z_2002-07-04T11:59:59Z

=over 4

=item MakeBlock

$status = S4PM::Blocking::MakeBlock($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime) ;

This creates a block in the $BlockDir directory extending from $CCSDSaStartTime
to $CCSDSaStopTime.  Times are in CCSDSa format:  YYYY-MM-DDThh:mm:ssZ

0 is returned if no block already exists and one can be succesfully created.
1 is returned if the block already exists.
If an error is encountered making the block, then other return codes (in
the range 10-20) will be returned.

=item CheckBlocks

$status = S4PM::Blocking::CheckBlocks($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime);

This checks to see if the input times fall within an existing block located 
in $BlockDir. Times are in CCSDSa format:  YYYY-MM-DDThh:mm:ssZ

0 is returned if no block is found.
1 is returned if a block is found.

=item ClearBlocks

$status = S4PM::Blocking::ClearBlocks($BlockDir, $CCSDSaStartTime, $CCSDSaStopTime);

This clears any blocks completely enclosed by the specified time boundaries.
0 is returned unless a hard error is encountered in clearing the block.

=back

=head1 EXAMPLES

 0 = MakeBlock("../Blocks/AiBr_AIRS","2002-07-14T00:00:00Z","2002-07-15T00:00:00Z")

 1 = CheckBlocks("../Blocks/AiBr_AIRS","2002-07-14T13:43:26Z","2002-07-14T13:49:26Z")

 0 = ClearBlocks("../Blocks/AiBr_AIRS","2002-07-14T00:00:00Z","2002-07-15T00:00:00Z") ;

=head1 LIMITATIONS

Times are expected to be in CCSDSs format (YYYY-MM-DDThh:mm:ssZ).

=head1 AUTHORS

Mike Theobald - NASA/GSFC, Code 610.2

=head1 TO DO

Something, I'm sure.

=cut

################################################################################
# Blocking.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::Blocking;
use strict;
use S4P::TimeTools;
require 5.6.0;
1;

################################################################################


sub MakeBlock {

### Inputs: Directory to create a block,
###         Start time string of the form YYYY-MM-DDThh:mm:ssZ
###         Stop time string of the form YYYY-MM-DDThh:mm:ssZ
### Returns: 0 if block created, 1 if block already exists, die otherwise

    my ($BlockDir, $StartTime, $StopTime) = @_;

# Check to see if BlockDir exists; if not create it; die on error

    unless (-d $BlockDir) {
        mkdir($BlockDir,0775) or S4P::perish(1, "Cannot create top-level Blocking Dir $BlockDir: $!");
    }

    my $block = $StartTime."_".$StopTime ;

    if (mkdir("$BlockDir/$block",0775)) {
        return(0) ;
    } else {
        return(1) if (-d "$BlockDir/$block") ;
    }

    S4P::perish(1, "Error creating Block $BlockDir/$block: $!") ;
}


sub CheckBlocks {

### Inputs: Directory containing blocks,
###         Start time string of the form YYYY-MM-DDThh:mm:ssZ
###         Stop time string of the form YYYY-MM-DDThh:mm:ssZ
### Returns: 0 if no block exists, 1 if one does

    my ($BlockDir, $StartTime, $StopTime) = @_;

    my @Blocks = glob("$BlockDir/*") ;
    my $status = 0 ;

    foreach (@Blocks) {

        my ($start,$stop) = split "_" , (split "\/")[-1] ;
        my $diff1 = S4P::TimeTools::CCSDSa_Diff($start,$StartTime) ;
        my $diff2 = S4P::TimeTools::CCSDSa_Diff($StopTime,$stop) ;

        unless (defined($diff1) and defined($diff2)) {
            S4P::perish(10, "Error from S4P::TimeTools::CCSDSa_Diff: $!") ;
        }
        if ( ($diff1 >= 0) and ($diff2 >= 0) ) {
            $status = 1 ;
        }
   }

    return($status) ;
}


sub ClearBlocks {

### Inputs: Directory containing blocks,
###         Start time string of the form YYYY-MM-DDThh:mm:ssZ
###         Stop time string of the form YYYY-MM-DDThh:mm:ssZ
### Returns: 0 if no block exists, 1 if one does

    my ($BlockDir, $StartTime, $StopTime) = @_;

    my @Blocks = glob("$BlockDir/*") ;

    foreach (@Blocks) {

        my ($start,$stop) = split "_" , (split "\/")[-1] ;
        my $diff1 = S4P::TimeTools::CCSDSa_Diff($start,$StartTime) ;
        my $diff2 = S4P::TimeTools::CCSDSa_Diff($StopTime,$stop) ;

        unless (defined($diff1) and defined($diff2)) {
            S4P::perish(10, "Error from S4P::TimeTools::CCSDSa_Diff: $!") ;
        }
        if ( ($diff1 >= 0) and ($diff2 >= 0) ) {
            unless (rmdir) { S4P::perish(10, "Error removing block $_ : $!") ; }
        }
    }

    return(0) ;
}

1;
