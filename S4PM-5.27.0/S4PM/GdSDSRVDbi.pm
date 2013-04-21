package S4PM::GdSDSRVDbi;

=pod

=head1 NAME

GdSDSRVDbi- SDSRV database utility routines.The algorithm of the methods are based on 
replace.pl which was developed by Steve Kreisler

=head1 SYNOPSIS

use S4PM::GdSDSRVDbi;

=head1 DESCRIPTION

Check SDSRV database to get supplied granule attributes to see the DeleteFromArchive 
flag which represents that the data is still available in the archive. If the 
DeleteFromArchive flag is 'Y',  then generates querys to find its replacement granule.

=head1 AUTHORS

Yangling Huang

=head1 CREATED

Dec 26, 2003

=cut

################################################################################
# GdSDSRVDbi.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use DBI;

################################################################################
# get_granule
################################################################################
sub get_granule {

  my ($dbh, $dbID) = @_;

  my %row;
  my %rec;
  my $query = qq{
        select dbID,
        ShortName,
        VersionID,
        LocalGranuleID,
	BeginningDateTime,	
        EndingDateTime,
	DeleteFromArchive
        from DsMdGranules
        where dbID = $dbID};

  my $sth = $dbh->prepare($query);
  $sth->execute;
  $sth->bind_columns( \( @row{ @{$sth->{NAME} } } ));

  while ( $sth->fetch ) {
    %rec = %row;
  }
  return \%rec;
}


################################################################################
# get_replacement_queries
################################################################################
sub get_replacement_queries {

  my $dbh = shift;
  my $query_reps;

  my $query_std = qq{
        select dbID,
        ShortName,
        VersionID,
	SizeMBECSDataGranule,
	LocalGranuleID,
	BeginningDateTime,
        EndingDateTime
        from DsMdGranules
        where ShortName = ?
        and VersionID = ?
        and EndingDateTime = ?
        and DeleteFromArchive = "N"
	order by dbID
   };

  my $query_oce = qq{
        select dbID,
        ShortName,
        VersionID,
        SizeMBECSDataGranule,
	LocalGranuleID,
        BeginningDateTime,
        EndingDateTime
        from DsMdGranules
        where ShortName = ?
        and VersionID = ?
        and LocalGranuleID like ?
        and DeleteFromArchive = "N"
        order by dbID
    };

  my $query_attrib = qq{
        select dbID,
        ShortName,
        VersionID,
        SizeMBECSDataGranule,
	LocalGranuleID,
        BeginningDateTime,
        EndingDateTime
        from DsMdGranules,
        DsMdGrStringInfoContent s
        where ShortName = ?
        and VersionID = ?
        and EndingDateTime = ?
        and grStringValue = ?
        and dbID=s.granuleId
        and attributeId in (
            select attributeId
            from DsMdAdditionalAttributes
            where AdditionalAttributeName in ("NodeType","SiteName")
        )
        and DeleteFromArchive = "N"
        order by dbID
    };

  my $query_sorce = qq{
        select dbID,
        ShortName,
        VersionID,
        SizeMBECSDataGranule,
	LocalGranuleID,
        BeginningDateTime,
        EndingDateTime
        from DsMdGranules
        where ShortName = ?
        and VersionID = ?
        and DeleteFromArchive = "N"
        order by dbID
    };

  $query_reps->{std} = $query_std;
  $query_reps->{oce} = $query_oce;
  $query_reps->{attrib} = $query_attrib;
  $query_reps->{sorce} = $query_sorce;
  return $query_reps;
}


################################################################################
# find_replacement_granule
#################################################################################

sub find_replacement_granule {

    my ($oldDbID_rec, $query_reps, $dbh ) = @_;

    my $ShortName = $oldDbID_rec->{ShortName};
    my $VersionID = $oldDbID_rec->{VersionID};
    my $LocalGranuleID = $oldDbID_rec->{LocalGranuleID};
    my $EndDateTime = $oldDbID_rec->{EndingDateTime};

    my ( $identifier,
         $sth,
         %row,
         %lastRow
       );

    if ((($ShortName =~ /^M[OY]/)
          and ($ShortName ne 'MOD000')
          and ($LocalGranuleID =~ /\.ADD/)
          and (length($ShortName)==6))
          or ($ShortName =~ /^M[OY]D021(?:QA|SC)/)) {

        my @parts = split /\./, $LocalGranuleID;
        unless ($ShortName =~ /^M[OY]D021SC/) {
            $identifier = (join '.', ($parts[0],$parts[1],$parts[2])) . '%';
        }
        else {
            $identifier =
              (join '.', ($parts[0],$parts[1],$parts[2],$parts[3],$parts[4])) . '%';
        }

        $identifier =~ s/_/\[_]/g;

        $sth = $dbh->prepare($query_reps->{oce});

        $sth->execute($ShortName,$VersionID,$identifier);

    } elsif ($ShortName =~ /^(?:M[OY]D(?:02|OC)CL[12]|SWF.*|AIR.*(?:BR|SD))$/) {
        if ($ShortName =~ /^AIR/) {
            $identifier = ($LocalGranuleID =~ /\.A\./) ?
              "Ascending" : "Descending";
        }
        else {
            (undef,undef,$identifier) = split /\./, $LocalGranuleID;
        }

        $sth =  $dbh->prepare($query_reps->{attrib});

        $sth->execute($ShortName,$VersionID,$EndDateTime,$identifier);

    } elsif ($ShortName =~ /^SOR3TSI/) {
        $sth = $dbh->prepare($query_reps->{sorce});

        $sth->execute($ShortName,$VersionID);

    } else {
        $sth = $dbh->prepare($query_reps->{std});

        $sth->execute($ShortName,$VersionID,$EndDateTime);
    }

    $sth->bind_columns( \( @row{ @{$sth->{NAME} } } ));

    while ( $sth->fetch ) {
        %lastRow = %row;
    }

    return \%lastRow;;
}

1;



