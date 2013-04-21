=head1 NAME

GdDbi - transparent database utility routines

=head1 SYNOPSIS

use S4PM::GdDbi;

$dbh = S4PM::GdDbi::db_connect([$server], [$mode]);

@granule_rows = S4PM::GdDbi::select_granules($dbh, $columns, $where, $order);

($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_by_datatime($dbh, 
  $esdt, $version_id, $ccsds_start, $ccsds_stop, $ur_srvr, 
  $additional_attributes)

($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_urls_from_mirador($dbh, 
  $esdt, $version_id, $ccsds_start, $ccsds_stop, $ur_srvr, $ra_bbox,
  $additional_attributes)

($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_et_al($dbh, $where, 
  $order, $ur_srvr, $additional_attributes)

=head1 DESCRIPTION

Provides transparent access to database to allow development on Oracle-based
V0 machine to subsequent Sybase-based ECS machine.


=head2 db_connect

Connects to data server database (by default), returning database handle.
By default, it connects to the g0acg01_srvr, but this can be overriden
in the first argument.
Typically, $mode is not needed; it will be appropriately filled in using the
OPS syntax.  

=head2 select_granules

Selects specified from the DsMdGranules database in a 
quasi-database-independent manner with respect to dates.
Dates are returned in the format 'YYYY-MM-DD HH:MM:SS'.

The columns should be specified as in a normal query, separated by commas:
e.g. "dbID, BeginningDateTime, EndingDateTime".

The where clause should be specified as normal but without conversions for
dates.  The format for date literals should be 'YYYY-MM-DD HH:MM:SS'. 
E.g.: "where BeginningDateTime <= 'YYYY-MM-DD HH:MM:SS'".

Each element in the array returned represents one row.  That element is a
references to an array, which holds the returned values for that row.

Example:  
  my @rows = S4PM::GdDbi::select_granules($dbh, 
    'dbID, BeginningDateTime',
    "ShortName='MOD000' and BeginningDateTime <= '2000-09-20 14:00:00'");
  foreach $ra_row(@rows) {
      printf "dbID=%s begin=%s", @{$ra_row};
  }

=head2 get_ur_et_al

get_ur_et_al supports a query for URs based on a supplied where clause
and optional order by clause.
It returns I<references> to:
   - a hash of URs
   - a hash of granule sizes in MB
   - a hash of additional attributes in an anonymous array

The key to the returned hashes is ESDT.Version/Begin/End, where
Begin and End are in "timestamp" format (CCSDSa without the T or Z).

Example:
  my $dbh = db_connect('OPS') or die "Cannot connect to OPS";
  my ($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_et_al($dbh, 
      'MOD000', '', '2000-12-25T00:00:00Z', '2000-12-24T01:59:59Z',
      'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]', 
      'ProductionDateTime, insertTime');

Searches for most recently inserted granule of the highest version of MOD000
matching that time interval.  
The ProductionDateTime is an additional attribute, and would be in 
$rh_add_attrs->{$key}->[0]. insertTime is in $rh_add_attrs->{$key}->[1].

=head2 get_ur_by_datatime

get_ur_by_datatime supports a query for URs based on the ESDT, start
and stop time in CCSDSa format.  See get_ur_et_al for what is returned.

Begin and End are in "timestamp" format (CCSDSa without the T or Z).

The granules returned are the most recent ones for a given version.
If version is not specified, it returns the most recent of the highest version.
Granules marked DeleteFromArchive are screened from the query.

Example:
  my $dbh = db_connect('OPS') or die "Cannot connect to OPS";
  my ($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_by_datatime($dbh, 
      'MOD000', '', '2000-12-25T00:00:00Z', '2000-12-24T01:59:59Z',
      'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]', 
      'ProductionDateTime, insertTime');

Searches for most recently inserted granule of the highest version of MOD000
matching that time interval.  

=head1 BUGS

select_granules returns Sybase dates as YYYY/MM/DD HH:MM:SS, but Oracle dates as
YYYY-MM-DD HH:MM:SS.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# GdDbi.pm,v 1.3 2007/07/19 18:25:14 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::GdDbi;
use Sys::Hostname;
use S4PM;
use S4P;
use S4P::TimeTools;
use Cwd;
use strict;
my $is_sybase;
1;

########################################################################
# db_connect($mode)
#   $mode - mode of the database to connect to (OPS|TS2|TS1)
# ----------------------------------------------------------------------
# db_connect connects to either Sybase or Oracle database using the 
# account specified in the DB_USER environment variable, with password 
# from the DB_PWD environment variable.
# ======================================================================
sub db_connect {
    require DBI;
    my $server = shift || 'g0acg01_srvr';
    my $mode = shift;
    $S4PM::GdDbi::is_sybase = (hostname() =~ /^[egl][03]/);
    my $dbh;
    my $db_user = $ENV{'DB_USER'};
    my $db_pwd = $ENV{'DB_PWD'};
    if ($S4PM::GdDbi::is_sybase) {
        $mode ||= S4PM::mode_from_path();
        $dbh = DBI->connect("dbi:Sybase:server=$server", $db_user, $db_pwd,
            {PrintError => 0});
        if (! $dbh) {
            S4P::logger ('ERROR', "DBI Connection failed: $DBI::errstr\n");
            return undef;
        }
        $mode = 'TS2' if ($mode =~ /DEV/);
        my $use_db = 'use EcDsScienceDataServer1';
        $use_db .= "_$mode" if ($mode ne 'OPS');
        my $rc = $dbh->do($use_db);
        if (! $rc) {
            S4P::logger('ERROR', "Failed to execute 'use database', mode=$mode");
            return undef;
        }
    }
    else {
        my $database = $mode;
        $dbh = DBI->connect("dbi:Oracle:$database", $db_user, $db_pwd,
            {PrintError => 0});
        if (! $dbh) {
            S4P::logger ('ERROR', "DBI Connection failed: $DBI::errstr\n");
            return undef;
        }
    }
    return $dbh;
}
########################################################################
# select_granules($dbh, $columns, $where, $order)
#   $dbh - database handle (required).
#   $columns - string with columns to select(required).  For datetime columns, 
#       DO NOT include the conversion to varchar; that is handled by 
#       select_granules
#   $where - string with where clause (optional).  For datetime columns,
#       DO NOT include the conversion to varchar; that is handled by
#       select_granules
#   $order - string with order by clause (optional).  For datetime columns,
#       DO NOT include the conversion to varchar; that is handled by
#       select_granules.
# ----------------------------------------------------------------------
# select_granules provides routines that can select from the ECS 
# DsMdGranules table implemented in either Oracle or Sybase.
# This primarily involves different datetime/varchar conversions.
# See the prologue for details.
# ======================================================================
sub select_granules {
    require DBI;
    my ($dbh, $columns, $where, $order) = @_;

    # Identify the date attributes in an easy-to-reference hash
    my %date_attrs = (
        'RangeBeginningDate' => 1,
        'RangeEndingDate' => 1,
        'CalendarDate' => 1,
        'lastUpdate' => 1,
        'BeginningDateTime' => 1,
        'EndingDateTime' => 1,
        'insertTime' => 1,
        'ProductionDateTime' => 1,
        'deleteEffectiveDate' => 1);

    # Make differing date substitutions for both select columns and where 
    # clause.  So far only two cases are supported:  Sybase and Oracle.
    if ($S4PM::GdDbi::is_sybase) {
        foreach (keys %date_attrs) {
            $where =~ s/($_\s*[<=>]+\s*)('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d')/$1 convert(datetime,$2)/g;
            $columns =~ s/($_)/convert(varchar,$1,111)+' '+convert(varchar,$1,108)/g;
            $order =~ s/($_)/convert(varchar,$1,111)+' '+convert(varchar,$1,108)/g;
        }
    }
    else {
        foreach (keys %date_attrs) {
            $where =~ s/($_\s*[<=>]+\s*)('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d')/$1 to_date($2,'YYYY-MM-DD HH24:MI:SS')/g;
            $columns =~ s/($_)/to_char($1,'YYYY-MM-DD HH24:MI:SS')/g;
            $order =~ s/($_)/to_char($1,'YYYY-MM-DD HH24:MI:SS')/g;
        }
    }

    # Add a 'where ' in front of the where clause if it doesn't already have one
    $where =~ s/^/where / if ($where && $where !~ /^where/i);

    # Add an 'order by ' in front of the order clause if it doesn't already have one
    $order =~ s/^/order by / if ($order && $order !~ /^order by/i);

    # Put the query together and execute it
    my $select = "select $columns from DsMdGranules $where" ;
    $select .= " $order" if $order;
    my $sth = $dbh->prepare($select);
    if (! $sth) {
        S4P::logger('ERROR', "Failed to prepare query $select: $DBI::errstr");
        return ();
    }
    my $now;
    printf STDERR ("Query prepared at %s\n%s\n", ($now=localtime()), $select) 
        if ($ENV{'OUTPUT_DEBUG'});
    my $rc = $sth->execute;
    printf STDERR ("Query executed at %s\n", ($now=localtime())) 
        if ($ENV{'OUTPUT_DEBUG'});
    if (! $rc) {
        S4P::logger('ERROR', "Failed to execute query $select: $DBI::errstr");
        return ();
    }

    # Fetch the rows
    # Create a new array for each one, or else they will all point to the same
    # info when they are returned.
    my $row;
    my @granules;
    while ($row = $sth->fetchrow_arrayref) {
        my @vals = @{$row};
        push @granules, \@vals;
    }
    printf STDERR ("Granules fetched at %s", ($now=localtime())) if ($ENV{'OUTPUT_DEBUG'});
    $sth->finish;
    return @granules;
}
########################################################################
# get_ur_by_datatime ($dbh, $esdt, $version_id, $ccsds_start, 
#     $ccsds_stop, $ur_srvr)
#   $dbh - database handle (required).
#   $esdt - ESDT ShortName (required).
#   $version_id - ESDT VersionID.  Optional - will return latest version.
#   $ccsds_start - Start of time range in CCSDSa format.
#   $ccsds_stop - End of time range in CCSDSa format.
#   $ur_srvr - Server for construction of UR (See S4PM::dbid2ur)
# ----------------------------------------------------------------------
# get_ur_by_datatime supports a query for URs based on the ESDT, start
# and stop time.  It returns an array of URs, including the most recent
# one for a given version.  If version is not specified, it returns
# the most recent one for the highest version.
# ======================================================================
sub get_ur_by_datatime {
    my ($dbh, $esdt, $version_id, $ccsds_start, $ccsds_stop, $ur_srvr, 
        $add_columns) = @_;

    # Convert times into form recognized by select_granules
    my $start = S4P::TimeTools::CCSDSa2timestamp($ccsds_start);
    my $stop = S4P::TimeTools::CCSDSa2timestamp($ccsds_stop);

    my $esdt_string = "ShortName = '$esdt' ";
    if (ref $esdt) {
        $esdt_string = "ShortName in ('" . join("','", @$esdt) . "')"
    }
    
    # Create Where clause
    my $where =<< "EOF";
$esdt_string and DeleteFromArchive = 'N' and
EndingDateTime between '$start' and dateadd(hh,1,'$stop')
and BeginningDateTime between dateadd(hh,-1,'$start') and '$stop'
EOF
    # Changed to speed performance and use index
    # See S. Kreisler/B. Trivedi for details on how it works

#((BeginningDateTime < '$stop' and EndingDateTime > '$start') or 
# (BeginningDateTime >= '$start' and EndingDateTime <= '$stop'))
#EOF

    # If version_id is specified, add it to where clause
    # Otherwise, we will use the order by clause
    # to take just the latest version
    my $order = 'VersionID, insertTime';
    $where .= " and VersionID = $version_id" if ($version_id);
    return get_ur_et_al($dbh, $where, $order, $ur_srvr, $add_columns);
}

sub get_ur_et_al {
    my ($dbh, $where, $order, $ur_srvr, $add_columns) = @_;

    # Check database connection
    if (! $dbh) {
        S4P::logger('ERROR', 'Database connection is not open');
        return;
    }

    # Set columns
    my $columns = 'dbID, ShortName, VersionID, BeginningDateTime, EndingDateTime, insertTime, SizeMBECSDataGranule';
    $columns .= ", $add_columns" if $add_columns;


    my (%ur, %gransize, $row, $key, %add_attrs);
    # Select granules from database
    foreach $row(select_granules($dbh, $columns, $where, $order)) {
        # Put the granule urs in a hash, keyed on begin/end
        # This causes duplicates to be overwritten by the most recent one
        # in the archive
        my @attrs = @{$row};

        # Trim trailing blanks from ShortName
        $row->[1] =~ s/\s*$//;

        # Compose key
        $row->[3] =~ s/\//-/g;
        $row->[4] =~ s/\//-/g;
        $key = sprintf "%s.%03d/%s/%s", $row->[1], $row->[2], $row->[3], $row->[4];

        # Make UR
        $ur{$key} = S4PM::dbid2ur($ur_srvr, $row->[1], $row->[2], $row->[0]);
        $gransize{$key} = $row->[6];

        # Put in added attributes
        splice(@attrs, 0, 7);
        $add_attrs{$key} = \@attrs if @attrs;
    }
    return (\%ur, \%gransize, \%add_attrs);
}
sub get_urls_from_mirador{
    my ($mirador_url, $esdt, $version_id, $ccsds_start, $ccsds_stop, 
        $ra_bbox, $additional_attributes) = @_;
    my (%ur, %gransize, %add_attrs);

### ECHO MAGIC GOES HERE ###

    return (\%ur, \%gransize, \%add_attrs);
}
