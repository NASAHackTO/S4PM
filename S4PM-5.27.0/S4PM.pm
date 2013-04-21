=head1 NAME

S4PM - general routines to support S4PM scripts

=head1 SYNOPSIS

use S4PM;

$type = S4PM::select_data_cfg_type($filename);

$ur = S4PM::dbid2ur($ur_server, $short_name, $version_id, $dbID);

($ur_server, $short_name, $version_id, $dbID)=S4PM::ur2dbid($ur);

%config_parms = S4PM::read_pev_config_file($filename);

$new_timedate = S4PM::leapsec_correction($old_datetime, $apply_instr_offset, $direction);

($box, $mode, $instance, $gear) = S4PM::get_box_mode_instance_gear();

$status = S4PM::submit_trouble_ticket($mode, $userid, $ar_machine, 
    $new_tt_parms, $update_tt_parms);

$space = S4PM::free_disk($path, $datatype, $database, $rh_pool, 
             $rh_pool_map, $rh_proxy, $rh_maxsize, 
             $use_actual_size, $actual_size);

$datatype = S4PM::get_datatype_if_proxy($datatype, $rh_proxy_hash);

($datatype, $version, $dbid) = S4PM::parse_ur_file($ur_filename);

$mode = S4PM::mode_from_path();

$filename = S4PM::make_patterned_filename($datatype, $dataver, $date_str, 0, [pattern]);

$glob = S4PM::make_patterned_filename($datatype, $dataver, $date_str, 1, [pattern]);

$glob = S4PM::make_patterned_glob($datatype, $dataver, $date_str);

@globs = S4PM::make_patterned_glob($datatype, $dataver, $date_str, @range_parms);

($datatype, $dataver, $date_str, $ext) = 
    S4PM::parse_patterned_filename($filename, [$pattern]);

($esdt, $platform, $begin, $version, $prod_time, $suffix) = S4PM::parse_s4pm_filename($filename);

$filename = S4PM::make_s4pm_filename($esdt, $platform, $begin_ccsds, $version, 
                                      $production_ccsds, $suffix);

$platform = S4PM::infer_platform($esdt);

$has_datetime = S4PM::filename_pattern_has_datetime($pattern);

=head1 DESCRIPTION

=over 4

=item parse_s4pm_filename 

Splits up a S4PM filename into its constituent parts.
The times are returned without any delimiters as YYYYDDDHHMM[SS].

=item make_s4pm_filename

This is almost the reverse of parse_s4pm_filename, except that it takes the 
input date/times in typical CCSDSa format (YYYY-MM-DDTHH:MM:SSZ), applying the 
necessary conversions in forming the filename.

=item dbid2ur

converts ECS DsMdGranules.dbID to a UR.
The first argument is the initial segment of the UR describing the server,
e.g., UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]

=item ur2dbid

converts an UR to ECS DsMdGranules.dbID.
returns initial segment, short name, version number, dbiID

=item submit_trouble_ticket

Automates submitting and optionally updating a Trouble Ticket (TT) in the
Remedy system using the DUE ardriver. Inputs are the userid in whose name the
TT will be submitted, the ECS mode which specifies which ardriver is used,
and two strings containing the responses to the interactive ardriver, one for
submitting a new TT and the other for updating the same TT. Refer to ardriver
documentation for the responses needed.

=item free_disk

Frees disk from disk allocation pool. 
If the $actual_size parameter is 0, maxsize from a configuration file is used.
If the $actual_size parameter is 1, the actual size of the file or set of
files is used. For this case, the actual size may be passed in as an argument
just in case the file has already been deleted from disk, as is the case
with s4pm_sweep_data.
If the $actual_size parameter is 2, the difference between the maxsize and the
actual size on disk is used.  This is called by s4pm_register_data.pl.
Thus for input files, two deallocations from the original maxsize allocations
are done:  the first is to deallocate "unused" space when we determine that
the actual file is smaller than the maxsize.  The second is to deallocate
the rest of that allocation, i.e., the actual size itself, when the file is removed.

=item read_pev_config_file

Read in a parameter=value, non-Perl syntax configuration file into a
hash and returns that hash

=item get_datatype_if_proxy

Given a data type and a reference to the %proxy_esdt_map (in the 
allocdisk.cfg file), this subroutine returns the data type which is a proxy
for the input. If there is no proxy, the input data type is returned.
This subroutine should be used if there is a chance that the data type
is included in the %proxy_esdt_map hash of the allocdisk.cfg file.

=item parse_ur_file

This routine takes the UR file (.ur) and retrieves the data type, data version,
and database ID. If the UR file contains a LocalGranuleID and not a proper
UR, the database ID is returned as undef. This routine works just as well
on a request stub file since it contains a UR as well.

=item leapsec_correction

AIRS needs their DPR Start/Stop times to align on integral 360 second
boundaries relative to 1/1/1958.  Such boundaries do not coincide with
on-hour UTC boundaries, owing to the fact that UTC has been adjusted for
leap seconds.
 
The routine is generalized to accept any granule width, not just 360 seconds.

If $apply_instr_offset is set to 1, an additional two seconds for the AIRS
instrument offset is included (as a subtraction). If set to 0, this addition
offset is not applied.

If $direction is set to -1, the corrections are reversed.
 
This routine returns the TAI58-mod-granule_width-aligned time that is less
than or equal to the supplied time.
 
- Convert input CCSDSa time to (year,month,day,hour,min,sec)
  using S4P::TimeTools::CCSDSa_DateParse
- Look up leap second correction for the given year/month/day
- Convert TimeTools values to Time::timegm values
- Convert input CCSDSa time to seconds-since-1/1/1970 using Time::timegm
- Calculate granule-width aligned seconds
- Convert back to timegm values for correction using a call to gmtime
- Convert back to TimeTools format
- Return CCSDSa time that's been aligned to granule-width seconds since TAI58.
 
This routine will only consider times greater than Jan/1/1972.
Earlier times will be treated as errors.
 
=item mode_from_path

Uses FindBin to getsthe ECS mode from the path of the script that is being run.
Valid values are TS2, TS1 and OPS.  DEV is mapped to TS2.

=item tar_links

Tar up directories/files/links, following symbolic links where encountered.

=back
 
=head2 EXAMPLES
 
 "ERROR" = AIRSLeapFilter("1969-12-15T12:24:34Z",360) ;
 
 "1997-12-15T12:24:03Z" = AIRSLeapFilter("1997-12-15T12:24:34Z",360) ;
 
 "2001-12-15T12:24:02Z" = AIRSLeapFilter("2001-12-15T12:24:34Z",360) ;
 
=head2 LIMITATIONS
 
Limited to times greater than Jan 1 1972.
Must be updated in the future when new leap second corrections are released.
Must have LEAPSEC_DIRECTORY environment variable defined to point to directory
containing toolkit leapsec.dat file (typical directory is
/usr/ecs/OPS/CUSTOM/TOOLKIT/toolkit/database/common/TD).

=over 4

=item infer_platform

Used to guess at platform based on ESDT (e.g. MY = P),
but now everything is A (AM-1, Aqua, Aura, ...)

=item get_box_mode_instance_gear

Retrieves the machine name, ECS mode, S4PM insntace name, and gear of the
current stations directory. Machine name is the output of uname;
ECS mode is one of 'TS1', 'TS2', or 'OPS'; instance is one of 'aqua_airs',
'aqua_modis', or 'terra'; and gear is one of 'reprocessing' or
'forward'.

=item select_data_cfg_type

Returns "old" if Select Data configuration file is the old, pre-5.6.0 type;
returns "new" if the Select Data configuration file is 5.6.0 style or later.

=item make_patterned_filename

Generates a file name or a glob pattern using a pattern made up of format 
specifiers. The format specifiers work similarly to the format specifiers in 
the UNIX 'date' command.  The pattern is typically obtained from an S4PM
configuration file via get_filename_pattern(), unless overridden via an
optional argument.

If the last argument, $glob_flag, is set to zero, a file name is returned.
If the last argument is set to non zero, a file glob pattern is returned
instead. The function make_patterned_glob is an alias for this latter
functionality.

Format specifiers in the pattern consist of either the ^ character or the ~
character followed by a single letter indicating what element the format 
specifier should be replaced with. 

Format specifiers that begin with the ^ character refer to the time and date
of the data. Format specifiers that begin with the ~ character refer to the
current time and date (i.e. a production time).

The following is a list of format specifiers currently supported:

^y or ~y    The 2-digit year
^Y or ~Y    The 4-digit year
^m or ~m    The decimal month (00 - 12)
^b or ~b    The abbreviated month name (Jan, Feb, Mar, ...)
^B or ~B    The full month name (January, February, March, ...)
^d or ~d    The decimal day of month (00 - 31)
^u or ~u    The decimal day of the week (1 - 7) with Monday being 1
^j or ~j    The day of year (000 - 366)
^H or ~H    The hour on a 24-hour clock (00 - 23)
^M or ~M    The minute (00 - 59)
^S or ~s    The second (00 - 59)
      ~N    The current time in the format YYYYjjjHHMMSS. This is equivalent
            to the ~Y~j~H~M~S

^E          The data type name
^V          The data type version

In addition to format specifiers, patterns may contain fixed characters that
will be used in all file names generated in S4PM. The standard MODIS file name
is implemented as: 

^E.A^Y^j.^H^M.^V.~N.hdf

which generates file names like:

MOD021KM.A2004232.0245.004.2005054121453.hdf

Format specifier limitations and requirements:

All patterns must include data type name and version (^E and ^V), 
unless an override pattern is specified.

All patterns must specify the year (either ^Y or ^y),
unless an override pattern is specified.

All patterns must specify the month (^m or ^b or ^B) and day (^d) or day of 
year (^j).

A format specifier can only be used once in a pattern.

Zeros will be assumed for time format specifiers left out of the pattern 
(i.e. ^H, ^M, and ^S). Thus, if the pattern you use is ^E.A^Y^j.^V.~N.hdf, 
S4PM will assume that the data time for these files is at 00:00:00. This 
might be perfectly appropriate for daily data.

All file names must be unique. The best way to do this is to make use of
the format specifiers starting with a ~ which uses components of the system
time.

=item parse_patterned_filename

Returns the data type name, version, a date/time string in CCSDS format,
and the file name extension when given a file name and a file name pattern. 
This is the reverse of the make_patterned_filename function.
Typically, the pattern is obtained from an S4PM configuration file, but it
can be overridden by a second argument.

=item make_patterned_glob

Returns a file matching glob pattern. It is equivalent to calling
make_patterned_filename with the last argument, $glob_flag, set to 1.

=item make_patterned_glob_list

Same as make_patterned_glob(), but returns a list of glob patterns instead
determined by the @range_parms argument. The @range_parms contains the
start, end, and increment (in minutes) defining the range of glob patterns
to return.

=item filename_pattern_has_datetime($pattern);

Returns true if pattern has the data date-time in it, false otherwise.

=back

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2
Stephen Berrick, SSAI, NASA/GSFC, Code 610.2
Mike Theobald, Emergent Information Technologies, Inc, NASA/GSFC, Code 610.2

=cut

################################################################################
# -@@@ S4PM, Version Release-5_27_0
# SccsId: 2006/08/07 12:22:19, 1.107
################################################################################

package S4PM;
use strict;
use Safe;
use Sys::Hostname;
use Cwd;
use S4P;
use S4P::TimeTools;
use S4P::ResPool;
use S4PM::Handles;
use File::Basename;
use Time::gmtime;
use Time::Local;
use FindBin;
require Exporter;

use vars qw(@ISA %EXPORT_TAGS @EXPORT_OK @EXPORT $VERSION);
@ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration       use S4PM ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
%EXPORT_TAGS = ( 'all' => [ qw(
        
) ] );

@EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

@EXPORT = qw(
        
);

$VERSION = '5.27.0';

require XSLoader;
XSLoader::load('S4PM', $VERSION);

1;

#==========================================================================
# UR (Universal Reference) routines
#==========================================================================
# dbid2ur($ur_srvr, $short_name, $version_id, $dbid)
#   $ur_srvr: segment of UR describing the server, e.g. 
#       UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]
#   $short_name: ESDT short name
#   $version_id: ESDT version id
#   $dbid:  DsMdGranules.dbID
#--------------------------------------------------------------------------
# Convert a dbID into a UR.
sub dbid2ur {
    my ($ur_srvr, $short_name, $version_id, $dbid) = @_;

    # Make sure version is 3 digits
    my $version = sprintf "%03d", $version_id;

    # Compute length of tail end of UR
    my $len = length($short_name) + length($version) + length($dbid) + 5;

    return sprintf ("%s:%d:SC:%s.%s:%s", $ur_srvr, $len, $short_name, 
        $version, $dbid);
}

sub ur2dbid {
    my $ur=shift;
    $ur =~ /(.*):\d+:SC:(.*)\.(\d\d\d):([^:]*)$/ or
      S4P::logger("WARN","Cannot decipher $ur");
    return ($1,$2,$3,$4);
}

sub read_pev_config_file {

################################################################################
#                          read_pev_config_file                                #
################################################################################
# PURPOSE: To read in parameter=value (non-Perl syntax) configuration file     #
# configuration file in the specify data station                               #
################################################################################
# DESCRIPTION: read_pev_config_file reads in non-Perl syntax parameter=value   #
#              configuration data from a specify configuratino file and returns#
#              the results in a hash whose key is the parameter name.          #
################################################################################
# RETURN: Hash containing the parameters and their values                      #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4PM::trim_whitespace                                                 #
#        S4P::perish                                                           #
################################################################################

    my $filename = $_[0];

    my %ConfigParms = ();

    S4P::logger("DEBUG", "read_pev_config_file: Entering read_pev_config_file()");

    open(CONFIG, "$filename") or 
        S4P::logger("ERROR", "read_pev_config_file: Cannot open $filename: $!");

    while (<CONFIG>) {
        chomp;
        s/#.*//;                        # Get rid of comment lines
        s/^\s+//;			# Trim off leading whitespace
        s/\s+$//;			# Trim off trailing whitespace
        next unless length;             # Anything left?

        my ($var, $value) = split( /\s*=\s*/, $_, 2);
        
        $value =~ s/^\s+//;
        $value =~ s/\s+$//;
        $ConfigParms{$var} = $value;
    }

    close CONFIG or S4P::perish(1, "read_pev_config_file: Cannot close $filename: $!");

    S4P::logger("DEBUG", "read_pev_config_file: Leaving read_pev_config_file()");

    return %ConfigParms;

}
#==================================================================================
# Leapsec routine (TODO: Move to S4P::TimeTools)
#==================================================================================
sub leapsec_correction {

    my $date_str = shift ;
    my $apply_offset = shift ;
    my $direction = shift ;

    if ($direction != -1) { $direction = 1 ; }

    my $instr_offset = -2*$direction;   # Additional AIRS instrument offset (seconds)

    S4P::logger("DEBUG", "leapsec_correction: Before instrument offset, start: [$date_str]");
    if ( $apply_offset ) {
        $date_str = S4P::TimeTools::CCSDSa_DateAdd($date_str, $instr_offset);
        S4P::logger("DEBUG", "leapsec_correction: After instrument offset, start: [$date_str]");
    }

### Return error if year < 1972 (prior to this date, we'd have to do
### interpolation)

    return "ERROR" if (S4P::TimeTools::CCSDSa_DateCompare($date_str,"1972-01-01T00:00:00.0Z") >= 0) ;

### Open TOOLKIT leapsec.dat file and scan in leapsec data

    unless ( exists( $ENV{LEAPSEC_DIRECTORY} ) ) {
        S4P::perish(60, "leapsec_correction: LEAPSEC_DIRECTORY not defined in environment. This environment variable must be set prior to running."); }

    my $leapsec_file = $ENV{LEAPSEC_DIRECTORY} . "/leapsec.dat";

    open(LEAPSEC,"$leapsec_file") or
        S4P::perish(100, "leapsec_correction: Could not open leapsec.dat file $leapsec_file: $!");

    my $leapsec = 0 ;
    my $prev_leapsec = 0 ;

    while (<LEAPSEC>) {
        next unless (/^\s(.*?)\s(.*?)\s+(.*?)\s.*=\s(.*?)\./);

        my $yy  = $1;
        my $mm = 1+(index("JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC",$2))/3;
        my $dd   = $3;
        my $leapsec  = $4;
        my $ldate_str = sprintf("%4.4d-%2.2d-%2.2dT00:00:00.0Z",$yy,$mm,$dd) ;

        last if (S4P::TimeTools::CCSDSa_DateCompare($date_str,$ldate_str) >= 0) ;

        $prev_leapsec = $leapsec;
    }

    $leapsec = $prev_leapsec ;

    close(LEAPSEC) or S4P::perish(100, "leapsec_correction: Could not close leapsec.dat file: $!");

### Add correction in using CCSDSa_DateAdd

    $leapsec = - $direction * $leapsec ;
    $date_str = S4P::TimeTools::CCSDSa_DateAdd($date_str,$leapsec);

    return $date_str;

}

sub get_box_mode_instance_gear {

    my $currdir = cwd();

    my $machine = `/bin/uname -n`;
    chomp($machine);
    if ( $currdir =~ /\/(.+)\/s4pm\/([^\/]+)\/stations\/([^\/]+)/ ) {
        my $modestr = $1;
        my $instance = $2;
        my $gear = $3;
        my $mode;
        if ( $modestr =~ /(TS1|TS2|OPS|DEV|TEST)/i ) {
            $mode = $1;
        }
        return ($machine, $mode, $instance, $gear);
    } else {
        return ('ERROR', 'ERROR', 'ERROR', 'ERROR');
    }
}

sub submit_trouble_ticket {
 
################################################################################
# Submit and optionally, update a Trouble Ticket
#
# Inputs:
#
#    $mode             String containing the ECS mode (TS1, TS2, or OPS). The 
#                      ardriver run will be in the mode specified.
#
#    $ar_machine       The machine to log onto (via ssh) from which to run 
#                      ardriver, e.g. g0mss10
#
#    $new_tt_parms     A string containing the responses needed for a new TT. 
#                      Make sure that newlines are embedded between responses.
#
#    $update_tt_parms  A string containing the responses needed to update the
#                      very same TT as was just created. Use this to immediately
#                      close the TT just opened, for example. Again, make sure
#                      there are newlines at the end of each response. To NOT
#                      update an entered TT, just set this to the empty string,
#                      e.g. $update_tt_parms = "".
#
# Easiest way to set $new_tt_parms or $update_tt_parms is to use a here-to
# document. For example:
#
#    my $new_tt_parms =<<EOF;
#sberrick
#L
#A
#sberrick
#L
#Title of TT
#This is the text of the problem.
#.
#This is the impact of the problem.
#.
#sberrick
#sberrick
#
#EOF
#
# Note the blank line at the end to ensure a newline after the last response.
#
################################################################################

    my ($mode, $userid, $ar_machine, $new_tt_parms, $update_tt_parms) = @_;

### Get hostname

    my $host = hostname();

    my $tmpdir = $ENV{'HOME'} . "/s4pm_cfg/$host/tmp";

    my $tt_open  = "$tmpdir/tt.open.$$";
    my $tt_close = "$tmpdir/tt.close.$$";
    my $home = $ENV{'HOME'};
    my $capture = "$tmpdir/remedy_capture.$$";

    unless ( open(TT, ">$tt_open") ) {
        S4P::logger("ERROR", "Failed to open TT file: $tt_open: $!");
        return -1;
    }

    print TT $new_tt_parms;

    unless ( close(TT) ) {
        S4P::logger("ERROR", "Failed to close TT file: $tt_open: $!");
        return -1;
    }

    my $cmd = "ssh -t -x $userid\@$ar_machine '/tools/gdaac/$mode/bin/CSS/ardriver -v -c < $tt_open' > $capture";
    my ($err_string, $err_number) = S4P::exec_system($cmd);
    if ( $err_number ) {
        S4P::logger("ERROR", "Failed to submit Trouble Ticket. You may try to use this command to do so yourself:\n\tssh -t -x $userid\@$ar_machine '/tools/gdaac/$mode/bin/CSS/ardriver -v -c < $tt_open'");
        return -2;
    }

    if ( $update_tt_parms ne "" ) {
        unless ( open(TT, ">$tt_close") ) {
            S4P::logger("ERROR", "Failed to open TT file: $tt_close: $!");
            return -1;
        }
        print TT $update_tt_parms;

        unless ( close(TT) ) {
            S4P::logger("ERROR", "Failed to close TT file: $tt_open: $!");
            return -1;
        }

        unless ( open(CAPT, "$capture") ) {
            S4P::logger("ERROR", "Failed to open TT capture: $capture: $!");
            return -1;
        }

        my $ttn;
        while ( <CAPT> ) {
            chomp;
            if ( $_ =~ /Entry id\:\s+GSF0+([1-9][0-9]+)/ ) {
                $ttn = $1;
                last;
            }
        }
        unless ( $ttn ) {
            S4P::logger("ERROR", "Submitted TT not found in screen capture.");
            return -2;
        }
        $cmd = "ssh -t -x $userid\@$ar_machine '/tools/gdaac/$mode/bin/CSS/ardriver -u $ttn < $tt_close'";
        my ($err_string, $err_number) = S4P::exec_system($cmd);
        if ( $err_number ) {
            S4P::logger("ERROR", "Failed to update Trouble Ticket. You may use this command to do so yourself:\n\tssh -t -x $userid\@$ar_machine '/tools/gdaac/$mode/bin/CSS/ardrive r -u $ttn < $tt_close'");
            return -2;
        }
    }

    unlink($tt_open);
    unlink($capture);
    unless ( $update_tt_parms eq "" ) { unlink($tt_close); }

    return 0;
}
#===========================================================================
# S4PM Multi-station utilities
#===========================================================================

sub free_disk {
    my ($file, $datatype, $alloc_db, $rh_pool, $rh_pool_map, $rh_proxy, 
        $rh_maxsize, $use_actual_size, $actual_size) = @_;
################################################################################
#                                 free_disk                                    #
################################################################################
# PURPOSE: To deallocate reserved disk resources for a granule                 #
################################################################################
# DESCRIPTION: free_disk frees up allocated disk space for a granule in the    #
#              disk reservation pool.                                          #
################################################################################
# RETURN: None                                                                 #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        S4P::ResPool::read_from_pool                                          #
#        S4P::ResPool::update_pool                                             #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    S4P::logger("DEBUG", "free_disk(): Entering free_disk() with file: [$file] and datatype: [$datatype]");

    my ($size);

    my $proxy_datatype = get_datatype_if_proxy($datatype, $rh_proxy);
    S4P::logger("DEBUG", "free_disk(): proxy_datatype is $proxy_datatype");
    my $actual_pool = get_disk_pool($proxy_datatype, $rh_pool, $rh_pool_map);
    S4P::logger("DEBUG", "free_disk(): actual pool is $actual_pool");

    my $maxsize = $rh_maxsize->{$proxy_datatype};
    unless ( defined S4P::ResPool::read_from_pool($actual_pool, $alloc_db) ) {
        S4P::logger("ERROR", "free_disk(): $actual_pool is not a valid data pool!"); 
        return 0;
    }
    # Using actual sizes
    # $use_actual_size = 0: deallocate maxsize
    # $use_actual_size = 1: deallocate actual_size
    # $use_actual_size = 2: deallocate (maxsize - actual_size)

    if ($use_actual_size) {
        # If $actual_size is not passed in, determine it by looking at file(s)
        $actual_size = S4PM::Handles::get_granule_size($file);
        if ($actual_size) {
            S4P::logger('INFO', "free_disk(): deallocating based on actual size $actual_size");
        } else {
            # Still didn't come up with an actual size, so use maxsize
            if ($use_actual_size == 1) {
                S4P::logger('WARN', "free_disk(): could not find non-zero $file, using maxsize $maxsize");
                $actual_size = $maxsize;
            } else {
                S4P::logger('WARN', "free_disk(): could not find non-zero $file, skipping pool adjustment");
                return 0;
            }
        }
        $size = ($use_actual_size == 2) ? ($maxsize - $actual_size) : $actual_size;
    }
    else {
        $size = $maxsize;
    }
    S4P::logger("INFO", "free_disk(): Releasing $size bytes from pool $actual_pool in the $alloc_db database."); 
    my $space = S4P::ResPool::update_pool($actual_pool, $size, $alloc_db);
    S4P::logger("INFO", "free_disk(): $space bytes were successfully returned to the pool: $actual_pool from the deletion of $file.");
    return $space;
}

sub get_datatype_if_proxy {

    my $datatype = shift;
    my $rh_proxy_hash = shift;

### If data type is in the list of the %proxy_esdt_map, then
### return the ESDT that it's mapped to. Else, return back
### the input ESDT.

    foreach my $key ( keys %$rh_proxy_hash ) {
        my @list = @{$rh_proxy_hash->{$key}};
        foreach my $pattern ( @list ) {
            if ( $datatype =~ /^$pattern$/ ) {
                return $key;
            }
        }
    }

    return $datatype;
}

sub get_disk_pool {
    my ($datatype, $rh_pool, $rh_pool_map, $rh_proxy) = @_;

    # If proxy hash is there, look up the proxy datatype
    # Otherwise, assume no proxy, or proxy already obtained
    my $proxy_datatype = $rh_proxy ? get_datatype_if_proxy($datatype, $rh_proxy) : $datatype;

    my $pool_name = $rh_pool_map->{$proxy_datatype} or
        S4P::perish(30, "get_disk_pool(): Could not find data type: [$datatype] in datatype_pool_map hash");
    my $actual_pool = $rh_pool->{$pool_name} or
        S4P::perish(30, "get_disk_pool(): Could not find key: [$pool_name] in datatype_pool hash");;
    return $actual_pool;
}

sub parse_ur_file {

    my $filename = shift;

    my ($datatype, $version, $dbid);

    open(UR, $filename) or S4P::perish(30, "Failed to open UR file for read: $filename: $!");
    my $str = <UR>;
    close(UR) or S4P::perish(30, "Failed to close UR file: $filename: $!");
    chomp($str);
    if ( $str =~ /^UR:.*:SC:([^.]+)\.([0-9]{3}):([0-9]+)$/ ) {
        $datatype = $1;
        $version  = $2;
        $dbid     = $3;
    } elsif ( $str =~ /^LGID:([^:]+):([0-9]{3}):/ ) {
        $datatype = $1;
        $version  = $2;
        $dbid     = undef;
    } elsif ( $str =~ /^FakeLGID:([^:]+):([0-9]{3}):/ ) {
        $datatype = $1;
        $version  = $2;
        $dbid     = undef;
    } else {
        $datatype = undef;
        $version  = undef;
        $dbid     = undef;
    }

    return ($datatype, $version, $dbid);
}

# mode_from_path():  needed for ECS-style configurations with modes
# TS2, TS1 and OPS

sub mode_from_path {
    # Infer mode from the path of the executable calling us
    my $dir = $FindBin::RealBin;
    my ($mode) = ($dir =~ m#/(DEV|TS2|TS1|OPS|s4pmts1|s4pmts2|s4pmops)/#);
    $mode =~ s/s4pm// ;
    $mode = uc($mode) ;
    $mode = 'TS2' if ($mode eq 'DEV');
    return $mode;
}
sub tar_links {
    my ($tarfile, @files) = @_;
    # Tar up the appropriate files in the directory
    my $osname = `/bin/uname -s`;
    my $tar_args = ($osname =~ /IRIX64/) ? "-Lcf" : "-hcf";
    my $cmd = "tar $tar_args $tarfile " . join(' ', @files) . ' > tar.stderr 2>&1';
    my ($errstr, $errnum) = S4P::exec_system($cmd);
    if ($errnum) {
        S4P::logger("ERROR", "Cannot make tar file $tarfile: $errstr");
        return;
    }

    # Many tar errors do not cause tar to exit non-zero...
    print STDERR "Size of tar.stderr: ", (-s 'tar.stderr'), "\n";
    if (-s 'tar.stderr') {
        system("cat tar.stderr");
        return;
    }
    else {
        unlink 'tar.stderr';
    }
    return 1;
}

sub get_regex {

### Generate a Perl regular expression corresponding to the input file
### name pattern.

    my $pattern = shift;

    my $regex = $pattern;

    $regex =~ s/\./\\./g;

    $regex =~ s/\^y/\(\\d\\d\)/;
    $regex =~ s/\^Y/\(\\d\\d\\d\\d\)/;
    $regex =~ s/\^d/\(\\d\\d\)/;
    $regex =~ s/\^b/\(\[A\-z\]\[A\-z\]\[A\-z\]\)/;
    $regex =~ s/\^H/\(\\d\\d\)/;
    $regex =~ s/\^j/\(\\d\\d\\d\)/;
    $regex =~ s/\^m/\(\\d\\d\)/;
    $regex =~ s/\^M/\(\\d\\d\)/;
    $regex =~ s/\^S/\(\\d\\d\)/;
    $regex =~ s/\^u/\(\\d\)/;
    $regex =~ s/\^B/\(\[A\-z\]\+\)/;

    $regex =~ s/\^E/\(\[\\w\-:]\+\)/;
    $regex =~ s/\^V/\(\[\\.A\-z0-9\]\+\)/;
    $regex =~ s/\^P/\(\\d\+\)/;

    $regex =~ s/~y/\(\\d\\d\)/;
    $regex =~ s/~Y/\(\\d\\d\\d\\d\)/;
    $regex =~ s/~d/\(\\d\\d\)/;
    $regex =~ s/~b/\(\[A\-z\]\[A\-z\]\[A\-z\]\)/;
    $regex =~ s/~H/\(\\d\\d\)/;
    $regex =~ s/~j/\(\\d\\d\\d\)/;
    $regex =~ s/~m/\(\\d\\d\)/;
    $regex =~ s/~M/\(\\d\\d\)/;
    $regex =~ s/~S/\(\\d\\d\)/;
    $regex =~ s/~u/\(\\d\)/;
    $regex =~ s/~B/\(\[A\-z\]\+\)/;
    $regex =~ s/~N/\(\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\)/;

    return $regex;
}

#============================================================================
# FILE NAMING ROUTINES
#============================================================================
sub parse_patterned_filename {

    my ($filename, $override_pattern) = @_;

    my $ext = $filename;
    $ext =~ s/^.*\.//;

    my $pattern  = $override_pattern || S4PM::get_filename_pattern();

    my %short_months = (
        'Jan' => "01", 'Feb' => "02", 'Mar' => "03", 'Apr' => "04",  
        'May' => "05", 'Jun' => "06", 'Jul' => "07", 'Aug' => "08", 
        'Sep' => "09", 'Oct' => "10", 'Nov' => "11", 'Dec' => "12",
    );
    my %long_months = (
        'January' => '01', 'February' => '02', 'March' => '03', 
        'April' => '04', 'May' => '05', 'June' => '06', 'July' => '07', 
        'August' => '08', 'September' => '09', 'October' => '10', 
        'November' => '11', 'December' => '12',
    );

    my ($month, $day, $year, $pmonth, $pday, $pyear);

### Set defaults if these items are not specified in the file name pattern

    my $minute  = '00';
    my $second  = '00';
    my $hour    = '00';
    my $pminute = '00';
    my $psecond = '00';
    my $phour   = '00';

### The %positions hash is for storing the position (starting from 1) of each
### format specifier in the pattern. For example, in the pattern:
###
### ^E.^Y^B-^d.hdf
###
### E is in position 1, Y is 2, B is 3, and d is in position 4

    my %positions = ();

### The %values hash will hold the parsed values for each format specifier
### in the pattern. 

    my %values    = ();

### Fill the %positions hash. This is done by determining the position of each
### format specifier in the file name pattern.

    my $in_format = 0;
    my $count = 1;
    foreach my $char ( split //, $pattern ) {
        if ( $in_format == 1 ) {
            $positions{$char} = $count;
            $count++;
            $in_format = 0;
        } elsif ( $in_format == 2 ) {
            $positions{"~" . $char} = $count;
            $count++;
            $in_format = 0;
        } elsif ( $in_format == 0 and $char eq '^' ) {
            $in_format = 1;
        } elsif ( $in_format == 0 and $char eq '~' ) {
            $in_format = 2;
        } else {
            next;
        }
    }

### Get the regular expression corresponding to the file name pattern

    my $regex = S4PM::get_regex($pattern);

### The regular expression determined above will include groups enclosed in
### parentheses. In such a regex, what matches in the first parenthesis group
### is automatically set to the variable $1, what matches in the second is
### automatically set to the variable $2, etc. This fact, along with the
### %positions hash will allow a mapping between a format specifier and its
### value as determined by the regex.

    foreach my $key ( keys %positions ) {
        no strict 'refs';	# strict doesn't like this bit of code
        my $position = $positions{$key};
        if ( $filename =~ /$regex/ ) {
            $values{$key} = $$position;
        } else {
            return undef;    # Parsing failed
        }
    }

### The year, in one for or another, must be specified in the pattern

    if ( exists $values{'Y'} ) {
        $year = $values{'Y'};
    } elsif ( exists $values{'y'} ) {
        $year = ( $values{'y'} < 70 ) ? $values{'y'} + 2000 : $values{'y'} + 1900;
    }
    if ( exists $values{'~Y'} ) {
        $pyear = $values{'~Y'};
    } elsif ( exists $values{'~y'} ) {
        $pyear = ( $values{'~y'} < 70 ) ? $values{'~y'} + 2000 : $values{'~y'} + 1900;
    }

    if ( exists $values{'j'} ) {
        (undef, $month, $day) = S4P::TimeTools::doy_to_ymd($values{'j'}, $year);
        if ( length($month) == 1 ) { $month = "0" . $month; }
        if ( length($day)   == 1 ) { $day   = "0" . $day; }
    }
    if ( exists $values{'~j'} ) {
        (undef, $pmonth, $pday) = S4P::TimeTools::doy_to_ymd($values{'~j'}, $pyear);
        if ( length($pmonth) == 1 ) { $pmonth = "0" . $pmonth; }
        if ( length($pday)   == 1 ) { $pday   = "0" . $pday; }
    }

### Setting month or day individually trumps setting it as a day of year.

    if ( exists $values{'m'} ) {
        $month = $values{'m'};
    } elsif ( exists $values{'b'} ) {
        $month = $short_months{ $values{'b'} };
    } elsif ( exists $values{'B'} ) {
        $month = $long_months{ $values{'B'} };
    }
    if ( exists $values{'~m'} ) {
        $pmonth = $values{'~m'};
    } elsif ( exists $values{'~b'} ) {
        $pmonth = $short_months{ $values{'~b'} };
    } elsif ( exists $values{'~B'} ) {
        $pmonth = $long_months{ $values{'~B'} };
    }

    if ( exists $values{'d'} ) {
        $day = $values{'d'};
    }
    if ( exists $values{'~d'} ) {
        $pday = $values{'~d'};
    }

    if ( exists $values{'H'} ) {
        $hour = $values{'H'};
    }
    if ( exists $values{'~H'} ) {
        $phour = $values{'~H'};
    }

    if ( exists $values{'M'} ) {
        $minute = $values{'M'};
    }
    if ( exists $values{'~M'} ) {
        $pminute = $values{'~M'};
    }

    if ( exists $values{'S'} ) {
        $second = $values{'S'};
    }
    if ( exists $values{'~S'} ) {
        $psecond = $values{'~S'};
    }

### We need to handle the case where the data type was attached with a file
### tag with a colon in between. The colon and the part afterward are not
### part of the data type name. So, we need to remove it.

    my $dt = $values{'E'};
    if ( $dt =~ /:/ ) {
        $dt =~ s/:.+//;
    }

    my $pdate_str;
    if ( exists $values{'~N'} ) {
        $pdate_str = S4P::TimeTools::yyyydddhhmmss2CCSDSa($values{'~N'});
    } else {
        $pdate_str = $pyear . "-" . $pmonth . "-" . $pday . "T" . $phour .
                   ":" . $pminute . ":" . $psecond . "Z";
    }

    my $date_str = $year . "-" . $month . "-" . $day . "T" . $hour .
                   ":" . $minute . ":" . $second . "Z";

    return ($dt, $values{'V'}, $date_str, $pdate_str, $ext);
}

sub make_patterned_filename {

### Based on pattern, data type, data version, and a CCSDS date string, 
### return a file name.
###
### Patterns are strings containing format specifiers for how the file name
### is to be patterned. The format specifiers are based on those used by the
### UNIX time command format option. 
###
### Format specifiers come in two types, those that begin with the ^ character
### and those that begin with the ~ character. Format specifiers that begin
### with the ^ character refer to data time. Format specifiers that begin
### with the ~ character refer to the current time (same as would be returned
### via the 'date' command on the machine in which this is running).
###
### ^E       The data type name
### ^V       The data type version
### ^y, ~y   The two-digit year
### ^Y, ~Y   The four-digit year
### ^d, ~d   The day of month (00 - 31)
### ^m, ~m   The month (00 - 12)
### ^b, ~b   The abbreviated month name (Jan, Feb, Mar, Apr, etc.)
### ^B, ~B   The long month name (January, February, March, etc.)
### ^j, ~j   The day of year (000 - 360)
### ^H, ~H   The hours on a 24-hour clock (00 - 23)
### ^M, ~M   The minutes (00 - 59)
### ^S, ~S   The seconds (00 - 59)
### ^u, ~u   The day of week (1 - 7) with Monday being 1
###     ~N   The current time in the form YYYYjjjHHMMSS (can be used to make
###          the generated file name unique). This is a shorthand for:
###          ~Y~j~H~M~S

    my $datatype = shift;
    my $dataver  = shift;
    my $date_str = shift;
    my $glob_flag = shift;
    my $override_pattern = shift;

    my $pattern = $override_pattern || S4PM::get_filename_pattern();

    # Skip pattern checks if override_pattern is specified
    unless ($override_pattern) {
        unless ( $pattern =~ /\^E/ and $pattern =~ /\^V/ ) {
            S4P::logger("FATAL", "S4PM::make_patterned_filename(): No data type (^E) and/or version (^V) specified in file name pattern: [$pattern].");
            return "ERROR";
        }
        unless ( $pattern =~ /\^Y/ or $pattern =~ /\^y/ ) {
            S4P::logger("FATAL", "S4PM::make_patterned_filename(): No year (^Y or ^y) specified in file name pattern: [$pattern]");
            return "ERROR";
        }
    }
    $pattern =~ s/\^E/$datatype/g;
    $pattern =~ s/\^V/$dataver/g;
 
    my $now = S4P::TimeTools::CCSDSa_Now;

    if ( $pattern =~ /\^y/ ) { 
        my $x = S4P::TimeTools::CCSDStoy($date_str);
        $pattern =~ s/\^y/$x/g;
    }
    if ( $pattern =~ /\^Y/ ) { 
        my $x = S4P::TimeTools::CCSDStoY($date_str);
        $pattern =~ s/\^Y/$x/g;
    }
    if ( $pattern =~ /\^d/ ) { 
        my $x = S4P::TimeTools::CCSDStod($date_str);
        $pattern =~ s/\^d/$x/g;
    }
    if ( $pattern =~ /\^m/ ) { 
        my $x = S4P::TimeTools::CCSDStom($date_str);
        $pattern =~ s/\^m/$x/g;
    }
    if ( $pattern =~ /\^b/ ) { 
        my $x = S4P::TimeTools::CCSDStob($date_str);
        $pattern =~ s/\^b/$x/g;
    }
    if ( $pattern =~ /\^B/ ) { 
        my $x = S4P::TimeTools::CCSDStoB($date_str);
        $pattern =~ s/\^B/$x/g;
    }
    if ( $pattern =~ /\^H/ ) { 
        my $x = S4P::TimeTools::CCSDStoH($date_str);
        $pattern =~ s/\^H/$x/g;
    }
    if ( $pattern =~ /\^M/ ) { 
        my $x = S4P::TimeTools::CCSDStoM($date_str);
        $pattern =~ s/\^M/$x/g;
    }
    if ( $pattern =~ /\^S/ ) { 
        my $x = S4P::TimeTools::CCSDStoS($date_str);
        $pattern =~ s/\^S/$x/g;
    }
    if ( $pattern =~ /\^j/ ) { 
        my $x = S4P::TimeTools::CCSDStoj($date_str);
        $pattern =~ s/\^j/$x/g;
    }
    if ( $pattern =~ /\^u/ ) { 
        my $x = S4P::TimeTools::CCSDStou($date_str);
        $pattern =~ s/\^u/$x/g;
    }

    if ( $glob_flag ) {
        $pattern =~ s/~[yYdmNMbBHSju]/*/g;
    } else {
        if ( $pattern =~ /~y/ ) { 
            my $x = S4P::TimeTools::CCSDStoy($now);
            $pattern =~ s/~y/$x/g;
        }
        if ( $pattern =~ /~Y/ ) { 
            my $x = S4P::TimeTools::CCSDStoY($now);
            $pattern =~ s/~Y/$x/g;
        }
        if ( $pattern =~ /~d/ ) { 
            my $x = S4P::TimeTools::CCSDStod($now);
            $pattern =~ s/~d/$x/g;
        }
        if ( $pattern =~ /~m/ ) { 
            my $x = S4P::TimeTools::CCSDStom($now);
            $pattern =~ s/~m/$x/g;
        }
        if ( $pattern =~ /~b/ ) { 
            my $x = S4P::TimeTools::CCSDStob($now);
            $pattern =~ s/~b/$x/g;
        }
        if ( $pattern =~ /~B/ ) { 
            my $x = S4P::TimeTools::CCSDStoB($now);
            $pattern =~ s/~B/$x/g;
        }
        if ( $pattern =~ /~H/ ) { 
            my $x = S4P::TimeTools::CCSDStoH($now);
            $pattern =~ s/~H/$x/g;
        }
        if ( $pattern =~ /~M/ ) { 
            my $x = S4P::TimeTools::CCSDStoM($now);
            $pattern =~ s/~M/$x/g;
        }
        if ( $pattern =~ /~S/ ) { 
            my $x = S4P::TimeTools::CCSDStoS($now);
            $pattern =~ s/~S/$x/g;
        }
        if ( $pattern =~ /~j/ ) { 
            my $x = S4P::TimeTools::CCSDStoj($now);
            $pattern =~ s/~j/$x/g;
        }
        if ( $pattern =~ /~u/ ) { 
            my $x = S4P::TimeTools::CCSDStou($now);
            $pattern =~ s/~u/$x/g;
        }
        if ( $pattern =~ /~N/ ) { 
            my ($year, $month, $day, $hour, $min, $sec, $error) =
                S4P::TimeTools::CCSDSa_DateParse($now);
            my $ndoy = S4P::TimeTools::day_of_year($year, $month, $day);
            my $now_str = sprintf("%04d%03d%02d%02d%02d", 
                $year, $ndoy, $hour, $min, $sec);
            $pattern =~ s/~N/$now_str/g;
        }
    }

### Concatenate contiguous groups of * characters into single ones

    if ( $glob_flag ) {
        $pattern .= '*';    # Add * to very end to handle .met, .ur extensions
        $pattern =~ s/\*+/*/g;
    }

    return $pattern;
}

sub make_patterned_glob {

    my $datatype = shift;
    my $dataver  = shift;
    my $date_str = shift;

    my $glob = S4PM::make_patterned_filename($datatype, $dataver, $date_str, 1);
    return $glob;
}

sub make_patterned_glob_list {

    my ($datatype, $dataver, $date_str, @range_parms) = @_;

    my @list = ();

    my $start = $range_parms[0];
    $start = -1 * abs($start);
    my $end   = $range_parms[1];
    $end = abs($end);
    my $inc   = $range_parms[2];

    my $new_time;
    my $count = 0;
    for (my $i = $start*60; $i <= $end*60; $i += ($inc*60)) {
        my $step = $i;
        $new_time = S4P::TimeTools::CCSDSa_DateAdd($date_str, $step);
        $list[$count] = S4PM::make_patterned_filename($datatype, $dataver, $new_time, 1);
        $count++;
    }
    
    return @list;
}

sub get_filename_pattern {

    my $default = "^E.A^Y^j.^H^M.^V.~N.hdf";

    unless ( exists $ENV{'S4PM_CONFIGDIR'} ) {
        S4P::logger("DEBUG", "S4PM::get_filename_pattern(): Environment variable S4PM_CONFIGDIR not set to the global Stringmaker directory. Default file name pattern will be used.");
        return $default;
    }

    my $file = $ENV{'S4PM_CONFIGDIR'} . "/s4pm_stringmaker_datatypes.cfg";

    unless ( -e $file ) {
        S4P::logger("DEBUG", "S4PM::get_filename_pattern(): Configuration file $file doesn't seem to exist! Default file name pattern will be used.");
        return $default;
    }

    my $compartment = new Safe 'CFG';
    $compartment->share('$s4pm_filename_pattern');
    $compartment->rdo($file) or 
        S4P::perish(30, "S4PM::get_filename_pattern(): Failed to import configuration file: $file: $!");

    if ( $CFG::s4pm_filename_pattern eq undef or 
         $CFG::s4pm_filename_pattern =~ /^\s*$/ ) {
        S4P::logger("DEBUG", "S4PM::get_filename_pattern(): File name pattern set in parameter \$s4pm_filename_pattern in $file is unset or blank. Default file name pattern willbe used.");
        return $default;
    }

    return $CFG::s4pm_filename_pattern;
}

# S4PM::make_modis_filename() is obsolete/deprecated

sub make_modis_filename {
    return make_s4pm_filename(@_);
}

# S4PM::make_s4pm_filename() is obsolete/deprecated

sub make_s4pm_filename {
    my ($esdt, $platform, $begin_ccsds, $version, $production_ccsds, $suffix) = @_;
    # Convert times
    my $begin = S4P::TimeTools::CCSDSa2yyyydddhhmmss($begin_ccsds);
    my $prod_datetime = $production_ccsds 
        ? S4P::TimeTools::CCSDSa2yyyydddhhmmss($production_ccsds)
        : '0000000000000';
    # Form name
    my $fname = sprintf("%s.%s%s.%s.%03d.%s", $esdt, $platform, 
        substr($begin, 0, 7), substr($begin, 7, 4), $version, $prod_datetime);
    $fname .= ".$suffix" if $suffix;
    return $fname;
}
    
# parse_modis_filename: obsolete/deprecated
sub parse_modis_filename {
    return parse_s4pm_filename(@_);
}
sub parse_s4pm_filename {
    my $filename = basename(shift);
    my ($esdt, $begindate_plus, $begin_hhmm, $version, $prod_time, $suffix) = split('\.', $filename);
    my $begin = substr($begindate_plus, 1);
    my $platform = substr($begindate_plus, 0, 1);
    $begin .= $begin_hhmm;
    return ($esdt, $platform, $begin, $version, $prod_time, $suffix);
}
sub infer_platform {
    my $esdt = shift;
#    return ( ($esdt =~ /^(MODPM|MY|PM|AIR)/) ? 'P' : 'A');
# We used to follow what we thought was the MODIS convention,
# i.e., A stands for AM-1.  Come to find out they abandoned that
# convention, so A=AM-1 *and* A=Aqua. But we keep this relict
# code above in case they change their minds.
    return 'A';
}

sub filename_pattern_has_datetime {
    my $pat = shift;
    return 0 unless ($pat =~ /\^H/ && $pat =~ /\^M/);  # Need hour & minute, not second
    return 0 unless ($pat =~ /\^Y/i);  # Check for Year
    return ( ($pat =~ /\^j/) || ($pat =~ /^m/ && $pat =~ /^d/));
}
