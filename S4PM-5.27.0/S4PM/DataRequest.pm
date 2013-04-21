=head1 NAME

DataRequest - routines to support making data requests to ECS

=head1 SYNOPSIS

use S4PM::DataRequest;

S4PM::DataRequest::compose_request($start, $stop, $increment, \@datatypes, $location, $ra_bbox)

@filenames = S4PM::DataRequest::write_requests(\%results);

=head1 DESCRIPTION

=head2 compose_request

compose_request takes as input a start and stop time, a time increment in 
seconds, a list of datatypes and some information about the data "location".
Optionally, a bounding box may be specified as a reference to an array 
as lat, long, lat, long.

For data in the ECS database, the location is the UR prefix.  
However, data can also be
"queried" by looking on local or remote (via anonymous FTP) disk and inferring
data times by filename convention.

compose_request breaks the specified time period up into bins based on the 
time increment and queries for all of the data in the whole time period.
It then places the resulting granules into bins based on granule start time.
The return value is a hash, in which the key is the start of the time bin
in the format 'YYYY-MM-DD HH:MM:SS'.  The value is a PDR, with each granule
represented as a FILE_GROUP.

For database queries, the FILE_SPECs are bogus, in that all FILE_GROUPs are 
treated as single-file, and the filenames used are based on the S4PM filename 
convention.  We're just using this to transmit information.
And the directory is just INSERT_DIRECTORY_HERE.

=head2 Data "Location"

For ECS requests, the $location is $ur_srvr.  

For non-ECS requests, the $location is a complex hash with information on where 
the data are located on disk, having the following structure:

  $location = {$esdt => 
                {$version =>
                  {'host' => $host,
                   'dirpat' => $pattern,
                   'filepat' => $pattern,
                    'filesize' => $typical_file_size,
                    'length' => $typical_length_in_secs,
                    'cache' => $cache_dir
                  }
                }
              }

For example:

  $location = {'MOD04_L2' => 
                {'5' =>
                   {'ftphost' => 'aadsweb.nascom.nasa.gov',
                    'dirpat' => 'allData/^V/^E/^Y/^j',
                    'filepat' => '^E.A^Y^j.^H^M.00^V.',
                    'filesize' => 2000000,
                    'length' => 300,
                    'cache' => 'metcache'
                   }
                }
              }
    

=head2 write_requests

Writes out requests as PDRs to files with the name DO.DATA_REQUEST.YYYYDDDHHMM.wo.
Perishes if an error is encountered.
Otherwise returns the array of filenames written.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# DataRequest.pm,v 1.13 2008/09/22 14:47:33 mhegde Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::DataRequest;
use strict;
use POSIX;
use File::Basename;
use S4P;
use S4P::TimeTools;
use S4P::PDR;
use S4P::FileSpec;
use S4P::MetFile;
use S4PM::GdDbi;
1;

sub compose_request {
    my ($start, $stop, $increment, $ra_datatypes, $location, $ra_bbox) = @_;

    # Set up "bins" of time on increment boundaries
    my %fg_bins;
    map {$fg_bins{$_} = []} S4P::TimeTools::CCSDSa_DateBins($start, $stop, $increment);

    # Find the max and min
    my @bin_keys = sort keys %fg_bins;
    my $min_bin = $bin_keys[0];
    my $max_bin = $bin_keys[$#bin_keys];

### $max_bin_edge defines the outer edge of the final bin, used later

    my $max_bin_edge = S4P::TimeTools::CCSDSa_DateAdd($max_bin, $increment); 

    print STDERR ("Query period is $start to $stop\n") if ($ENV{'OUTPUT_DEBUG'});

    my $adj_start = $start ;
    my $adj_stop = $stop ;

    if ($main::APPLY_LEAPSEC_CORRECTION == 1) {
        $adj_start = S4PM::leapsec_correction($adj_start,1,1) ;
        $adj_stop = S4PM::leapsec_correction($adj_stop,1,1) ;
    }

    # Prepare time part of where clause
    my @times = ($start, $stop);
    map {s/T/ /; s/Z//} @times;

    # If $location is a UR, this is just a string.
    # That tells us to open an ECS database connection.
    my $dbh;
    if (!ref($location)) {
        # Open database connection
        $dbh = S4PM::GdDbi::db_connect();
        if (! $dbh) {
            S4P::logger('ERROR', "No database connection, returning from compose_request");
            return;
        }
    }
    my $datatype;

    # Make a query for each datatype
    my ($gran_key, $bin);
    my ($esdt, $version);
    my %version_esdts;
    foreach $datatype(@{$ra_datatypes}) {
        ($esdt, $version) = split('\.', $datatype);
        $version =~ s/^0*//;
        $version ||= 'N/A';
        push @{$version_esdts{$version}}, $esdt;
    }
    foreach $version(keys %version_esdts) {
        # $version => [$esdt, $esdt, ...]
        my $ra_esdts = $version_esdts{$version};
        
        # EXECUTE QUERY
        my $now = localtime();
        my $vers = ($version =~ 'N/A') ? '' : $version;
        printf STDERR ("Beginning query for %s.%03d at %s...\n", 
            $datatype, $vers, $now) if ($ENV{'OUTPUT_DEBUG'});

        my ($rh_ur, $rh_gran_size, $rh_add_attrs, $rh_metfile, $rh_extra);

        # Get granule info from ECHO
        if ( ref $location && $location->{$esdt}{$version}{catalog} =~ /echo/i ) {
            ($rh_ur, $rh_gran_size, $rh_add_attrs) =
              S4PM::DataRequest::search_echo( $location->{$esdt}{$version}{datacenter}, $ra_esdts->[0], $vers, $adj_start, 
                $adj_stop, $ra_bbox, 'ProductionDateTime' ); 
        }
        # Get granule info from local or remote disk using data time
        elsif (ref $location) {
            ($rh_ur, $rh_gran_size, $rh_metfile, $rh_extra) = 
              S4PM::DataRequest::find_urls_by_datatime($location, 
                $ra_esdts, $vers, $adj_start, $adj_stop)
        }
        # Get granule info from ECS database
        else {
            ($rh_ur, $rh_gran_size, $rh_add_attrs) = 
              S4PM::GdDbi::get_ur_by_datatime($dbh, $ra_esdts, $vers, 
                $adj_start, $adj_stop, $location, 'ProductionDateTime');
            foreach my $key(keys %$rh_gran_size) {
                $rh_gran_size->{$key} *= (1024.*1024.);
            }
        }
        print STDERR (scalar(keys %{$rh_ur}) . " rows found for $datatype\n")
            if ($ENV{'OUTPUT_DEBUG'});
        printf STDERR ("Completed query at %s...\n", ($now = localtime())) 
            if ($ENV{'OUTPUT_DEBUG'});

        # Formulate FILE_GROUP objects for each granule and put it in the right bin
        foreach $gran_key(keys %{$rh_ur}) {
            my ($dt, $begin, $end) = split('/', $gran_key);
            $begin = S4P::TimeTools::timestamp2CCSDSa($begin);
            $end = S4P::TimeTools::timestamp2CCSDSa($end);
            if ($main::APPLY_LEAPSEC_CORRECTION == 1) {
                $begin = S4PM::leapsec_correction($begin,1,-1) ;
                $end = S4PM::leapsec_correction($end,1,-1) ;
            }
            my $production_time = $rh_add_attrs->{$gran_key}->[0];
            $production_time =~ s/\//-/g;  # Account for Sybase/Oracle difference
            my $file_group = granule2file_group($rh_ur->{$gran_key}, $dt, $begin, 
                S4P::TimeTools::timestamp2CCSDSa($end), 
                S4P::TimeTools::timestamp2CCSDSa($production_time),
                $rh_gran_size->{$gran_key},
                $rh_metfile->{$gran_key}, $rh_extra->{$gran_key});

            $bin = S4P::TimeTools::CCSDSa_DateFloorB(
                S4P::TimeTools::CCSDSa_DateRound($begin, 60), $increment);
            # Granule could start before the earliest time bin...
            if ($bin lt $min_bin) {
                $bin = $min_bin;
            }
            # But it shouldn't start after the max_bin + increment = max_bin_edge
            elsif ($bin gt $max_bin_edge) {
                S4P::logger('ERROR', "Granule $rh_ur->{$gran_key} has start of $begin, too late for $max_bin_edge");
                return;
            }
            print STDERR ("Putting $gran_key into $bin\n") if ($ENV{'OUTPUT_DEBUG'});
            # Find a bin
            if (! exists($fg_bins{$bin})) {
                S4P::logger('DEBUG', "Cannot find time bin for $dt / $begin");
            }
            else {
                push @{$fg_bins{$bin}}, $file_group;
            }
        }
    }

    # Done with database connection; give it up.
    $dbh->disconnect if $dbh;

    # Go through each bin, make a PDR and put it in the %results hash
    my %results;
    foreach $bin(sort keys %fg_bins) {
        next if (! @{$fg_bins{$bin}});
        # Make PDR
        my $pdr = new S4P::PDR;
        $pdr->file_groups($fg_bins{$bin});
        $results{$bin} = $pdr;
    }
    return %results;
}
##############################################################################
sub write_requests {
    my ($rh_results, $job_id, $work_order_id) = @_;
    my $bin;
    my @filenames;
    foreach $bin(keys %{$rh_results}) {
        my $bin_str = S4P::TimeTools::CCSDSa2yyyydddhhmmss($bin);
        substr($bin_str, -2) = '';  # Chop off ss
        $bin_str .= ".$job_id" if $job_id;  # Add optional job_id
        my $filename = "DO.REQUEST_DATA.$bin_str.wo";
        my $pdr = $rh_results->{$bin};
        $pdr->recount;
        $pdr->work_order_id($work_order_id) if ($work_order_id);
        if ($pdr->total_file_count) {
            S4P::logger('DEBUG', "Writing PDR for time bin $bin to $filename");
            if ($pdr->file_groups && $pdr->write_pdr($filename) == 0) {
                push @filenames, $filename;
            }
            else {
                S4P::perish(200, "Failure to write PDR to $filename: $!");
            }
        }
    }
    return (@filenames);
}
##############################################################################
# S E M I - P R I V A T E   M E T H O D S
##############################################################################
sub granule2file_group {
    my ($ur, $esdt, $begin, $end, $production_time, $size, $metfile, 
        $ra_extra) = @_;

    my ($ur_server, $short_name, $version_id, $dbID, $node_name, 
        $dir_id, $file_id, $platform);
    if ($ur =~ /^(ftp|https*):/) {
        my $protocol = $1;
        my $path;
        ($short_name, $version_id) = split('\.', $esdt);
        ($node_name, $path) = ($ur =~ m#$protocol://([^/]+)(/.*)#);
        # Strip out buried netrc entry, now that it is safely ensconced
        # in the  node_name
        $ur =~ s#http://(.*?):.*?/#http://$1/#;
        $dir_id = dirname($path);
        $file_id = basename($path);
    }
    else {
        ($ur_server, $short_name, $version_id, $dbID) = S4PM::ur2dbid($ur);
        $dir_id = 'INSERT_DIRECTORY_HERE';
        # Get Short Name and Version ID from UR, then infer platform from ShortName
        $platform = ($short_name =~ /^(MY|PM)/) ? 'P' : 'A';
    }

    # Start file_group
    my $file_group = new S4P::FileGroup;
    $file_group->ur($ur);
    $file_group->data_type($short_name);
    $file_group->data_version($version_id);
    $file_group->node_name($node_name) if $node_name;

    # Set data_start, data_end: assume times in CCSDSa format
    $file_group->data_start($begin);
    $file_group->data_end($end);

    # Now make a FILE_SPEC
    my $file_spec = new S4P::FileSpec;
    $file_spec->directory_id($dir_id);
    $file_spec->file_type('SCIENCE');

    # Might not be a real one, but has all the necessary info in it
    $file_id ||= S4PM::make_s4pm_filename($short_name, $platform, $begin,
         $version_id, $production_time, '');
    $file_spec->file_id($file_id);
    $file_spec->file_size(int(ceil($size))); # Brain-dead ceiling fn
    my @file_specs = $file_spec;
    if ($metfile) {
        my $met_spec = new S4P::FileSpec;
        $met_spec->directory_id($dir_id);
        $met_spec->file_id($metfile);
        $met_spec->file_type('METADATA');
        my $fsize = (-s "$dir_id/$metfile") || 1024;
        $met_spec->file_size($fsize);
        push @file_specs, $met_spec;
    }
    # Add in any extra files:  assume SCIENCE, same directory
    push (@file_specs, @$ra_extra) if $ra_extra;
    $file_group->file_specs(\@file_specs);
    return $file_group;
}
##################################################################
# find_urls_by_datatime is an alternate to get_ur_by_datatime,
# which looks on local or remote disk for the necessary filenames.
# start times are inferred by filename convention, which is passed
# in as a filename pattern.
sub find_urls_by_datatime {
    my ($location, $ra_esdts, $vers, $start, $stop) = @_;
    my (%url, %size, %metfile, %extra_fs);
    my ($start_search, $stop_search) = S4P::TimeTools::format_CCSDS_to_compare($start, $stop);
    foreach my $esdt(@$ra_esdts) {
        if (! exists $location->{$esdt}) {
            S4P::logger('ERROR', "cfg_location for $esdt not in configuration file");
        }
        elsif ($location->{$esdt}->{$vers}->{'mri_root'}) {
            my $version_string = $location->{$esdt}->{$vers}->{'version_string'} || $vers;
            search_s4pa_mri ($location->{$esdt}->{$vers}, $esdt, 
                $version_string, $start, $stop, \%url, \%size, \%metfile, 
                \%extra_fs);
        }
        else {
            search_metfiles ($location->{$esdt}->{$vers}, $esdt, $vers,
                $start_search, $stop_search, \%url, \%size, \%metfile, \%extra_fs);
        }
    }
    return (\%url, \%size, \%metfile, \%extra_fs);
}
sub search_metfiles {
    my ($rh_esdt_loc, $esdt, $vers, $start, $stop, $rh_url, $rh_size, $rh_metfile, $rh_extra_fs) = @_;
        # Extract information from config hash
    my $ftphost = $rh_esdt_loc->{'ftphost'};
    # If ftphost is not set, data are local
    my $url_host = $ftphost || S4P::PDR::gethost();
    my $dirpat = $rh_esdt_loc->{'dirpat'};
    my $filepat = $rh_esdt_loc->{'filepat'};
    my $filesize = $rh_esdt_loc->{'filesize'};
    my $length = $rh_esdt_loc->{'length'};
    my $filename_has_datetime = S4PM::filename_pattern_has_datetime($filepat);
    my $cache = $rh_esdt_loc->{'cache'} unless $filename_has_datetime;

    # Figure out what directory this day of data is in
    my $dir = S4PM::make_patterned_filename($esdt, $vers, $start, 0, $dirpat);

    # Get a directory listing
    my @files = ($ftphost) ? ftp_dirlist($ftphost, $dir) : local_dirlist($dir);
    my @match;
    my @metfiles = grep /\.(xml|met)$/, @files;
    my %metfiles = map {($_,1)} @metfiles;

    my $cache_dir = cached_metdir($cache, $esdt, $vers, $start) if $cache;
    unless ($filename_has_datetime) {
        if ($ftphost) {
            fetch_metfiles($ftphost, $dir, $cache_dir, \@metfiles);
        }
        else {
            symlink_metfiles($dir, $cache_dir, \@metfiles);
        }
    }
    my ($ra_data_files, $rh_pktfiles) = identify_data_files(@files);
    my @data_files = @$ra_data_files;
    my %pktfiles = %$rh_pktfiles;
    
    # Loop through data files
    foreach my $file(@data_files) {
        my ($begin, $end);

        # Parse filename for date-time if possible
        if ($filename_has_datetime) {
            my @parts = S4PM::parse_patterned_filename($file, $filepat);
            $begin = $parts[2];
            # Use default length because it is more expensive to get the metadata
            $end = S4P::TimeTools::CCSDSa_DateAdd($begin, $length);
        }
        # Otherwise obtained from cached metfile
        else {
            my ($metfile) = grep {-e $_} map {"$cache_dir/$file.$_"} qw(xml met);
            ($begin, $end) = S4P::MetFile::get_datetime_range($metfile);
        }

        my ($begin_search, $end_search) = S4P::TimeTools::format_CCSDS_to_compare($begin, $end);
        if ($end_search ge $start && $begin_search le $stop) {
            my $key = results_key($esdt, $vers, $begin, $end);
            # If absolute path, append directly to host to form URL
            $rh_url->{$key} = ($dir =~ m#^/#) ? "ftp://$url_host$dir/$file"
                                         : "ftp://$url_host/$dir/$file";
            $rh_size->{$key} = ($ftphost) ? $filesize : (-s "$dir/$file");
            if (exists $metfiles{"$file.xml"}) {
                $rh_metfile->{$key} = "$file.xml";
            }
            elsif (exists $metfiles{"$file.met"}) {
                $rh_metfile->{$key} = "$file.met";
            }
            # If working with packet files, add them onto an
            # extra hash of FILE_SPEC objects, with the same key
            # as everything else
            foreach my $pktfile(@{$pktfiles{$file}}) {
                my $fsize = ($ftphost) ? $filesize : (-s "$dir/$pktfile");
                my $file_spec = new S4P::FileSpec(
                    'directory_id' => $dir, 
                    'file_id' => $pktfile, 
                    'file_type' => 'SCIENCE', 
                    'file_size' => $fsize);
                push @{$rh_extra_fs->{$key}}, $file_spec;
            }
        }
    }
}
sub search_s4pa_mri {
    my ($rh_esdt_loc, $esdt, $vers, $start, $stop,
        $rh_url, $rh_size, $rh_metfile, $rh_extra_fs) = @_;
    my $mri_root = $rh_esdt_loc->{'mri_root'};

    # We use a pseudo-machine name in the .netrc as a crude password manager
    my $netrc_entry = $rh_esdt_loc->{'netrc_entry'};

    # Execute a query using S4PA's Machine Request Interface (MRI) 
    my $result = s4pa_mri_query($mri_root, $netrc_entry,  $esdt, $vers, 
        $start, $stop);

    my $http_root = $rh_esdt_loc->{'http_root'};
    my $dirpat = $rh_esdt_loc->{'dirpat'};
    my $dir = S4PM::make_patterned_filename($esdt, $vers, $start, 0, $dirpat);
    my @granules = split(/<\/Granule>/, $result);
    foreach my $g(@granules) {
        parse_mri_granule($g, $esdt, $vers, $dir, $http_root, $netrc_entry,
            $rh_url, $rh_size, $rh_metfile, $rh_extra_fs) if ($g =~ /<Granule/);
    }
    return 1;
}
sub parse_mri_granule {
    my ($granule, $esdt, $vers, $dir, $http_root,  $netrc_entry,
        $rh_url, $rh_size, $rh_metfile, $rh_extra_fs) = @_;
    my $g_begin = parse_xml_tag($granule, 'RangeBeginningDateTime');
    my $g_end = parse_xml_tag($granule, 'RangeEndingDateTime');
    my @files = split(/<\/File>/, $granule);
    my $key = results_key($esdt, $vers, $g_begin, $g_end);

    my ($g_size, $g_file, @fnames);
    foreach my $f(sort @files) {
        # Get size of file and sum to get granule size
        my $fsize = parse_xml_tag($f, 'FileSize');
        $g_size += $fsize;

        # Parse filename
        my $fname = parse_xml_tag($f, 'FileName');
        push @fnames, $fname;

        # If we are working with packet files, then add to the
        # extra set of FileSpec objects
        # Also, keep an eye out for the first packet file, which
        # we use as the name of the granule
        if (is_extra_packet_file($fname)) {
            my $file_spec = new S4P::FileSpec('directory_id'=>$dir, 
                'file_id' => $fname, 'file_type' => 'SCIENCE',
                'file_size' => $fsize);
            push @{$rh_extra_fs->{$key}}, $file_spec;
        }
        else {
            $g_file = $fname;
        }
    }

    # Place into our hashes
    my $url = sprintf("%s/%s/%s", $http_root, $dir, $g_file);
    # Embed netrc entry at the beginning of the URL if set
    $url =~ s#http://(.*?)/#http://$1:$netrc_entry/# if ($netrc_entry);
    $rh_url->{$key} = $url;
    $rh_size->{$key} = $g_size;
    # Extract met filename from LOCATOR attribute after skipping ESDT.V:
    my ($metfile) = ($granule =~ /<Granule LOCATOR=".*?:(.*?)">/);
    $rh_metfile->{$key} = $metfile;
    return 1;
}
sub s4pa_mri_query {
    my ($mri_root, $netrc_entry, $esdt, $vers, $start, $stop, @attrs ) = @_; 
    # We're really getting an HTTP login, disguised as an FTP login
    my ($user, $password) = S4P::get_ftp_login($netrc_entry) or return;
    $start =~ s/T/%20/;
    $start =~ s/Z//;
    $stop =~ s/T/%20/;
    $stop =~ s/Z//;
    my $url = sprintf('%sdataset=%s&version=%s&startTime=%s&endTime=%s&action=list&format=long&overlap', 
        $mri_root, $esdt, $vers, $start, $stop);
    S4P::logger('DEBUG', "MRI query: $url");
    my $result = S4P::http_get($url, $user, $password);
    $result =~ s/^.*<GranuleList>//s;
    $result =~ s/<\/GranuleList>$//s;
    return (($result =~ /<\/Granule>/i) ? $result : undef);
}
sub parse_xml_tag {
    my ($string, $tag) = @_;
    my ($ans) = ($string =~ /<$tag>(.*?)<\/$tag>/is);
    return $ans;
}

sub results_key {
    my ($esdt, $vers, $begin, $end) = @_;
    return sprintf("%s.%03d/%s/%s", $esdt, $vers, $begin, $end);
}
# Checks if this is a packet file OTHER THAN the "main" packet file P*1.PDS.
sub is_extra_packet_file {my $f = shift; return ($f =~ /^P.*[023456789]\.PDS$/i);}
sub identify_data_files {
    my @files = @_;
    my (%pktfiles);
    my (@data_files);
    foreach my $file(@files) {
        # Hash packet files for matchup later
        # keyed on the P*1.PDS filename
        if (is_extra_packet_file($file)) {
            my $fkey = $file;
            $fkey =~ s/\d\.PDS/1.PDS/;
            push @{$pktfiles{$fkey}}, $file;
        }
        # If file is not a metadata or browse file
        # push onto data file list
        elsif ($file !~ /\.(xml|met|jpg)/) {
            push @data_files, $file;
        }
    }
    return (\@data_files, \%pktfiles);
}
sub ftp_dirlist {
    my ($host, $dir) = @_;
    require Net::FTP;
    # specify $FTP_FIREWALL shell env. variable to enable ftp through firewall
    my %ftp_args = (Passive => 1, Timeout => 120);
    if ($ENV{FTP_FIREWALL}) {
        $ftp_args{Firewall} =  $ENV{FTP_FIREWALL};
        $ftp_args{FirewallType} = $ENV{FTP_FIREWALL_TYPE} || 1;
    }
    my $ftp;
    unless ($ftp = Net::FTP->new($host, %ftp_args)) {
        S4P::logger("ERROR: FTP connection to $host failed");
        return;
    }
    unless ($ftp->login('anonymous','s4pm@s4pm.gsfc.nasa.gov')) {
        S4P::logger("ERROR: FTP login to $host failed");
        return;
    }
    my @files = $ftp->ls($dir);
    $ftp->quit();
    return (map {basename $_} @files);
}

sub local_dirlist{
    my $dir = shift;
    if (!opendir(DIR, $dir)) {
        S4P::logger('WARN', "Failed to opendir $dir: $!");
        return;
    }
    my @files = grep {!/^\./ && !(-d "$dir/$_")} readdir DIR;
    closedir DIR;
    return @files;
}

#############################################################################
# cached_metdir returns the directory of the cached metfiles
# and creates it if it doesn't exist.
#############################################################################
sub cached_metdir {
    my ($cache, $esdt, $vers, $datetime) = @_;
    my $date = substr($datetime, 0, 10);
    # YYYY-MM-DD -> YYYY.MM.DD
    $date =~ s/-/./g;
    my $datatype = sprintf("%s.%s", $esdt, $vers);
    my $dir = join('/', $cache, $datatype, $date);
    # Generate metadata cache directory tree, if needed
    if (! -d $dir) {
        foreach my $d ($cache, "$cache/$datatype", "$cache/$datatype/$date") {
            unless (-d $d) {
                unless (mkdir($d, 0775)) {
                    S4P::logger('ERROR', "Failed to make cache directory $d: $!");
                }
            }
        }
    }
    return $dir;
}
sub symlink_metfiles {
    my ($dir, $local_dir, $ra_metfiles) = @_;
    my @failed;
    my %gotfiles;

    # Loop through needed metfiles for cache
    foreach my $f(@$ra_metfiles) {
        my $cache_metfile = "$local_dir/$f";
        # First look for dangling symlinks and unlink them
        if (-l $cache_metfile && -z $cache_metfile) {
            unlink $cache_metfile;
        }
        # If symlink exists, skip it
        unless (-l $cache_metfile) {
            # For some data types, granules are seldom actually produced, and
            # if none are there, there will be no directory for them (yet). So,
            # don't complain in this case (this happens with AIRS L0 a lot).
            unless (-e $dir and symlink("$dir/$f", $cache_metfile)) {
                push @failed, $cache_metfile;
            }
            else {
                $gotfiles{$f} = $cache_metfile;
            }
        }
    }
    if (@failed) {
        my $n_fail = scalar(@failed);
        S4P::logger('ERROR', "$n_fail metfiles failed symlink");
    }
    return %gotfiles;
}
sub fetch_metfiles {
    my ($ftphost, $dir, $local_dir, $ra_metfiles) = @_;
    my %need;
    # Form a hash of the files we need (vs. the files in cache)
    foreach my $metfile(@$ra_metfiles) {
        my $cache_metfile = "$local_dir/$metfile";
        unless (-f $cache_metfile) {
            $need{$metfile} = $cache_metfile;
        }
    }
    # Go and fetch the needed files to cache
    return ftp_getfiles($ftphost, $dir, %need);
}
sub ftp_getfiles {
    my ($host, $dir, %files) = @_;
    my %local_files;

    # Login to remote site
    require Net::FTP;
    my $ftp = Net::FTP->new($host) or return;
    # TO DO:  convert to Netrc
    $ftp->login('anonymous','s4pm@s4pm.gsfc.nasa.gov') or return;

    # Change to remote directory
    if (! $ftp->cwd($dir)) {
        S4P::logger('ERROR', "Failed to cwd to remote dir $dir");
        return;
    }

    my @failed;
    # Obtain remote files
    foreach my $remote_file(keys %files) {
        my $local_file = $files{$remote_file};;
        if ($ftp->get($remote_file, $local_file)) {
            $local_files{$remote_file} = $local_file;
        }
        else {
            push @failed, $remote_file;
        }
    }

    # Format error message if there were failures
    if (@failed) {
        my $n_fail = scalar(@failed);
        my $msg = join(',', splice(@failed, 0, 2), '...');
        S4P::logger('ERROR', "$n_fail metfiles failed FTP: $msg");
    }
    $ftp->quit();
    return %local_files;
}

################################################################################
# A method to search ECHO catalog
sub search_echo
{
    require S4P::EchoSearch;
    my ( $dataCenterId, $esdt, $vers, $startTime, $endTime, $boundingBox,
        @attributeList ) = @_;

    my ( $rh_ur, $rh_gran_size, $rh_add_attrs ) = ( {}, {}, {} );
    my $echo = S4P::EchoSearch->new();
    unless ( $echo->login() ) {
        S4P::logger( "ERROR", $echo->getErrorMessage() );
	return ( $rh_ur, $rh_gran_size, $rh_add_attrs );
    }
    my $hits = $echo->searchCollection( DATASET => $esdt, DATA_CENTER_ID => $dataCenterId );
    if ( $echo->onError() ) {
        S4P::logger( "ERROR", $echo->getErrorMessage() );
        return ( $rh_ur, $rh_gran_size, $rh_add_attrs ); 
    }
    if ( $hits == 0 ) {
        S4P::logger( "ERROR", "Collection, $esdt, not found" );
        return ( $rh_ur, $rh_gran_size, $rh_add_attrs ); 
    }
    S4P::logger( "INFO", "Collection, $esdt, found" );
    
    my $result = $echo->searchGranule( DATASET => $esdt, VERSION => $vers,
        BEGIN_DATE => $startTime, END_DATE => $endTime, 
        BOUNDING_BOX => $boundingBox,
	DATA_CENTER_ID => $dataCenterId );
    if ( $echo->onError() ) {
        S4P::logger( "ERROR", $echo->getErrorMessage() );
        return ( $rh_ur, $rh_gran_size, $rh_add_attrs );
    }
    
    foreach my $element ( keys %$result ) {
	my $granule = $result->{$element};
        my $key = sprintf( "%s.%3.3d/%s/%s",
            $esdt, $vers, $granule->{begin}, $granule->{end} );
        $rh_ur->{$key} = $granule->{url}[0];
        $rh_gran_size->{$key} = $granule->{size};
        foreach my $attribute ( @attributeList ) {
            $rh_add_attrs->{$attribute} = $granule->{lc($attribute)};
        }
    }
    return ( $rh_ur, $rh_gran_size, $rh_add_attrs );
}
