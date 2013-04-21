#!/tools/gdaac/COTS/perl-5.8.5/bin/perl 

=head1 NAME

s4pm_acquire_data.pl - script to acquire files from S4PA for reprocessing input

=head1 SYNOPSIS

s4pm_acquire_data.pl.pl 
B<-f> I<file>
B<-r> I<dir>
[B<-F> I<dir>]
[B<-l> I<dir>]
[B<-L> I<dir>]
[B<-t>]
[B<-a>]
[B<-A> I<host_alias>]
[B<-w>]
[B<-P>]
I<input_workorder>

=head1 DESCRIPTION

B<s4pm_acquire_data.pl> processes input work order in which specification is 
made of data file(s) to acquire from S4PA via FTP, based on the older 
s4pm_request_data.pl.  The chief difference is that instead of simply 
requesting the files, it either symlinks (if on the same machine) or does an
FTP get.

Stub files are generated for files that have been obtained from S4PA and
placed in a "ACQUIRES" subdirectory under the Acquire Data station directory. 
As each work order is processed, the existence of stub files is checked to 
avoid ordering essentially the same file twice. Additionally, acquires for 
duplicate files (same data time coverage with different insert/production time) 
are checked (nominal production only, on demand processing checks fully 
specified file name for duplicates by use of command line option [-a]).

Prior to obtaining files from S4PA, space is allocated within a disk pool 
for the incoming files. Filesize is obtained from the FILE_SPEC within each 
FILE_GROUP in the incoming workorder.

The available pool disk space is checked. If there is enough disk space, the 
amount of space available in the pool is updated. Otherwise we wait.

If FTP get fails, then the space allocated per datatype will be reallocated.

=head1 ARGUMENTS

=item B<-a>

Command line option used to skip pattern matching when checking for duplicate 
files (same data time coverage with different insert/production time) in the 
acquire. All fields in the stubfile name are used when checking for the 
existence of stub files (fully specified stub file). Used only for On Demand 
processing. Not invoked for nominal processing.

=item B<-t>

Command line option used to rename input work order as an output workorder for 
tracking acquires. Output work order is sent to Track Request station via 
Stationmaster.  Used primarily for On Demand processing. Not invoked for 
nominal processing.

=item B<-f> I<file>

Configuration file (required).
This is actually the disk allocation configuration file.

=item B<-r> I<directory>

Directory specification where stub files for acquireed data files are placed
(required).

=item B<-l> I<directory>

This causes the allocation of disk pool space to be skipped for "local" data 
files. Local files are FILE_GROUPS with a NODE_NAME matching that returned 
from a gethost()/gethostbyname(). It assumes that local inputs are symbolic 
links only and therefore, no disk allocation is necessary. The argument 
specifies what directory to put the links in.

=item B<-L> I<directory>

This assumes that all datatypes are local, no matter what the NODE_NAME in the
PDR says.  (This is to get around S4PA limitation that shows external 
interface.) All local inputs are symbolic links only and therefore, no disk
allocation is necessary.  The argument specifies what directory to put the 
links in.

=item B<-A> I<host_alias>

Alias for host, used when determining whether the PDR refers to the same
host as the S4PM instance, e.g., -A airsraw1u.ecs.nasa.gov.

=item B<-F> I<directory>

Root directory to append onto directory when doing symlinks, e.g., /ftp.
(This has no effect for FTP transfers.)

=item B<-w>

"Winnowing" mode.  Discard FILE_GROUPs from PDRs for datatypes that are
not found in the allocation database. (This will produce an INFO message, though.)

=item B<-P>

Send back PAN to originating system, according to pathname in
ORIGINATING_SYSTEM attribute.

=head1 CONFIGURATION FILE

The s4pm_acquire_data.pl script uses the Allocate Disk configuration file for 
configuration items. It contains four hashes. The first hash relates the 
product data type (key) to the maximum data product size (value). The second 
hash maps the file data type (key) to the actual pool (value).  The third hash 
describes the relationship between the pool (key) and disk pool location 
(value).  And the last one maps so-called "proxy" ESDTs to the list of ESDTs 
they proxy for.

datatype_maxsize = (
     'MOD000' => 6500000000,
);

%datatype_pool_map = (
     'MOD000' => 'INPUT',
);

%datatype_pool = (
   'MODIS' => "/usr/daac/dev/prgsrc/s4pm/testbed.unit/mock_data/output/"
);

%proxy_esdt_map = (
    'MODOCL23' => ['MO04MW', 'MO04MD', 'MO04RD'],
    'MODOSS1'  => ['MOD021KM', 'MOD02HKM', 'MOD02QKM'],
);

=head1 AUTHOR

Bruce Vollmer, Bob Mack and Chris Lynnes, NASA/GSFC, Code 610.2, Greenbelt, MD 20771 

=cut

################################################################################
# s4pm_acquire_data.pl,v 1.9 2008/06/12 14:33:27 mtheobal Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_a $opt_A $opt_l $opt_L $opt_t $opt_f $opt_r $opt_F $opt_w $opt_P
            $pdr
            $do
            $jobtype
            $jobid
	    $input_pdr
            $PUSHDIR
            $FILENAME
            $mode
            $command
          );

use Getopt::Std;
use Safe;
use Cwd 'realpath';
use S4P;
use S4P::PDR;
use S4P::PAN;
use S4P::MetFile;
use S4P::TimeTools;
use S4PM;
use File::Basename;
require 5.6.0;
use File::Copy;
use S4P::ResPool;

# Initialize disk allcation items

my ($pool, $space);
my $pool_db = "../s4pm_allocate_disk.db";
my $wait_for_disk = 300;
my $total_wait = 86400 * 365;  # Got a problem if wait is longer than 1 year!

# Get run time tokens 

S4P::logger('INFO', "Command line: " . join(' ', $0, @ARGV));

getopts('aA:tf:F:r:l:L:wP');

S4P::logger('INFO', "Will return PAN to provider on success") if $opt_P;

# Get subdirectory for stub files

my $REQUESTS_DIR = $opt_r;

# Read the configuration file containing pool database configuration
# and mappings.

my $compartment = new Safe 'CFG';
$compartment->share('%datatype_maxsize', '%datatype_pool_map', '%datatype_pool', '%proxy_esdt_map');
$compartment->rdo($opt_f) or
    S4P::perish(30, "main: Failed to read configuration file $opt_f in safe mode: $!");

# Get filename to process (input workorder name)

my $filename = $ARGV[0];

if (! -r $filename) {
    S4P::perish(2,"main: Cannot read work order from $filename");
}

S4P::logger("INFO", "*** s4pm_acquire_data.pl starting for work order: $filename");

# Parse the input workorder filename

($do, $jobtype, $jobid) = split(/\./, $filename, 4);

# Read input work order

$pdr = S4P::PDR::read_pdr($filename);
my $is_local = $opt_L || local_or_remote($pdr);
if (on_local_network($pdr)) {
    delete $ENV{'FTP_FIREWALL_TYPE'};
    delete $ENV{'FTP_FIREWALL'};
}
$opt_l = $opt_L if $opt_L;  # If force-local is set, copy directory to $opt_l for simplicity
if ($is_local && ! $opt_l) {
    S4P::logger('WARN', "Data is local but no symlink directory specified");
    S4P::logger('WARN', "This will result in a file copy over FTP");
}
my $symlinks = $is_local && $opt_l;
my $root;
if ($symlinks) {
    S4P::perish(2, "Link directory $opt_l does not exist or is not a directory") 
        unless (-d $opt_l);
}

# If the -t option is set in the script invocation, then the input work 
# order will be copied and sent to the Track Requests station.

if ($opt_t ) {

### Create output filename without DO prefix

    my $track_filename = $jobtype . ".$jobid" . ".wo";

### Rename input work order to output work order

    copy ($filename, $track_filename);

}

# Check total file count in PDR, quit if <= 0

my $total_file_count = $pdr->total_file_count;
unless ($total_file_count > 0) {
    S4P::perish(1,"main: File count in input PDR is $total_file_count");
}

# Initialize hash for paired URs and stubfiles that are candidates
# for ordering from ECS

my %URpool = ();

# Initialize array for list of URs to order from ECS

my @URlist = ();

# Initialize array for list of datatypes to order from ECS

my @datatypes_ordered = ();

# Set aside an array for all the file groups from the input work order

my @input_file_groups = @{$pdr->file_groups};

# Initialize string, counter and array for duplicate files

my $dup_string = "";
my $dup_count = 0;
my @dup_files = ();

# Loop through the file groups in the input PDR

my @acquire_fg;
foreach my $fg (@input_file_groups) {

### Get UR and data_type from file group

    my $UR = $fg->ur || make_ur($fg);
    $fg->ur($UR) unless $fg->ur;  # Fill in the UR for downstream PDRs
    my $datatype = S4PM::get_datatype_if_proxy($fg->data_type, \%CFG::proxy_esdt_map);

### Set aside an array for all the file_specs in this file group

    my @file_specs = @{$fg->file_specs};

    if (! @file_specs) {
        S4P::logger('WARN',"main: No file_specs in file_group for $datatype");
        next;
    }

### Get science files from file group

    my @science_files = sort $fg->science_files;
    my $num_science_files = @science_files;

    S4P::logger("DEBUG", "main: Number of science files in file group: $num_science_files");

    # Use the first science file for the stub file
    ($FILENAME,$PUSHDIR) = fileparse($science_files[0]);

# Prepend the *original* data type (i.e. not the proxy data type) to the file 
# name so that it can be searched for more easily by Sweep Data (when it comes 
# time to delete the stub file)

    $FILENAME = $fg->data_type . "." . $FILENAME;

### Create full path name for stubfile

    my $STUBFILE = "$REQUESTS_DIR/$FILENAME.req";

    S4P::logger("DEBUG","main: Stub filename: $STUBFILE");

### If stub file exists then it has already been obtained from S4PA, don't 
### process. if stub file does not exist then write stub file to "REQUESTS" 
### directory add corresponding UR to the list of URs to order from ECS.

### Add data type to list of data types to order from ECS and allocate space 
### for this datatype in this file group.

    unless( -e $STUBFILE) {

####### Unless opt_a is invoked, pattern matching will be used to check for 
####### duplicate files (same datatype and datatime). 

####### If opt_a is invoked, then only the full filename of the stubfile will
####### be checked for duplicate files. No pattern matching is used to check
####### for duplicates.

        unless($opt_a) {

########### Check for duplicate input files in the system (same data type 
########### and data time, different production time). If duplicates are found, 
########### increment dup_count, log pertinent info on the duplicate.

########### Parse file name and reconstruct all but production time as pattern
########### to check for duplicate files in the system. If a duplicate is 
########### found, increment the counter and add store the corresponding stub 
########### file.

            (my $dtype, my $yearday, my $datatime, my $version_id, my $prodtime) = split(/\./,$FILENAME,5);

            my $pattern = "$REQUESTS_DIR/" . join(".",$dtype,$yearday,$datatime,$version_id) . '*';

            my @filelist = glob($pattern);
            push(@dup_files,@filelist);

            if (@dup_files) {
                $dup_count++;
                next;
            }
    
        }

        $URpool{$STUBFILE} = $UR;

        push(@datatypes_ordered, $datatype);

        unless ( $symlinks) {

########### Find the maximum file size for this datatype.

            my $total_req_size = $CFG::datatype_maxsize{$datatype};

########### Map the datatype to the pool name and find the pool directory

            my $data_pool;
            if ( exists $CFG::datatype_pool_map{$datatype} ) {
                $data_pool = $CFG::datatype_pool_map{$datatype};
            } 
            # Winnowing mode:  skip datatypes in the PDR if there is no
            # disk pool for them
            elsif ($opt_w) {
                S4P::logger('INFO', "No disk pool for $datatype, skipping...");
                next;
            }
            else {
                S4P::perish(30, "main: No disk pool has been configured for data type: $datatype ACTION: Check the Allocate Disk configuration file.");
            }

            if ( exists $CFG::datatype_pool{$data_pool} ) {
                $pool = $CFG::datatype_pool{$data_pool};
            } else {
                S4P::perish(30, "main: No data pool directory has been configured for data pool: $data_pool ACTION: Check the Allocate Disk configuration file.");
            }

########### Update the pool amount. If there is not enough space, sleep until
########### space becomes available.

            unless ( -e $pool_db ) {
                S4P::perish(30, "main: Disk pool database $pool_db doesn't seem to exist!");
            }

            unless ( defined S4P::ResPool::read_from_pool($pool, $pool_db) ) {
                S4P::perish(30, "main: No space for ". basename($pool) . " defined in $pool_db!");
            }

            $space = S4P::await($total_wait, $wait_for_disk, "Disk space is low for $datatype which requires: $total_req_size from $pool. Currently, " . S4P::ResPool::read_from_pool($pool, $pool_db) . " is available", \&S4P::ResPool::update_pool, $pool, -1*$total_req_size, $pool_db);

            if (! $space) {
                S4P::perish(2,"main: Could not obtain $total_req_size for $datatype after $total_wait secs");
            }
            S4P::logger("INFO", "main: $total_req_size bytes allocated for $datatype.");
        } 
        else {
            S4P::logger("INFO", "main: Data is local, to be symlinked, so no disk pool allocation is performed.");
        }
        push @acquire_fg, $fg;
    } 
    else {
        S4P::logger("INFO", "main: STUBFILE: $STUBFILE exists, will not re-acquire file");
    }
}

my @new_fg;
my $err_code = 0;
if (@acquire_fg) {
    $pdr->file_groups(\@acquire_fg);
    $pdr->recount();
}
else {
    my $msg = sprintf("No file_groups left in PDR after %schecking stub files, exiting normally.",
        $opt_w ? "winnowing and " : '');
    S4P::logger("INFO", $msg);
    exit(0);
}

# See if we are local (symlink) or remote (FTP)
if ($symlinks) {
    if ($opt_F) {
        foreach my $fg(@{$pdr->file_groups}) {
            map {$_->directory_id("$opt_F/" . $_->directory_id)} @{$fg->file_specs};
        }
    }
    my $dir = realpath($opt_l);
    @new_fg = map {$_->symlinks($dir)} @{$pdr->file_groups};
    unless (@new_fg) {
        S4P::logger('ERROR', "Failed to create symlinks") unless @new_fg;
        $err_code = 4;
    }
}
else {   # Remote FTP/HTTP Branch
    S4P::logger('INFO', "Fetching the data to: $pool");
    # N.B.:  Mixed FTP/HTTP PDRs are not yet supported
    my $use_http = ($pdr->file_groups->[0]->ur =~ /^http/);
    my ($success, $ng) = $use_http ? $pdr->http_get($pool) 
                                   : $pdr->ftp_get($pool, 1, 0);
    print "$ng file_groups retrieved\n";
    if (! $success) {
        $err_code = 5;
        S4P::logger ("Failed to retrieve some file_groups");
    }
    else {
        # Clone FILE_GROUP objects
        @new_fg = map {$_->copy} @{$pdr->file_groups};
        my $here = S4P::PDR::gethost();

        # Change directories to be the target directory
        # Change nodename to be the target host
        foreach my $fg(@new_fg) {
            map {$_->directory_id($pool)} @{$fg->file_specs};
            $fg->node_name($here);
        }
    }
}

# Verify checksums, if set
# but only if we had success in transfer
if ($err_code == 0) {

    if (! $pdr->verify_checksums) {
        $err_code = 6;
        S4P::logger ("Checksums failed verification in some file_groups");
    }

}

if($err_code == 0) {

    S4P::logger("INFO","main: acquire successful for workorder $filename"); 

### Write stub files to acquires directory

    while ((my $stubfile, my $ur) = each(%URpool)) {
        my $rcode = S4P::write_file($stubfile,"$ur\n");
        S4P::perish(102,"main: Error writing stubfile: $stubfile") unless($rcode);
    }

    # Write skeletal metadata file if none exists
    map {make_metfile($_)} @new_fg;

    # Create output work order for register_data
    output_new_pdr($pdr, \@new_fg);

    # Send back PAN if specified on command line
    return_pan($pdr) if $opt_P;

}  else {

    S4P::logger("ERROR","main: PDR fetch FAILED for workorder $filename");
    S4P::logger("ERROR","main: FTP_FIREWALL=$ENV{'FTP_FIREWALL'}; FTP_FIREWALL_TYPE=$ENV{'FTP_FIREWALL_TYPE'}");

### Reallocate space for files that will not be coming to s4pm
### due to failed acquire (unless these are only links)

    unless ( $symlinks) {

        foreach my $dt (@datatypes_ordered) {

########### Find the maximum file size for this datatype

            my $total_req_size = $CFG::datatype_maxsize{$dt};

########### Map the datatype to the pool name and find the pool directory

            my $data_pool = $CFG::datatype_pool_map{$dt};
            $pool = $CFG::datatype_pool{$data_pool};

########### Update the pool amount

            $space = S4P::ResPool::update_pool($pool, $total_req_size, $pool_db);

            if($space) {
                S4P::logger("INFO", "main: $total_req_size bytes deallocated for $dt.");
            }
    
        }
    } else {
        S4P::logger("INFO", "main: Skipping reallocation since no allocations were made.");
    }

    S4P::perish(1,"main: Error fetching data for workorder $filename, error_code: $err_code");
}

S4P::logger("INFO", "*** s4pm_acquire_data.pl successfully completed for workorder: $filename");

exit 0;

sub on_local_network {
    my $pdr = shift;
    my $here = S4P::PDR::gethost();
    $here =~ s/[^\.]\.//;
    foreach my $fg (@{$pdr->file_groups}) {
        my $there = $fg->node_name;
        $there =~ s/[^\.]\.//;
        if ($here ne $there) {
           S4P::logger('INFO', "This host is on $here, data is on $there:  will use firewall if set");
           return 0;
        }
    }
    return 1;
}

sub local_or_remote {
    my $pdr = shift;
    my $here = S4P::PDR::gethost();
    $opt_A ||= $here;
    foreach my $fg (@{$pdr->file_groups}) {
        my $node_name = $fg->node_name;
        if ($node_name ne $here && $node_name ne $opt_A) {
           S4P::logger('INFO', "This host is $here ($opt_A), data is at $node_name:  will xfer via FTP or HTTP");
           return 0;
        }
    }
    S4P::logger('INFO', "Data is on this host ($here): will symlink it");
    return 1;   # Got all the way through: must be local
}
sub on_local_network {
    my $pdr = shift;
    my $here = S4P::PDR::gethost();
    # Strip down to network
    $here =~ s/[^\.]+\.//;
    foreach my $fg (@{$pdr->file_groups}) {
        my $there = $fg->node_name;
        $there =~ s/[^\.]+\.//;
        # If full domain is not specified, or networks match, 
        # then it's on the local network and we don't want the firewall
        if ($there =~ /\./ && $here ne $there) {
           S4P::logger('INFO', "This host is on $here, data is on $there:  will use firewall if set");
           return 0;
        }
    }
    return 1;
}

sub make_metfile {
    my $fg = shift;

    # Return if FILE_GROUP has a metfile already
    my $metfile = $fg->met_file;
    return $metfile if $metfile;

    # Get full pathname of science file(s)
    my @scifiles = $fg->science_files;
    my $scifile = shift @scifiles;
    if ($scifile !~ /\.hdf$/i) {
        S4P::perish(103, "Sorry, do not know how to make a metadata file for non-HDF-EOS files");
    }

    # Extract metadata from HDF-EOS file
    my $met = S4P::MetFile::hdfeos2odl($scifile) or
        S4P::perish(104, "Failed to make ODL from science file $scifile");
    $metfile = "$scifile.met";
    S4P::write_file($metfile, $met) or
        S4P::perish(105, "Failed to write ODL to metadata file $metfile");

    # Create FILE_SPEC and add to FILE_GROUP
    my $fs = new S4P::FileSpec('file_type' => 'METADATA', 
        'pathname' => $metfile, 'file_size' => (-s $metfile) );
    push @{$fg->file_specs}, $fs;
    return $metfile;
}

sub make_ur {
    my $fg = shift;
    my $metfile = $fg->met_file;
    $metfile =~ s/\.(xml|met)$//;
    my $node_name = $fg->node_name;
    return "ftp://$node_name/$metfile";
}
sub output_new_pdr {
    my ($pdr, $ra_file_groups) = @_;
    my $new_pdr = $pdr->copy();
    $new_pdr->file_groups($ra_file_groups);
    $new_pdr->recount();
    my $filename = sprintf("REGISTER.%d_%d.wo", time(), $$);
    # Write PDR; (don't forget about funky error code reversal)
    if ($new_pdr->write_pdr($filename) == 0) {
        S4P::logger("INFO", "Wrote output PDR to $filename");
    }
    else {
        S4P::logger("ERROR", "Failed to write output PDR to $filename: $!");
        exit(11);
    }
    return $filename;
}
sub return_pan {
    my $pdr = shift;
    # We stashed the return path for the PAN in ORIGINATING_SYSTEM
    my $pan = S4P::PAN->new($pdr);

    # N.B.: Successful PANs only so far
    my @t = gmtime();
    my $timestamp = sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", $t[5]+1900,
        $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
    $pan->disposition( '', "SUCCESSFUL", $timestamp );
    my $dest = $pdr->originating_system;
    S4P::logger('INFO', "Writing PAN to $dest");
    $dest =~ s/(ftp|http)://;
    my $protocol = $1;
    if ($protocol) {
        die "Protocol $protocol not yet supported" unless ($protocol eq 'ftp');
        $dest =~ s#//(.*?)/#/#;
        my $host = $1;
        my ($remote_file, $remote_dir) = fileparse($dest);
        S4P::logger('INFO', "PAN remote_dir=$remote_dir, remote_file=$remote_file");
        $pan->write($remote_file, $host, $remote_dir) 
            ? S4P::logger('INFO', "Successfully wrote PAN $remote_file to $remote_dir on $host")
            : S4P::perish(2, "Failed to write PAN to $remote_dir/$remote_file on $host");
        # PAN module does not unlink the temporary file
        unlink($remote_file) or warn "Failed to unlink temporary file $remote_file";
    }
    else {
        $pan->write($dest) or
            die "Failed to write PAN to $dest";
    }
    return 1;
}
