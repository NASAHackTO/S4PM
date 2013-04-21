#!/usr/bin/perl -w

=head1 NAME

s4pm_export.pl - script to export files produced within S4PM 

=head1 SYNOPSIS

s4pm_export.pl 
B<-c> 
[B<[-w>]
[B<[-P]>]
[B<-n>]
workorder

=head1 DESCRIPTION

B<s4pm_export.pl> processes output work order from run_pge station (essentially 
a PDR), performs some fundamental integrity checks, and moves the work order as 
a PDR file to a directory that is polled for disposition (ECS ingest, Data Pool 
insert). An s4pm_export.cfg file is used to

(1) specify the polling directory(s) where the PDRs will be sent, and 

(2) map datatypes to the polling directories. 

(3) specify datatypes that are not "renamed" to the LocalGranuleID on export.

The destinations are as follows:

=over 4

=item DEFAULT

DEFAULT is nominal ECS ingest.

=item EPD

EPD is the ECS Polling interface for external subsetters to distribute data.
In this case, it tweaks the PDR to remove the DATA_VERSION and FILE_TYPE 
attributes, which are not part of the PDR for this interface.

=item DP

DP is export to the data pool insert station.

=item EZ

EZ is for ersatz ingest.  This writes a successful SHORTPAN immediately to the 
Receive PAN station.

=head2 Renaming according to LocalGranuleID

The default behaviour (as of 5.12) is to create a symlink to the file with the 
LocalGranuleID as the name, and export that in the PDR. Listing a data type in 
the s4pm_export.cfg file in the @non_lgid_datatypes will suppress this 
behaviour.

=head1 ARGUMENTS

=over 4

=item B<-w>

Wait period; period of time (seconds) for script to sleep to ensure that
linked data files are successfully archived in ECS before attempting to
archive production history or browse files.

N.B.:  This is deprecated as unreliable. Instead, this is handled by releasing 
linked PDRs only when the succesful PAN has been received.

=item B<-c>

Configuration file used (required). Configuration file contains mapping of 
data type to some criteria on which PDRs will be split (e.g., destination 
directory). Used when splitting PDRs by datatype or other criteria associated 
with a data type.  See $pdr->split( $attribute) in S4P::PDR.pm.  Output PDRs 
are exported to the respective destination directories specified in the Export 
station'sconfiguration file.

Sample configuration file contents:

 %datatype_destination_map =(
        'RMA01' => 'DEFAULT',
        'RMA021KM' => 'DEFAULT',
        'RMA02HKM' => 'DEFAULT',
        'RMA02OBC' => 'DEFAULT',
        'RMA02QKM' => 'DEFAULT',
        'RMA03' => 'DEFAULT',
        'RMT01' => 'DATA_POOL',
        'RMT021KM' => 'DATA_POOL',
        'RMT02HKM' => 'DATA_POOL',
        'RMT02OBC' => 'DATA_POOL',
        'RMT02QKM' => 'DATA_POOL',
        'RMT03' => 'DATA_POOL'
 );

 %datatype_destination = (
   'DEFAULT' => "/usr/daac/dev/prgsrc/s4pm/testbed.unit/mock_data/output/ECS",
   'DATA_POOL' => "/usr/daac/dev/prgsrc/s4pm/testbed.unit/mock_data/output/DATA_POOL"
 );

@non_lgid_datatypes = ('RMA01');
 
=item B<-P>

Switch to turn on ersatz external ingest followed by the directory where short 
Product Acceptance Notification (PAN) indicating successsful ingest will be 
placed for each PDR generated.

=item B<-n>

Skip renaming of output files according to the LocalGranuleId attribute in the
metadata. By default, output files are renamed based on their LocalGranuleId.
The -n turns off this default behavior. This is primarily used in on-demand
processing which already handles this need.

=back

=head1 AUTHOR

Bruce Vollmer and Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_export.pl,v 1.5 2008/03/21 17:44:02 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;

use vars qw($opt_w $opt_P $opt_s $opt_c $opt_n
            $pdr
            $do
            $jobtype
            $jobid
            $dest
            $new_pdr
            $orig_system
            $outfile
);
no warnings 'once';

use Getopt::Std;
use File::Basename;
use Safe;
use Cwd 'realpath';
use S4P;
use S4P::PDR;
use S4P::MetFile;

getopts('w:P:s:c:e:n');

# Read s4pm_export.cfg file

my $compartment = new Safe 'CFG';
$compartment->share('%datatype_destination', '%datatype_destination_map', '@non_lgid_datatypes');
$compartment->rdo($opt_c) or
    S4P::perish(30, "main: Failed to read in configuration file $opt_c in safe mode: $!");


# Get wait time and ECS polling dir from station cfg file

my $wait_time = $opt_w;

# If data files are to be processed, no wait paramater is passed (undef).
# If prod_hist or browse to be processed, wait parameter is passed,
# process snoozes, adequate time is given for all linked data files
# to be archived in ECS.

if ($opt_w) {
    S4P::logger("INFO", "main: Wait time specified: $wait_time");
    sleep $wait_time;
}

# Get filename to process (input workorder name)

my $filename = $ARGV[0];

S4P::logger("INFO", "*** s4pm_export.pl starting for work order: $filename");

# Parse the input workorder filename
($do, $jobtype, $jobid) = split(/\./, $filename, 3);

# Read originating system from input work order

$pdr = S4P::PDR::read_pdr($filename);
$orig_system = $pdr->originating_system;

unless ($orig_system) {
    S4P::perish(20, "main: originating system unspecified in input work order:
    $filename");
}

# Determine filesize of input workorder

my $filesize = -s $filename;

# Get out if filesize is greater than 1 MB

if ($filesize > 1000000) {
    S4P::perish(20, "main: PDR filesize exceeds 1 MB: $filename is $filesize bytes");
}

# Determine length of filename string (max = 255)

my $filename_length = length($filename);
if ($filename_length > 255) {
    S4P::perish(20, "main: PDR filename exceeds 255 chars: $filename_length");
}

# Symlink to LocalGranuleID if necessary
my @nonlgid = ('FAILPGE', @CFG::non_lgid_datatypes); # Always skip FAILPGE
lgid_symlinks($pdr, \@nonlgid) unless ( $opt_n ) ;

# Sort the PDRs internally by FILE_GROUP directory
# This will sort by datatype for single file granules, by datatype and time
# for multifile granules
$pdr->sort_by_dir();

# Split the input workorder according to config criteria; default creates 
# single PDR

my %pdrs = $pdr->split(\&split_by_dest);
my $num_pdrs = keys %pdrs;
S4P::logger("INFO", "main: Number of output PDRs is $num_pdrs");

while (($dest, $new_pdr) = each(%pdrs)) {

### Append the destination to the first field in the PDR if it's something 
### other than the default

    if ($dest eq "DEFAULT" || $dest eq "EPD") {
            $outfile = join(".",$orig_system,$jobid,"PDR");
    } else {
            $outfile = join(".",$orig_system . "_$dest",$jobid,"PDR");
    }
    if ($dest eq "EPD") {
        my $text = $pdr->sprint(1);  # Argument specifies "spacious" PDRs
        $text =~ s/\n\s*DATA_VERSION\s*=\s*\d\d\d;//g;
        $text =~ s/\n\s*FILE_TYPE\s*=\s*\w+;//;
        S4P::write_file($outfile, $text) or 
            S4P::perish(10, "Failed to write output file $outfile");
    }
    else {
        $new_pdr -> write_pdr($outfile);
    }

### Put the pieces together to form the full path location of where
### the PDR file will be pushed

    my $dest_dir = $CFG::datatype_destination{$dest} or 
        S4P::perish(11, "Cannot find destination for $dest");
    my $final_destination = "$dest_dir/" . "$outfile";

    S4P::logger("DEBUG", "main: Polling directory: $dest_dir");
    S4P::logger("DEBUG", "main: Full path of final PDR: $final_destination");


    my $rc = S4P::move_file($outfile, $final_destination);
    unless ($rc) {
        S4P::perish(70, "main: copy of $outfile to polling directory failed;
                    Polling directory is $dest_dir");
    }

    S4P::logger("INFO", "main: EXPORT Success: copied file $outfile to $final_destination \n");

### If opt_P is specified in the station.cfg then we're doing fake ECS ingest
### generate short PAN to indicate successful ingest

    if ($dest eq "EZ") {
        generate_PAN();
    }

    unlink $outfile;
}

S4P::logger("INFO", "*** export.pl successfully completed for workorder: $filename");

exit 0;

sub generate_PAN {

### Sleep to simulate ECS processing and delay in getting PAN back

    sleep 10;

### Read in PAN directory as specified in station.cfg

    my $PAN_dir = $opt_P;

    my @PAN = ();

    my $PANdestination;

    my $datetime = S4P::format_gmtime(time);

    my @datetime = split(/ /,$datetime);
    my $date = $datetime[0];
    my $time = $datetime[1];

    my $timestamp = $date . "T" . $time . "Z";


    $PAN[0] = "MESSAGE_TYPE = SHORTPAN;";
    $PAN[1] = "DISPOSITION = SUCCESSFUL;";
    $PAN[2] = "TIME_STAMP = $timestamp;";

    my $PANstring = join("\n",@PAN);

    $PANdestination = "$PAN_dir/" . join(".", $orig_system . "_$dest", $jobid, "PAN");

    my $rcode = S4P::write_file($PANdestination,"$PANstring\n");

        if ($rcode == 0) {
            S4P::perish(110, "generate_PAN(): Problem writing PAN to $PANdestination \n");
        }

}
sub lgid_symlinks {
    my ($pdr, $ra_non_lgid_datatypes) = @_;

    # Convert array to hash for easy lookup
    my %non_lgid_datatypes = map {($_, 1)} @{$ra_non_lgid_datatypes};
    $non_lgid_datatypes{'Browse'} = 1;  # Browse has no LGID (or metfile)
    $non_lgid_datatypes{'PH'} = 1;      # PH has no LGID (or metfile)
    foreach my $fg(@{$pdr->file_groups}) {
        # Skip unless in configured list.
        next if (exists $non_lgid_datatypes{$fg->data_type});

        # Only works for single-file granules
        my @sci_files = $fg->science_files;
        my $multifile = (scalar(@sci_files) > 1);

        # Get LocalGranuleID from metadata file
        my $metfile = $fg->met_file or 
            S4P::perish(31, "No metfile for datatype " . $fg->data_type);
        my %met = S4P::MetFile::get_from_met($metfile,'LOCALGRANULEID');
        my $lgid = $met{'LOCALGRANULEID'} or 
            S4P::perish(32, "Cannot find LocalGranuleID in metfile $metfile");
      
        # Loop through FILE_SPECS (hopefully only two)
        foreach my $fs(@{$fg->file_specs}) {
            # If multi-file granule, we link only the metadata file
            # This is useful for exporting multi-granule PDS, as it
            # allows the metadata filename to look like P*1.PDS.met
            next if ($multifile && $fs->file_type eq 'SCIENCE');
            my $link;

            # Put symlinks in a subdirectory so it doesn't confuse s4pm_find_data.pl
            my $link_dir = sprintf("%s/symlinks", $fs->directory_id);

            # If the directory does not already exist AND we can't make it, quit
            if ( !(-d $link_dir) && ! mkdir($link_dir)) {
                S4P::perish(33, "Cannot create symlink dir $link_dir: $!");
            }

            my $pathname = $fs->pathname;
            $pathname =~ s#//#/#g;

            # Form the symlink pathname
            if ($fs->file_type eq 'METADATA') {
                # Figure out metadata extension (.met or .xml)
                my ($name, $path, $ext) = fileparse($metfile, '.xml', '.met');
                $link = sprintf("%s/%s%s", $link_dir, $lgid, $ext);
            }
            elsif ($fs->file_type eq 'SCIENCE') {
                $link = sprintf("%s/%s", $link_dir, $lgid);
            }
            # Rename according to LGID iff it is in same group as science file
            elsif ($fs->file_type eq 'BROWSE' && @sci_files) {
                my $newfile;
                if (is_jpeg($fs->pathname)) {
                    $newfile = "$lgid.jpg";
                }
                else {
                    $newfile = $lgid;
                    $newfile =~ s/\.hdf$/browse.hdf/i;
                }
                $link = sprintf("%s/%s", $link_dir, $newfile);
            }

            # Link may already exist, say from an abortive run
            # Make sure it points to the same file!
            if (-l $link) {
                my $reallink = realpath(readlink($link));
                if ($reallink eq realpath($pathname)) {
                    S4P::logger('INFO', "Link $link to $pathname already exists");
                    $fs->pathname($link);
                }
                else {
                    S4P::perish(34, "Link $link already exists but points to $pathname");
                }
            }
            # Create symlink and modify FileSpec with new path
            else {
                symlink($pathname, $link) or 
                    S4P::perish(33, "symlink of $pathname to $link failed: $!");;
                $fs->pathname($link);
                S4P::logger('INFO', "Linked $pathname to $link");
            }
        }
    }
    return $pdr;
}

sub split_by_dest {
    my $data_type = $_[0]->data_type;
    my $dest = $CFG::datatype_destination_map{$data_type};
    S4P::perish(21, "No destination for $data_type") unless $dest;
    return($dest);
}
sub is_jpeg {
    my $file = shift;
    open (F, $file) or die "Cannot open file $file: $!";
    my $buf;
    my $n = read(F, $buf, 3);
    return 0 if ($buf ne "\377\330\377");
    return 1;
}
