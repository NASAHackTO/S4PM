#!/usr/bin/perl

=head1 NAME

s4pm_register_data.pl - station script to catch new data and create INSERT / SELECT work orders

=head1 SYNOPSIS

s4pm_register_data.pl 
B<[-t]> 
B<[-l]> 
B<-f> I<config_file> 
B<-a> I<allocdisk_config_file> 
B<[-d]> I<allocdisk_db>
B<[-q]> 
B<[H]> 
B<[F]> 
pdr_file

=head1 DESCRIPTION

B<s4pm_register_data.pl> inventories new data, i.e. that created by S4P or 
newly arrived from ECS.

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

Configuration file with trigger datatypes

=item B<-a> I<allocdisk_conf_file>

Configuration file for disk allocation.  This is accessed to move data to
its proper location if it is just dumped in the INPUT pool, whereas it
should be going to a different pool.

=item B<-d> I<allocdisk_db>

Adjust disk allocations according to the actual size of the data.
This deallocates an amount of space equal to the difference between
maxsize and the actual file size.

=item B<-t>

Trigger a SELECT work order.

=item B<-l>

"Local" mode.  This means that we shouldn't try to rename the files.
If this is not specified, files are renamed to the local convention.

=item B<-F>

Fix bad start times for L0 granules.  This rounds down to the nearest
even hour.

=item B<-H>

Use data handles. In this case, the granule_id in the INSERT work order is
the UR file.  The UR file in turn lists the UR, followed by the metadata file,
followed by the data files.  These files are stored with their native
filenames in a data_files/ subdirectory to make globbing more efficient.

=back

=head1 FILES

=head2 Input Work Order

The input work order is a Product Delivery Record format with the local location
of the files.  ORIGINATING_SYSTEM and EXPIRATION_TIME are ignored.

=head2 Output Work Orders

=over 4

=item INSERT

The data granules in the input work order are in a simple list, one granule per line along with the granule metadata, in a single INSERT work order.
Multi-file granules (i.e. Level 0) are listed by their directory, not the
individual files.

The begin and end date are read from the metadata file in the PDR.
If either of these is not present, a string of 0000-00-00T00:00:00Z is
inserted instead.

=item SELECT_*

For each data granule that triggers an algorithm, a SELECT_* work order is 
constructed.

=head2 UR File

Register Data also writes the UR (or LGID) to a file. The file name is the
granule_id with a .ur extension, for single file granules, or MOD000.*.ur 
for MOD000 granules.

=back

=head2 Configuration File

=over 4

=item %uses

This is the number of uses that a given ESDT is put to within the system.
For example, a MOD000 is used to trigger 24 5-minute windows of Level 1 and 
geolocation data, It is also used as ancillary with the granule behind
it to handle end scans, making for a total of 25.

This number should not include the "export" use; that is added via the B<-e>
argument on the command line.

=item %trigger_select

This has a mapping of what kind of algorithms to trigger a SELECT for.
The key is the ESDT shortname, the value is an array of the names of the 
algorithm:

e.g. 'MOD03' => ['MoPGE02'].

=item %offset_times

This accounts for datatypes whose metadata times represent a point in time, 
instead of a coverage.  The key is ESDT shortname.  The value is an anonymous
array of 2 offset times (one for the beginning, one for the end) in seconds.
E.g., %offset_times = ('OZ_DAILY' => [-12*3600, 12*3600]);

=item %quality_assessment

This hash allows you to run a Quality Assessment script on the data before
doing anything irreversible, if the -q flag is specified.  
The key is the ESDT shortname.  The value is the script command. 
This script will be run with the data file as the last argument.
If it exits non-zero, B<s4pm_register_data.pl> will exit non-zero.

Example:  %quality_assessment = ('AM1ATTN0' => 'attitude_check -t .0002');

=item %ragged_granule_trap

This is a hash that contains data types where we want B<s4pm_register_data.pl> 
to throw an exception (i.e. fail) when the arriving data is not on the two-hour
boundary. This is meant for Level 0 data. The failure is meant to alert
operations that the Level 0 is not nominal and should be investigated.
The station should be configured to allow the forcing of a granule once
trapped. Currently, all data types trapped must have nominal coverages of
two hours and be on the two-hour boundary. The hash values must be set to 1.

=item %trigger_block

This hash registers commands that check for (and in some cases also remove)
trigger blocks.  The key is the algorithm to be triggered, and the value is the
command line to be executed.  The command line may include arguments, but must
take as the last three arguments $pge, $start and $end.
The command will be constructed as:
  $cmd $pge $start $end
The trigger block code should exit 0 if no trigger block is found, 1 if a
trigger block is found, and something else if an error occurs.

=item $job_id_round

Number of seconds to round off to when constructing output job_ids.
The default is 60 seconds, i.e., minutes.
This generates job_ids like:
  SELECT_AiBr_AMSU.2007001000500.wo

However, if a job_id_round of 1 is specified, we would get:
  SELECT_AiBr_AMSU.2007001000525.wo

=back

=head1 AUTHOR

Chris Lynnes

=cut

################################################################################
# s4pm_register_data.pl,v 1.16 2008/07/01 21:55:36 clynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_a $opt_d $opt_l $opt_f $opt_F $opt_q $opt_t $opt_H);
use Getopt::Std;
use File::Basename;
use File::Copy;
use S4P;
use S4P::PDR;
use S4P::TimeTools;
use S4PM;

getopts('a:d:lf:qtFH');
usage() unless @ARGV;

# Read configuration files
my ($rh_trigger_select, $rh_uses, $rh_offset_times, $rh_ragged_granule_trap, 
    $rh_quality_assessment, $rh_trigger_block, $job_id_round) = 
    S4PM::RegisterData::read_config($opt_f);
my ($rh_pool_map, $rh_pool, $rh_proxy, $rh_maxsize) = 
    S4PM::RegisterData::read_allocation($opt_a);

# Parse PDR
my $pdr = S4P::PDR::read_pdr($ARGV[0]) or 
    S4P::perish(2, "Cannot parse PDR $ARGV[0]");

my ($prefix,$job_type,$job_id) = split('\.', $ARGV[0]);
my ($file_group);

# Loop #1 through data
S4PM::RegisterData::screen_data($pdr, $ARGV[0], $rh_proxy, $rh_offset_times, 
    $rh_ragged_granule_trap, $opt_F, $opt_q, $rh_quality_assessment);

# Loop #2 through granules (=file_groups) in PDR

my @insert_text;

foreach $file_group(@{ $pdr->file_groups }) {
    my ($begin, $end, $granule_id);

    # Obtain ESDT (ShortName) from file group
    my $esdt = S4PM::get_datatype_if_proxy($file_group->data_type, $rh_proxy);

    # Check for file_group->ur; required for granfind
    S4PM::RegisterData::set_ur($file_group, $esdt) unless ($file_group->ur);

    # Number of uses = configured uses + export use
    # Currently enforcing configuration
    if (! exists $rh_uses->{$esdt}) {
        S4P::perish(5, "This Register Data station not configured for $esdt");
    }
    my $uses = $rh_uses->{$esdt};

    if ($opt_H) {
        my $dir = S4PM::RegisterData::allocation_dir($esdt, $rh_pool_map, $rh_pool) if $opt_a;
        ($granule_id, $begin, $end) = S4PM::RegisterData::data_handle($file_group, $dir, $esdt, $rh_offset_times->{$esdt}, $opt_F);
    }
    elsif ($opt_l) {
        my @science_files = $file_group->science_files();
        # Go up one level to directory if multi-file granule
        $granule_id = (scalar(@science_files) > 1) 
                       ? dirname($science_files[0]) : $science_files[0];
        # Write UR file for this FILE_GROUP
        S4P::write_file("$granule_id.ur", $file_group->ur . "\n");
    }
    else {
        # Move files to appropriate location if Allocate Disk configuration 
        # is specified
        $granule_id = move_files($file_group, $esdt, $rh_pool_map, $rh_pool, $granule_id) if ($opt_a);
        # Foreign files must be renamed to our filename convention
        ($granule_id, $begin, $end) = rename_files($file_group, 
            $rh_offset_times->{$esdt}, $opt_F);
        # Write UR file for this FILE_GROUP
        S4P::write_file("$granule_id.ur", $file_group->ur . "\n");
    }

    S4P::logger('INFO', "Granule_id=$granule_id, Uses=$uses");

    # If allocation database is specified, we need to adjust the
    # allocation downwward from maxsize to the (now known) actual file size,
    # unless the file is only a symbolic link
    S4PM::RegisterData::adjust_allocation($granule_id, $esdt, $opt_d, $rh_pool, 
            $rh_pool_map, $rh_proxy, $rh_maxsize) 
        if ($rh_pool && $opt_d && ! -l $granule_id);

    # OK, that's all we need to do if we're just routing to EXPORT
    next if ($file_group->status eq 'EXPORT_ONLY');

    # Add to INSERT work order text
    push(@insert_text, sprintf("FileId=%s Uses=%s\n", $granule_id, $uses))
        if ($uses);

    # If this is a trigger work order, make a SELECT PDR.
    S4PM::RegisterData::trigger_select($file_group, 
      $rh_trigger_select->{$esdt}, $rh_trigger_block, $begin, $end, 
      $job_id_round) if ($opt_t);
}
# Export is routed through Register Data if Data Handles are in effect
S4PM::RegisterData::write_export_work_order($pdr, $job_id) if $opt_H;

S4PM::RegisterData::write_insert_work_order(@insert_text);
exit(0);
#######################################################################
sub move_files {
    my ($file_group, $esdt, $rh_datatype_pool_map, $rh_datatype_pool, 
        $granule_id) = @_;

    my $dir = S4PM::RegisterData::allocation_dir($esdt, $rh_datatype_pool_map,
        $rh_datatype_pool);

    # Get inode info for new directory (will compare to avoid unnecessary moves)
    my ($dev0, $ino0) = stat($dir);
    my ($file_spec, $move);
    foreach $file_spec(@{$file_group->file_specs}) {
        # Check to see if they are the same directory
        my ($dev, $ino) = stat($file_spec->directory_id);
        next if ($dev == $dev0 && $ino == $ino0);

        # Move to new directory
        $move++;
        my $old = $file_spec->pathname();
        $file_spec->pathname("$dir/" . $file_spec->file_id);
        move ($old, $dir)
            ? S4P::logger('INFO', "Moved $old to $dir")
            : S4P::perish (40, "Cannot move $old to $dir: $!");
        # Check to see if there is an XML file going with it
        # and move that too
        if ($old =~ /\.met$/) {
            my $xml_met = $old;
            $xml_met =~ s/met$/xml/;
            if (-f $xml_met) {
                move($xml_met, $dir)
                    ? S4P::logger('INFO', "Moved $xml_met to $dir")
                    : S4P::perish (40, "Cannot move $xml_met to $dir: $!");
            }
        }
    }
    my $new_granule_id = ($move && $granule_id) ? ("$dir/" . basename($granule_id)) : $granule_id;
    $new_granule_id =~ s#//#/#g;
    return $new_granule_id;
}
sub rename_files {
    my ($file_group, $ra_offset_times, $fix_it) = @_;
    my ($esdt, $version, $begin, $end, $production_time, $platform) = 
        S4PM::RegisterData::data_info($file_group, $ra_offset_times, $fix_it);
    my ($file_spec, $granule_id);

    # Make directory for multi-file granules
    if (scalar(@{$file_group->file_specs}) > 2) {
        # TO DO:
        #    Adjust start and end time to 2-hour boundaries
        #    Adjust uses for short granules
        my $directory = $file_group->file_specs->[0]->directory_id;

        # Remove trailing blanks; make sure we have a slash at the end
        $directory =~ s#[ /]*$#/#;

        # Make a directory for the L0 files
        $directory .= S4PM::make_patterned_filename($esdt, $version, $begin, 0);
        mkdir $directory, 0775 or 
            S4P::perish(6, "Failed to make directory $directory for L0 files: $!");
        $granule_id = $directory;
        S4P::logger('INFO', "Moving PDS files to $directory");

        # Move the files into the new directory
        foreach $file_spec(@{ $file_group->file_specs }) {
            my $new_name = ($file_spec->file_type eq 'METADATA') 
                ? "$directory.met"
                : "$directory/" . $file_spec->file_id;
            # Move the file
            rename($file_spec->pathname, $new_name) or S4P::perish(7, 
                "Failed to move " . $file_spec->pathname . " to $new_name");
            # Change the path in the file_spec
            # N.B.:  this must be done before making the SELECT order
            $file_spec->pathname($new_name);
        }
    }
    else {
        # Rename the file to follow our filename conventions if not locally
        # generated.
        my $base = S4PM::make_patterned_filename($esdt, $version, $begin, 0);
        foreach $file_spec(@{ $file_group->file_specs }) {
            my $old_path = $file_spec->pathname;
            my $new_base = $base;
            my $new_name = ($file_spec->file_type eq 'METADATA') ? 
                 "$new_base.met" : $new_base;
            my $new_path = $file_spec->directory_id . '/' . $new_name;
            next if ($new_path eq $old_path);
            S4P::logger('INFO', "Renaming $old_path to $new_path");
            rename($old_path, $new_path) or 
                S4P::perish(7, "Failed to move $old_path to $new_path: $!");
            if ( -e "$old_path.xml" ) {
                S4P::logger('INFO', "Renaming $old_path.xml to $new_path.xml");
                rename("$old_path.xml", "$new_path.xml") or
                    S4P::perish(7, "Failed to move $old_path.xml to $new_path.xml: $!");
            }
            $file_spec->pathname($new_path);
        }
        my @science_files = $file_group->science_files();
        $granule_id = $science_files[0];
    }
    return ($granule_id, $begin, $end);
}
sub usage {
    die "Usage: $0 [-t] [-l] -f config_file -a allocdisk_config_file [-A] allocdisk_db [-q] pdr_file";
}

package S4PM::RegisterData;
use Safe;
use File::Basename;
use File::Copy;
use S4P::PDR;
use S4P::FileGroup;
use S4P::MetFile;
use S4P::TimeTools;
use S4P;
use strict;
1;
sub adjust_allocation {
    my ($granule_id, $esdt, $db, $rh_pool, $rh_pool_map, $rh_proxy, $rh_maxsize) = @_;
    my $space = S4PM::free_disk($granule_id, $esdt, $db, $rh_pool, 
        $rh_pool_map, $rh_proxy, $rh_maxsize, 2);
    if ($space) {
        S4P::logger('INFO', "Granule_id $granule_id allocation adjust downward $space bytes");
    }
    else {
        S4P::perish(157, "Failed to adjust allocation downward for $granule_id");
    }
    return;
}
sub allocation_dir {
    my ($esdt, $rh_datatype_pool_map, $rh_datatype_pool) = @_;
    # Get proper pool based on ESDT from datatype_pool_map
    my $datatype_pool = $rh_datatype_pool_map->{$esdt} or
        S4P::perish (30, "Cannot find $esdt in datatype_pool_map");

    # Get proper directory by looking pool up in datatype_pool
    my $dir = $rh_datatype_pool->{$datatype_pool} or
        S4P::perish (31, "Cannot find $datatype_pool in datatype_pool");
    return $dir;
}
sub check_begin_time {
    my ($file_group, $ra_offset_times, $fix_it, $trap) = @_;
    my ($begin);

    # Obtain metfile
    my $metfile = $file_group->met_file or 
        S4P::perish(5, "Failed to find a metadata file in FileGroup");

    # Get begin_time from metadata file
    if ($begin = S4P::MetFile::get_start_datetime($metfile)) {
        if ($ra_offset_times) {
            my $ccsds = $begin;
            $ccsds =~ s/\.\d*Z$/Z/;
            $begin = S4P::TimeTools::CCSDSa_DateAdd($ccsds, $ra_offset_times->[0]);
        }
    }
    else {
        S4P::logger('INFO', "Could not obtain begin time from metadata file $metfile");
        # Got to return something for those data products that 
        # don't have a begin_time
        return '0000-00-00T00:00:00Z';
    }

    # For L0 MODIS, check to see if it starts on the 2-hour boundary
    # If it doesn't, fail as a default
    # If fixit flag is set, round down to nearest even hour
    if ( $trap == 1 and $begin !~ /\d[02468]:00:00/) {
        if ($fix_it) {
            my $ccsds = $begin;
            # CCSDSa_DateParse does not handle fractional seconds
            $ccsds =~ s/\.\d*Z$/Z/;  
            $begin = S4P::TimeTools::CCSDSa_DateFloorB($ccsds, 7200);
            S4P::logger('INFO', "Rounding $ccsds down to $begin");
        }
        else {
            S4P::logger('ERROR', "Bad L0 begin time ($begin): probably a short granule");
            $begin = undef;
        }
    }
    $file_group->data_start($begin) if ($begin);
    return $begin;
}
sub check_quality {
    my ($file_group, $qa_script) = @_;

    # Check to see if there is a qa_script for this ESDT
    return 1 unless $qa_script;

    # Execute quality assessment script
    my $files = join(' ', $file_group->science_files);
    my $cmd = "$qa_script $files";
    S4P::logger('INFO', "Executing QA script: $cmd");
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger('ERROR', "QA failed: $errstr");
        return 0;
    }
    return 1;
}
#######################################################################
# convert_to_odl - convert Data Pool XML metadata file in a file_group to ODL
#######################################################################
sub convert_to_odl {
    my $file_group = shift;
    my @file_specs = @{$file_group->file_specs};
    my $rc = 0;
    foreach my $file(@file_specs) {
        next unless ($file->file_id =~ /\.xml$/i);
        # Got one!
        my $xml_path = $file->pathname;

        # Form output ODL filename
        my $odl_path = $xml_path;
        $odl_path =~ s/\.xml$/.met/;

        # Read in XML file
        my $xml = S4P::read_file($xml_path) or
            S4P::perish(100, "Cannot read XML file: $xml_path");;
        my $odl = S4P::MetFile::xml2odl($xml);    # Convert XML to ODL

        # Write out ODL
        # Leave the old XML around for Data Pool inserts and such
        S4P::write_file($odl_path, $odl) or
            S4P::perish(110, "Cannot write ODL file $odl_path: $!\n");
        $file->file_id(basename($odl_path));

        $rc = 1;
    }
    return $rc;
}
sub data_handle {
    my ($file_group, $dir, $esdt, $ra_offset_times, $fix_times) = @_;
    my ($esdt, $version, $begin, $end, $production_time, $platform) =
        data_info($file_group, $ra_offset_times, $fix_times);

    my $pattern = S4PM::get_filename_pattern();
    if ($production_time) {
        # Fix production time to be suitable for filenaming
        $production_time =~ s/[\-:TZ]//g;
        $production_time =~ s/\.\d*//g;
        # Work around to incorporate actual production time into filename
        $pattern =~ s/~N/$production_time/;
    }
    $pattern .= '.ur';

    # Directory may have been specified in allocation
    # Otherwise, use the directory where the data already are
    $dir ||= $file_group->file_specs->[0]->directory_id;

    # Hang on, need to see whether the directory we have is a multi-file
    # granule directory (mfg_dir)
    my $mfg_dir;
    if (-f "$dir.met" || -f ("$dir.xml")) {
        $mfg_dir = $dir;
        # Yup, it's a multi-file granule directory. 
        # Go up one to get to the disk pool directory.
        $dir = dirname($mfg_dir);
    }

    # Set up new data_files/ sub-directory
    $dir =~ s#/$##;

    # Granule_id is the root of the UR file
    my $granule_id = "$dir/" . 
        S4PM::make_patterned_filename($esdt, $version, $begin, 0, $pattern);
    my $file_dir = "$dir/data_files";
    if (! -d $file_dir) {
        mkdir($file_dir, 0775) or S4P::perish(209, "Cannot mkdir $file_dir: $!");
    }

    if ($mfg_dir) {
        # Tack on the multi-file granule directory component to avoid
        # possible filename collisions among granules
        my $mfg_base = basename($mfg_dir);
        $file_dir .= "/$mfg_base";
        if (! -d $file_dir) {
            mkdir($file_dir, 0775) or 
                S4P::perish(209, "Cannot mkdir $file_dir: $!");
        }
    }

    # Metadata/Data move section
    my ($metfile, $browse_file, @data_files);

    # There could be an XML metadata file as well
    my $xmlfile = $file_group->met_file();
    $xmlfile =~ s/\.met/.xml/;

    # Loop through FILE_SPECs
    foreach my $fs(@{$file_group->file_specs}) {
        # Move file
        my $old_path = $fs->pathname;
        my $new_dir = $file_dir;

        # For multi-file granules, keep the metadata one level above the
        # data files, i.e., at the same level as the "container" dir
        $new_dir = dirname($new_dir) 
            if ($mfg_dir && ($fs->file_type eq 'METADATA'));

        my $new_path = "$new_dir/" . $fs->file_id;
        move($old_path, $new_path) or 
            S4P::perish(211, "Failed to move $old_path to $new_path: $!");
        $fs->directory_id($new_dir);

        # Save the metadata and data filenames for our data_handle (.ur) file
        if ($fs->file_type eq 'METADATA') {
            $metfile = $fs->pathname();
        }
        elsif ($fs->file_type eq 'BROWSE') {
            $browse_file = $fs->pathname();
        }
        else {
            push (@data_files, $fs->pathname);
        }
    }

    # Don't forget the original XML metadata file if it exists
    if (-f $xmlfile) {
        move($xmlfile, $file_dir) or 
            S4P::perish(213, "Failed to move $xmlfile to $file_dir: $!");
        $xmlfile = "$file_dir/" . basename($xmlfile);
    }

    # If there was a Multi-File Granule (mfg_dir) dir, remove it
    if ($mfg_dir) {
        rmdir($mfg_dir) or 
            S4P::perish(214, "Failed to rmdir multi-file-granule directory $mfg_dir: $!");
        S4P::logger('INFO', "Removed empty multi-file-granule dir $mfg_dir");
    }

    # Write data handle file, also known as .ur file
    open UR, ">$granule_id" or 
        S4P::perish(212, "Cannot write to handle (.ur) file $granule_id: $!");
    print UR $file_group->ur, "\n";
    foreach my $data_file(@data_files) {
        print UR "DATA=$data_file\n";
    }
    print UR "BROWSE=$browse_file\n" if ($browse_file);
    print UR "MET=$metfile\n";
    print UR "XML=$xmlfile\n" if (-f $xmlfile);
    close UR or S4P::perish(212, "Cannot close handle (.ur) file $granule_id: $!");
    
    return ($granule_id, $begin, $end);
}
sub data_info {
    my ($file_group, $ra_offset_times, $fix_it) = @_;
    my $esdt = S4PM::get_datatype_if_proxy($file_group->data_type, $rh_proxy);
    my $version = $file_group->data_version;
    my ($metfile, $begin, $end);
    # Obtain metfile
    if (! ($metfile=$file_group->met_file)) {
        S4P::perish(5, "Failed to find a metadata file in $esdt FileGroup");
    }

    # Get end_time from metadata file (not used in name, but in output PDR)
    $end = S4P::MetFile::get_stop_datetime($metfile);
    if ($end && $ra_offset_times) {
        my $ccsds = $end;
        # CCSDSa_DateParse does not handle fractional seconds
        $ccsds =~ s/\.\d*Z$/Z/;
        $end = S4P::TimeTools::CCSDSa_DateAdd($ccsds, $ra_offset_times->[1]);
    }
    $end ||= '0000-00-00 00:00:00';
    # Lop off second fractions
    $end =~ s/\.\d+Z$/Z/;

    # Get production time
    my %found = S4P::MetFile::get_from_met($metfile, 'PRODUCTIONDATETIME');
    my $production_time = $found{'PRODUCTIONDATETIME'};

    # Get begin time
    my $begin = $file_group->data_start or 
        S4P::perish(123, "Cannot rename file: no begin time obtained from metfile");

    # Get platform
    my $platform = S4PM::infer_platform($esdt);
    return ($esdt, $version, $begin, $end, $production_time, $platform);
}
sub newdata_pdr {
    my ($file_group, $begin) = @_;
    my $pdr = S4P::PDR::start_pdr();
    my $new_fg = $file_group->copy;
    $pdr->file_groups([$new_fg]);
    return $pdr;
}
sub read_allocation {
    my $alloc_file = shift;
    return unless $alloc_file;
    my $compartment = new Safe 'ALLOC';
    $compartment->rdo($alloc_file) or 
       S4P::perish(1, "Cannot read config file $alloc_file in safe mode: $!\n");
    return (\%ALLOC::datatype_pool_map, \%ALLOC::datatype_pool, 
        \%ALLOC::proxy_esdt_map, \%ALLOC::datatype_maxsize);
}
sub read_config {
    my $file = shift;
    return unless $file;
    my $compartment = new Safe 'CFG';
    $compartment->rdo($file) or 
        S4P::perish(1, "Cannot read config file $file in safe mode: $!\n");
    return (\%CFG::trigger_newdata, \%CFG::uses, \%CFG::offset_times,
            \%CFG::ragged_granule_trap, \%CFG::quality_assessment, 
            \%CFG::trigger_block, $CFG::job_id_round);
}
sub screen_data {
    my ($pdr, $orig_filename, $rh_proxy, $rh_offset_times, 
        $rh_ragged_granule_trap, $fix_bad_times, $qa_flag, 
        $rh_quality_assessment) = @_;
    # Loop #1 through granules (=file_groups) in PDR
    # Get start time and check for short granules
    # Apply quality assessment script if -q flag is specified
    # Quits before we start renaming files if:
    #    fixit flag not set and short granule, or
    #    QA flag set and QA script exits non-zero.
    my (@fixtime_file_groups, @badqa_file_groups, @good_file_groups);
    foreach $file_group(@{ $pdr->file_groups }) {
        my $esdt = S4PM::get_datatype_if_proxy($file_group->data_type, $rh_proxy);

        # If metadata is XML, convert it to ODL
        convert_to_odl($file_group);

        if (! check_begin_time($file_group, $rh_offset_times->{$esdt}, 
            $fix_bad_times, $rh_ragged_granule_trap->{$esdt}) ) {
            push @fixtime_file_groups, $file_group;
        }
    	# Execute quality assessment script if -q specified
    	# Argument lets us bypass this in the failure handler
    	elsif ($qa_flag && 
              !check_quality($file_group, $rh_quality_assessment->{$esdt}) ) 
        {
            push @badqa_file_groups, $file_group;
    	}
    	else {
            S4P::logger('INFO', "Adding to good FILE_GROUPs: " . $file_group->met_file());
            push @good_file_groups, $file_group;
    	}
    }
    # Split PDR into a one with good files, to be recycled,
    # and one with bad ones, to be purged by the failure handler
    if (@fixtime_file_groups || @badqa_file_groups) {
        # If there are any good ones left, recycle a work order with
        # just the good ones.
        if (@good_file_groups) {
            my $newfile = split_pdr($pdr, \@good_file_groups, $orig_filename);
            S4P::logger('INFO', "Moving PDR with good filegroups to ../$newfile");
            move($newfile, '..');
        }
        split_pdr($pdr, \@fixtime_file_groups, $orig_filename, 'FIX_TIME') 
            if (@fixtime_file_groups);
        split_pdr($pdr, \@badqa_file_groups, $orig_filename, 'BAD_QA') 
            if (@badqa_file_groups);
        # Perish if we split due to bad times or QA
        S4P::perish(141, "Exiting due to bad times and/or QA results")
            if (@fixtime_file_groups || @badqa_file_groups);
    }
    return $pdr;
}
sub set_ur {
    my ($file_group, $esdt) = @_;
    my @sci_files = $file_group->science_files;
    my $version = $file_group->data_version;
    my $fake_lgid = sprintf("FakeLGID:%s:%03d:%s", $esdt, 
        $version, basename($sci_files[0]));
    $file_group->ur($fake_lgid);
    S4P::logger('INFO', "No UR found, making fake LGID $fake_lgid");
}
#######################################################################
# split_pdr - output two PDRs, a good file_group and a bad one
#######################################################################
sub split_pdr {
    my ($pdr, $ra_file_groups, $orig_filename, $new_jobtype) = @_;

    # Reset the file_groups to the specified ones and recount
    $pdr->file_groups($ra_file_groups);
    $pdr->recount;
    my $new_filename = "$orig_filename.$$.wo";
    $new_filename =~ s/^DO\.\w+?\./DO.$new_jobtype./ if ($new_jobtype);

    if ($pdr->write_pdr($new_filename) != 0) {
        S4P::perish(110, "Failed to write modified pdr to $new_filename: $!");
    }
    else {
        S4P::logger('INFO', "Wrote modified pdr to $new_filename");
    }
    return $new_filename;
}
#######################################################################
# trigger_block - block triggering of an algorithm
#######################################################################
sub trigger_block {
    my ($block_cmd, $pge, $start, $end) = @_;
    $block_cmd .= " $pge $start $end";
    my ($errstr, $rc) = S4P::exec_system($block_cmd);
    S4P::logger('INFO', "Executing trigger block script:\n\t$block_cmd\n");
    if ($rc == 0) {
        S4P::logger('INFO', "No block on $pge from $start to $end");
        return 0;
    }
    elsif ($rc == 1) {
        S4P::logger('INFO', "Block found for $pge from $start to $end");
        return 1;
    }
    else {
        S4P::perish(150, "Error in trigger block command $block_cmd, rc=$rc, $errstr");
    }
}
sub trigger_select {
    my ($file_group, $ra_trigger_select, $rh_trigger_block, $begin, $end, 
        $job_id_round) = @_;
    if (!$ra_trigger_select || scalar(@$ra_trigger_select) == 0) {
        S4P::logger('INFO', "No triggers for datatype " . $file_group->data_type);
        return;
    }
    if (! $begin) {                                # Branch for local mode
        my $esdt = $file_group->data_type;
        my $metfile = $file_group->met_file or
            S4P::perish(5, "Failed to find a metadata file in $esdt FileGroup");
        S4P::logger('INFO', "Local mode: getting start/stop from $metfile");
        $begin = S4P::MetFile::get_start_datetime($metfile);
        $end = S4P::MetFile::get_stop_datetime($metfile);
    }

    ####### Trim off any fractional seconds
    $begin =~ s/\.\d+Z$/Z/;
    $end =~ s/\.\d+Z$/Z/;

    $file_group->data_start($begin);
    $file_group->data_end($end);
    S4P::logger('INFO', "In trigger section: begin=$begin, end=$end");

    # Round begin time off to the nearest minute and reformat
    # to job_id status
    $job_id_round ||= 60;
    my $new_job_id = S4P::TimeTools::CCSDSa2yyyydddhhmmss(
        S4P::TimeTools::CCSDSa_DateRound($begin, $job_id_round) );

    # For each algorithm we have to trigger with this product,
    # generate a SELECT PDR
    foreach (@$ra_trigger_select) {
        # Check for trigger block
        if ($rh_trigger_block->{$_}) {
            if (S4PM::RegisterData::trigger_block($rh_trigger_block->{$_}, $_, $begin, $end)) 
            {
                S4P::logger('INFO', "Trigger is blocked for $_ from $begin to $end");
                next;
            }
        }
        S4P::logger('INFO', "Triggering $_");
        my $pdr = newdata_pdr($file_group);
        my $file = sprintf "SELECT_%s.%s.wo", $_, $new_job_id;
        S4P::perish(8, "Failed to write PDR $file") 
            if ($pdr->write_pdr($file) != 0);
    }
    return 1;
}
sub write_export_work_order {
    my ($orig_pdr, $job_id) = @_;

    my $filename = "EXPORT.$job_id.wo";

    # Loop through file_groups, keeping the ones for export
    my $export_pdr = $orig_pdr->copy();
    my @export_fg;
    foreach my $fg (@{$export_pdr->file_groups}) {
        next unless ($fg->status =~ /^EXPORT/);  # EXPORT or EXPORT_ONLY
        my $export_fg = $fg->copy();

        # Unset fields so as not to upset ECS ingest
        map { $export_fg->{$_}=undef } qw(status ur data_start data_end);

        # Add it to the list to write out
        push (@export_fg, $export_fg);
    }
    $export_pdr->file_groups(\@export_fg);

    return if (scalar(@export_fg) == 0);

    # Don't forget:  write_pdr has reversed sense of exit code
    if ($export_pdr->write_pdr($filename) != 0) {
      S4P::perish(3, "Cannot write EXPORT PDR");
    }
    S4P::logger('INFO', "Wrote EXPORT work order to $filename");
    return $filename;
}
sub write_insert_work_order {
    my @insert_text = @_;
    if (!scalar(@insert_text)) {
        S4P::logger('INFO', "No inserts to record");
        return;
    }
    # Open INSERT work order file
    my $insert_file = 'INSERT.' . S4P::unique_id . '.wo';
    open INSERT, ">$insert_file" or 
        S4P::perish(3, "Cannot write to INSERT work order $insert_file: $!");
    print INSERT @insert_text;
    close INSERT;
    S4P::logger('INFO', "Wrote INSERT work order $insert_file");
}
