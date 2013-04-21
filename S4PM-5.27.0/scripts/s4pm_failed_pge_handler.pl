#!/usr/bin/perl

=head1 NAME

s4pm_failed_pge_handler.pl - Run Algorithm failure handler for punting

=head1 SYNOPSIS

s4pm_failed_pge_handler.pl
B<-c> I<config_file>
B<-a> i<alloc_disk_cfg>
[B<-o>]

=head1 DESCRIPTION

This script is a failure handler script for the Run Algorithm station. 
The -c argument specifies the name of the configuration file containing 
the uses by which to add back due to the failure and the -a argument
specifies the Allocate Disk configuration file.

The -o option is used only for on-demand processing strings. 

The script performs these steps:

=over 4

=item 1

Examines the PDR file for the export of the failed algorithm tar file (which is 
assumed to exist in the job directory). In standard processing, this PDR is
modified so that the directory locations reflect the location to where the 
failed algorithm tar file is being moved. The PDR is then moved to the Export 
station. In on-demand processing, no modification is done. The work order is
renamed into a ORDER_FAILURE work order and moved into the Track Requests
station.

=item 2

Examines the UPDATE work order (which is assumed to exist in the job
directory) and modifies the use settings therein to reflect dependent algorithms
that will never run because of this algorithm's failure. The modified UPDATE
work order is then moved to the Track Data station.

=item 3

For output products that will never be made and for whom disk has already
been allocated, a SWEEP work order is sent to the Sweep Data station. This
station will deallocate the space and delete the files (if they exist). 

=item 4

Physically moves the failed algorithm tar file and its associated metadata file
to the proper location (to which the new S4P::PDR is now pointing).

=item 5

Cleans out the failed job directory and then removes the directory itself.

=back

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_failed_pge_handler.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Cwd;
use File::Copy;
use File::Basename;
use S4P::PDR;
use S4P;
use S4P::PCF;
use S4PM;
use Getopt::Std;
use Safe;
use strict;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw($pwd
            $opt_o
            $opt_c
            $opt_a
           );

$pwd = cwd();
chomp($pwd);

################################################################################

getopts('oc:a:');

my $on_demand = undef;
if ( $opt_o ) {
    $on_demand = 1;
}

unless ( $opt_c ) {
    S4P::perish(30, "main: No configuration file specified with the -c flag.");
}
unless ( $opt_a ) {
    S4P::perish(30, "main: No Allocate Disk configuration file specified with the -a flag.");
}

my $Metadata = undef;
my $Tarfile = undef;

# Read in this script's configuration file which contains the number of uses
# to account for for each algorithm

my $compartment1 = new Safe 'CFG1';
$compartment1->share('%Uses');
$compartment1->rdo($opt_c) or
    S4P::perish(30, "main: Failed to read in configuration file $opt_c in safe mode: $!");

# Read in Allocate Disk configuration file since it contains the directory where
# FAILPGE granules are to be sent

my $compartment2 = new Safe 'CFG2';
$compartment2->share('%datatype_pool_map', '%datatype_pool');
$compartment2->rdo($opt_a) or
    S4P::perish(30, "main: Failed to read in configuration file $opt_a in safe mode: $!");

S4P::logger("DEBUG", "main: Starting s4pm_failed_pge_handler.pl.");

# We are in the failed algorithm job directory. So, extract the algorithm name 
# from this directory's name

my $PGEname = get_pge_name();

if ( $PGEname eq undef ) {
    S4P::perish(40, "main: Could not parse algorithm name from failed job directory name.");
} else {
    S4P::logger("DEBUG", "main: Algorithm name of this failed job is: [$PGEname]");
}

# Verify that a PDR file exists for the failed algorithm tar file and its 
# metadata file and if so, retreve it

my $PDR = get_pdr_filename();
if ( $PDR eq undef ) {
    S4P::perish(102, "main: Could not find a FAILPGE PDR file in $pwd.");
} else {
    S4P::logger("DEBUG", "main: PDR file name is: [$PDR]");
}

# Now, read in the PDR

my $pdr = new S4P::PDR('text' => S4P::read_file($PDR));

# The PDR for a failed algorithm tar file and its metadata file should only 
# contain one file group with two file specs. Verify that.

my $total_file_count = $pdr->total_file_count;
if ( $total_file_count > 2 ) {
    S4P::perish(30, "main: Improperly formed failed algorithm PDR. Has more than 2 files: [total_file_count]");
}
my @file_groups = @{$pdr->file_groups};

# From the PDR's file group object, retrieve the file names of the failed
# algorithm tar file and its metadata file. The metadata file name should be the
# same as the tar file name but with the .met extension.

my @Filenames = get_tar_filename(@file_groups);
if ( ! @Filenames ) {
    S4P::perish(41, "main: Could not retrieve failed algorithm tar file and metadata file names from PDR.");
} else {
    foreach my $name (@Filenames) {
        if ( $name =~ /\.met$/ ) {
            $Metadata = $name;
        } else {
            $Tarfile = $name;
        }
    }
}
if ( $Metadata eq undef ) {
    S4P::perish(42, "main: No recognizable metadata file name (.met file) found in PDR file specs.");
}

# Modify the existing PDR file. The original PDR has the correct file names 
# and sizes. But the directory locations are set to './', i.e. the current
# directory. 
#
# In standard processing, we need to assume that the tar file will be exported.
# Since we'll be moving the failed algorithm tar file to another location, 
# we'll need to have the correct locations in the PDR.
#
# In on-demand processing, we don't care since we're going to rename the
# EXPORT_FAILPGE work order into ORDER_FAILURE and send it to track_requests.
# The contents of the work order are irrelevant.

unless ( $on_demand ) {

### Make a new array of file specs by modifying the current file specs

    my @file_specs = modify_pdr(@file_groups);

### Now, overwrite the PDR's original file spec array with the new one we
### just made. Then force a recount of the files within (just to be sure),
### and finally, write out the PDR with the .tmp file name extension.
### Next, get rid of the original PDR file and move the temp one to the
### original's file name.

    if ( ! @file_specs ) {
        S4P::perish(110, "main: Could not update failed algorithm PDR file.");
    } else {
        S4P::logger("DEBUG", "main: Creating modified PDR.");
        $pdr->file_specs(\@file_specs);
        $pdr->recount();
        $pdr->write_pdr("$PDR.tmp");
        unlink("$PDR") or 
            S4P::perish(70, "main: Could not delete original PDR file: $!");
        rename("$PDR.tmp", "$PDR") or 
            S4P::perish(71, "main: Could not rename new S4P::PDR file to old PDR file name: $!");
    }
}

# Physically, move the failed algorithm tar file and its metadata file to proper
# data directory (where the PDR is saying they are).

move_files($Tarfile, $Metadata);

# Modify the existing UPDATE work order (to Track Data) so that it not only
# reflects uses due to this algorithm's failure, but also reflects uses of 
# dependent algorithms that will never run since this one failed.

update_uses($PGEname);

# Create a SWEEP work order for the output products that need to be deleted
# (if they had a chance to be created at all) and  whose space needs to be
# deallocated.

clean_output($PGEname);

# Now, move the EXPORT work order to the Export station so that it will
# trigger the export of the failed algorithm tar file to the ECS archive.

move_pdr($PDR, $on_demand);

# Finally, clean out the failed job directory and all its contents.

clean_up();

S4P::logger("INFO", "s4pm_failed_pge_handler.pl completed successfully!");

sub clean_output {

    my $PGEname = shift;
 
    my $file;
    my $pcffile;
    my @output_files;

    opendir(DIR, "$pwd") or S4P::perish(72, "clean_output: Could not open directory: [$pwd]: $!");
    
### Locate PCF which is the work order

    while ( defined($file = readdir(DIR)) ) {
        if ( $file =~ /^DO.RUN_/ ) {
            $pcffile = $file;
            S4P::logger("DEBUG", "clean_output: Located PCF file: [$pcffile]");
            last;
        }
    }
    my ($prefix, $job_type, $job_id, $rest) = split('\.', $pcffile);

    closedir DIR or S4P::perish(73, "clean_output: Could not closedir $pwd: $!");

    my $pcf = S4P::PCF::read_pcf($pcffile) or S4P::perish(40, "clean_output: Could not read/parse PCF: [$pcffile]: $!");

    map { push(@output_files, split) } values %{$pcf->product_output_files};

    my $clean_wo = "DO.SWEEP.FAILED_" . "$PGEname" . "_" . "$job_id.wo";
    open(SWEEP, ">$clean_wo") or S4P::perish(110, "clean_output: Cannot create SWEEP work order file: [$clean_wo]: $!");
    
    foreach my $out ( @output_files ) {
        print SWEEP "$out\n";
        S4P::logger("DEBUG", "clean_output: Adding $out to SWEEP work order.");
    }
   
    close SWEEP or S4P::perish(110, "clean_output: Cannot close SWEEP work order file: [$clean_wo]: $!");

    S4P::logger("DEBUG", "clean_output: SWEEP work order for failed output products created successfully.");

    move("$clean_wo", "../../sweep_data") or S4P::perish(75, "clean_output: Cannot move SWEEP work order: $clean_wo to Sweep Data station: ../../sweep_data: $!");

}

sub get_pge_name {

################################################################################
#                                 get_pge_name                                 #
################################################################################
# PURPOSE: To determine the name of the PGE that failed                        #
################################################################################
# DESCRIPTION: get_pge_name extracts the PGE name from the name of the current #
#              directory. The name of the current directory is of the form:    #
#              FAILED.RUN_<PGEname>.jobid                                      #
################################################################################
# RETURN: PGE name                                                             #
#         undef if PGE name not obtainable                                     #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    if ( $pwd =~ /FAILED\.RUN_([^.]+).+$/ ) {
        return $1;
    } else {
        return;
    }
}


sub get_pdr_filename {

################################################################################
#                              get_pdr_filename                                #
################################################################################
# PURPOSE: To determine the file name of the PDR file for the failed PGE       #
################################################################################
# DESCRIPTION: get_pdr_filename determines the file name of the PDR file which #
#              is assumed to have been built by the Run PGE station. It is     #
#              further assumed to be named: EXPORT_FAILPGE*                    #
################################################################################
# RETURN: PDR file name                                                        #
#         undef if PDR not found                                               #
################################################################################
# CALLS: S4P::perish                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my $file;

    opendir(DIR, $pwd) or S4P::perish(76, "get_pdr_filename: Could not opendir $pwd: $!");

    while ( defined($file = readdir(DIR)) ) {
        if ( $file =~ /^EXPORT_FAILPGE/ ) {
            return $file;
        }
    }

    closedir DIR or S4P::perish(77, "get_pdr_filename: Could not closedir $pwd: $!");

    return;
}

sub get_tar_filename {

################################################################################
#                            get_tar_filename                                  #
################################################################################
# PURPOSE: To determine the file names of failed PGE tar and metadata files    #
################################################################################
# DESCRIPTION: get_tar_filename examines the PDR file and pulls out if the name#
#              of the failed PGE tar file and the associated metadata file. It #
#              is assumed that the Run PGE station as populated the PDR with   #
#              the file name accurately.                                       #
################################################################################
# RETURN: @name - array containing the tar file name and its metadata file name#
#         undef if information not found                                       #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my @file_groups = @_;

    my $file_group;
    my @names;

    my $num_file_groups = scalar( @file_groups );
    
    if ( scalar(@file_groups) > 1 ) {
        S4P::logger("FATAL", "get_tar_filename: Improperly formed failed PGE PDR. Has more than one file group.");
        return;
    } else {
        $file_group = shift @file_groups;
    }

    my @file_specs = @{$file_group->file_specs};

    if ( scalar(@file_specs) != 2 ) {
        S4P::logger("FATAL", "get_tar_filename: Improperly formed failed PGE PDR. Has more than 2 file specs.");
        return;
    } else {
        push(@names, $file_group->file_specs->[0]->file_id);
        push(@names, $file_group->file_specs->[1]->file_id);
        return @names;
    }
}

sub modify_pdr {

################################################################################
#                                modify_pdr                                    #
################################################################################
# PURPOSE: To modify the original PDR with actual directory locations          #
################################################################################
# DESCRIPTION: modify_pdr modifies the pre-existing PDR file by updating the   #
#              directory locations with the directory where the failed PGE     #
#              tar file will actually reside.                                  #
################################################################################
# RETURN: @file_specs - an updated file spec to replace the original one       #
#         undef if new file spec could not be created                          #
################################################################################
# CALLS: S4P::logger                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my @file_groups = @_;

    my $dir;

    unless ( exists $CFG2::datatype_pool_map{'FAILPGE'} ) {
        S4P::perish(30, "modify_pdr: No pool for data type FAILPGE in \%datatype_pool_map hash. Check Allocate Disk configuration file: $opt_a.");
    }
    my $pool = $CFG2::datatype_pool_map{'FAILPGE'};
    unless ( exists $CFG2::datatype_pool{$pool} ) {
        S4P::perish(30, "modify_pdr: No directory location for data pool $pool in \%datatype_pool hash. Check Allocate Disk configuration file: $opt_a.");
    }
    $dir = $CFG2::datatype_pool{$pool};

    my $file_group = shift @file_groups;
    my @file_specs = @{$file_group->file_specs};

    foreach my $file_spec (@file_specs) {
        $file_spec->directory_id($dir);
    }
    return @file_specs;
}

sub move_files {

################################################################################
#                                  move_files                                  #
################################################################################
# PURPOSE: To move the failed PGE tar file and metadata file to the proper dir #
################################################################################
# DESCRIPTION: move_files simply moves the pre-existing failed PGE tar file and#
#              its associated metadata file to the directory expected by the   #
#              revised PDR file. That location is determined from the          #
#              %datatype_pool_map hash in the Allocate Disk config file.       #
################################################################################
# RETURN: undef                                                                #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($tarfile, $metadata) = @_;

    unless ( exists $CFG2::datatype_pool_map{'FAILPGE'} ) {
        S4P::perish(30, "move_files: No pool for data type FAILPGE in \%datatype_pool_map hash. Check Allocate Disk configuration file: $opt_a.");
    }
    my $pool = $CFG2::datatype_pool_map{'FAILPGE'};
    unless ( exists $CFG2::datatype_pool{$pool} ) {
        S4P::perish(30, "move_files: No directory location for data pool $pool in \%datatype_pool hash. Check Allocate Disk configuration file: $opt_a.");
    }
    my $dir = $CFG2::datatype_pool{$pool};
 
    move("$tarfile", $dir) or
        S4P::perish(70, "move_files: Moving of failed PGE tar file failed: $!");
    move("$metadata", $dir) or
        S4P::perish(70, "move_files: Moving of failed PGE tar metadata file failed: $!");

    return;
}

sub update_uses {
 
################################################################################
#                                update_uses                                   #
################################################################################
# PURPOSE: To determine the number of uses to decrement for each affected      #
#          granule                                                             #
################################################################################
# DESCRIPTION: update_uses determines how to modify the use settings in the    #
#              Track Data database based upon the fact that this PGE and       #
#              (perhaps) dependent algorithm will not run. At this point, the  #
#              logic is very simple and based on hashes for each algorithm.    #
#              For files whose uses are to be updated, an UPDATE work order is #
#              produced and sent to the Track Data station.                    #
################################################################################
# RETURN: undef                                                                #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my $PGEname = shift;

    my $file;
    my $UpdatePDR;
    my $datatype;

    opendir(DIR, $pwd) or S4P::perish(70, "update_uses: Could not opendir $pwd: $!");

    while ( defined($file = readdir(DIR)) ) {
        if ( $file =~ /^UPDATE/ ) {
            $UpdatePDR = $file;
        }
    }
    S4P::logger("DEBUG", "update_uses: UPDATE work order found: [$UpdatePDR]");
    
    open(UPDATE, "$UpdatePDR") or 
        S4P::perish(100, "update_uses: Could not open UPDATE work order: $UpdatePDR: $!");

    open(NEWUPDATE, ">DO.$UpdatePDR") or 
        S4P::perish(110, "update_uses: Could not open NEW UPDATE work order: DO.$UpdatePDR: $!");

    while (<UPDATE>) {
        my @items = split( /\s+/ );
        my $item = shift @items;
        my ($tag, $pathname) = split( /=/, $item);
        $datatype = parse_data_type($pathname);
        if ( exists $CFG1::Uses{$PGEname}{$datatype} ) {
            print NEWUPDATE "FileId=$pathname Uses=-" . $CFG1::Uses{$PGEname}{$datatype} . "\n";
        } else {
            S4P::logger("WARNING", "update_uses: Could not find datatype $datatype in \%Uses hash for $PGEname. Check s4pm_failed_pge_handler.cfg file.");
        }
    }
 
    close UPDATE    or S4P::perish(100, "update_uses: Could not close file $UpdatePDR: $!");
    close NEWUPDATE or S4P::perish(110, "update_uses: Could not close file DO.$UpdatePDR: $!");

    move("DO.$UpdatePDR", "../../track_data") or 
        S4P::perish(70, "update_uses: Moving of UPDATE work order failed: $!");

    closedir DIR or S4P::perish(70, "update_uses: Could not closedir $pwd: $!");
}

sub move_pdr {

################################################################################
#                                  move_pdr                                    #
################################################################################
# PURPOSE: To move the modified PDR to the Export station in standard          #
#          processing and to Track Services in on-demand processing.           #
################################################################################
# DESCRIPTION: move_pdr simply moves the modified PDR file to the Export       #
#              station for processing.                                         #
################################################################################
# RETURN: undef                                                                #
################################################################################
# CALLS: S4P::perish                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my ($PDR, $on_demand) = @_;
    my $jobid;

    rename("$PDR", "DO.$PDR") or 
        S4P::perish(70, "move_pdr: Could not rename PDR file to prepend 'DO': $!");
    if ( $on_demand ) {
        if ( $PDR =~ /^EXPORT_FAILPGE.[^_]+_([0-9_]+)\.wo$/ ) {
            $jobid = $1;
        } else {
            S4P::perish(10, "Failed to parse PDR file for job id.");
        }
        rename("DO.$PDR", "DO.ORDER_FAILURE.$jobid.wo");
        move("DO.ORDER_FAILURE.$jobid.wo", "../../track_requests") or 
            S4P::perish(70, "move_pdr: Moving of PDR file failed: $!");
    } else {
        move("DO.$PDR", "../../export") or 
            S4P::perish(70, "move_pdr: Moving of PDR file failed: $!");
    }

    return;
}

sub parse_data_type {

################################################################################
#                              parse_data_type                                 #
################################################################################
# PURPOSE: To determine the data type from an entry in the UPDATE work order   #
################################################################################
# DESCRIPTION: parse_data_type determines the data type from an entry in the   #
#              UPDATE work order. Each such entry is assumed to have this      #
#              form:                                                           #
#                                                                              #
#              FileId=<fullpathname> Uses=<uses>                               #
#                                                                              #
#              The data type is assume to be contained within the              #
#              <fullpathname>                                                  #
################################################################################
# RETURN: $datatype - data type                                                #
#         undef if data type could not be determined                           #
################################################################################
# CALLS: None                                                                  #
################################################################################
# CALLED BY: update_uses                                                       #
################################################################################

    my $pathname = shift;

    my ($filename, $directory) = fileparse($pathname);
    my $datatype;

    if ( $filename =~ /^([^.]+).+$/ ) {
        $datatype = $1;
        if ( $datatype =~ /^PSPEC/ ) { $datatype = "PSPEC"; }
        return $datatype;
    } else {
        return;
    }
}

sub clean_up {

################################################################################
#                                clean_up                                      #
################################################################################
# PURPOSE: To clean out the failed job directory and then delete it            #
################################################################################
# DESCRIPTION: clean_up simply deletes all remaining files in the failed job   #
#              directory and then deletes the directory itself. By the time    #
#              this sub is called, all work should have been completed.        #
################################################################################
# RETURN: undef                                                                #
################################################################################
# CALLS: S4P::perish                                                           #
################################################################################
# CALLED BY: main                                                              #
################################################################################

    my $file;

    opendir(DIR, $pwd) or S4P::perish(70, "clean_up: Could not opendir $pwd: $!");

    while ( defined($file = readdir(DIR)) ) {
        unless ( $file eq ".." or $file eq "." ) {
            unlink($file) or S4P::perish(70, "clean_up: Could not delete file: $file: $!");
        }
    }

    my($dir, $root) = fileparse($pwd);

    chdir("..");

    rmdir($dir) or S4P::perish(70, "clean_up: Cannot remove directory $dir: $!");
}


