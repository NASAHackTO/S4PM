#!/usr/bin/perl

=head1 NAME

s4pm_failed_service_handler.pl - Run Algorithm failure handler services

=head1 SYNOPSIS

s4pm_failed_service_handler.pl
B<-c> I<config_file>

=head1 DESCRIPTION

This script is a failure handler script for the Run Algorithm station. 
The -c argument specifies the name of the configuration file containing 
the uses by which to add back due to the failure.

The script performs these steps:

=over 4

=item 1

Examines the UPDATE work order (which is assumed to exist in the job
directory) and modifies the use settings therein to reflect dependent algorithms
that will never run because of this algorithm's failure. The modified UPDATE
work order is then moved to the Track Data station and a copy of it is sent
to Track Requests as an ORDER_FAILURE work order.

=item 2

For output products that will never be made and for whom disk has already
been allocated, a SWEEP work order is sent to the Sweep Data station. This
station will deallocate the space and delete the files (if they exist). 

=item 3

Cleans out the failed job directory and then removes the directory itself.

=back

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_failed_service_handler.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Cwd;
use File::Copy;
use File::Basename;
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
            $opt_c
           );

$pwd = cwd();
chomp($pwd);

################################################################################

getopts('c:');

unless ( $opt_c ) {
    S4P::perish(30, "main: No configuration file specified with the -c flag.");
}

# Read in this script's configuration file which contains the number of uses
# to account for for each algorithm

my $compartment = new Safe 'CFG';
$compartment->share('%Uses');
$compartment->rdo($opt_c) or 
    S4P::perish(30, "main: Failed to read in configuration file $opt_c in safe mode: $!");

# Read in Allocate Disk configuration file since it contains the directory where
# FAILPGE granules are to be sent

S4P::logger("DEBUG", "main: Starting s4pm_failed_service_handler.pl.");

# We are in the failed algorithm job directory. So, extract the algorithm name 
# from this directory's name

my $PGEname = get_pge_name();

if ( $PGEname eq undef ) {
    S4P::perish(40, "main: Could not parse algorithm name from failed job directory name.");
} else {
    S4P::logger("DEBUG", "main: Algorithm name of this failed job is: [$PGEname]");
}

# Modify the existing UPDATE work order (to Track Data) so that it not only
# reflects uses due to this algorithm's failure, but also reflects uses of 
# dependent algorithms that will never run since this one failed.

# Also, copy the UPDATE work order to and ORDER_FAILURE work order and send
# that on to Track Requests.

update_uses($PGEname);

# Create a SWEEP work order for the output products that need to be deleted
# (if they had a chance to be created at all) and  whose space needs to be
# deallocated.

clean_output($PGEname);

# Finally, clean out the failed job directory and all its contents.

clean_up();

S4P::logger("INFO", "s4pm_failed_pge_handler.pl completed successfully!");

sub clean_output {

    my $PGEname = shift;
 
    my $file;
    my $pcffile;
    my @output_files;

    opendir(DIR, "$pwd") or S4P::perish(72, "clean_output(): Could not open directory: [$pwd]: $!");
    
### Locate PCF which is the work order

    while ( defined($file = readdir(DIR)) ) {
        if ( $file =~ /^DO.RUN_/ ) {
            $pcffile = $file;
            S4P::logger("DEBUG", "clean_output(): Located PCF file: [$pcffile]");
            last;
        }
    }
    my ($prefix, $job_type, $job_id, $rest) = split('\.', $pcffile);

    closedir DIR or S4P::perish(73, "clean_output(): Could not closedir $pwd: $!");

    my $pcf = S4P::PCF::read_pcf($pcffile) or S4P::perish(40, "clean_output(): Could not read/parse PCF: [$pcffile]: $!");

    map { push(@output_files, split) } values %{$pcf->product_output_files};

    my $clean_wo = "DO.SWEEP.FAILED_" . "$PGEname" . "_" . "$job_id.wo";
    open(SWEEP, ">$clean_wo") or S4P::perish(110, "clean_output(): Cannot create SWEEP work order file: [$clean_wo]: $!");
    
    foreach my $out ( @output_files ) {
        print SWEEP "$out\n";
        S4P::logger("DEBUG", "clean_output(): Adding $out to SWEEP work order.");
    }
   
    close SWEEP or S4P::perish(110, "clean_output(): Cannot close SWEEP work order file: [$clean_wo]: $!");

    S4P::logger("DEBUG", "clean_output(): SWEEP work order for failed output products created successfully.");

    move("$clean_wo", "../../sweep_data") or S4P::perish(75, "clean_output(): Cannot move SWEEP work order: $clean_wo to Sweep Data station: ../../sweep_data: $!");

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

sub update_uses {
 
################################################################################
#                                update_uses                                   #
################################################################################
# PURPOSE: To determine the number of uses to decrement for each affected      #
#          granule                                                             #
################################################################################
# DESCRIPTION: update_uses determines how to modify the use settings in the    #
#              Track Data database based upon the fact that this algorithm and #
#              (perhaps) dependent algorithms will not run. At this point, the #
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
    my $jobid;

    opendir(DIR, $pwd) or S4P::perish(70, "update_uses(): Could not opendir $pwd: $!");

    while ( defined($file = readdir(DIR)) ) {
        if ( $file =~ /^UPDATE\.[^_]+_(.+).wo$/ ) {
            $UpdatePDR = $file;
            $jobid = $1;
            my ($p1, $p2, $p3, $p4) = split(/_/, $jobid);
            $jobid = $p3 . "_" . $p4;
            copy($file, "DO.ORDER_FAILURE.$jobid.wo");
            move("DO.ORDER_FAILURE.$jobid.wo", "../../track_requests") or
                S4P::perish(70, "update_uses(): Moving of ORDER_FAILURE work order to ../../track_requests failed: $!");

        }
    }
    S4P::logger("DEBUG", "update_uses(): UPDATE work order found: [$UpdatePDR]");
    
    open(UPDATE, "$UpdatePDR") or 
        S4P::perish(100, "update_uses(): Could not open UPDATE work order: $UpdatePDR: $!");

    open(NEWUPDATE, ">DO.$UpdatePDR") or 
        S4P::perish(110, "update_uses(): Could not open NEW UPDATE work order: DO.$UpdatePDR: $!");

    while (<UPDATE>) {
        my @items = split( /\s+/ );
        my $item = shift @items;
        my ($tag, $pathname) = split( /=/, $item);
        $datatype = parse_data_type($pathname);
        if ( exists $CFG::Uses{$PGEname}{$datatype} ) {
            print NEWUPDATE "FileId=$pathname Uses=-" . $CFG::Uses{$PGEname}{$datatype} . "\n";
        } else {
            S4P::logger("WARNING", "update_uses(): Could not find datatype $datatype in \%Uses hash for $PGEname. Check s4pm_failed_pge_handler.cfg file.");
        }
    }
 
    close UPDATE    or S4P::perish(100, "update_uses(): Could not close file $UpdatePDR: $!");
    close NEWUPDATE or S4P::perish(110, "update_uses(): Could not close file DO.$UpdatePDR: $!");

    move("DO.$UpdatePDR", "../../track_data") or 
        S4P::perish(70, "update_uses(): Moving of UPDATE work order failed: $!");

    closedir DIR or S4P::perish(70, "update_uses(): Could not closedir $pwd: $!");
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

    opendir(DIR, $pwd) or S4P::perish(70, "clean_up(): Could not opendir $pwd: $!");

    while ( defined($file = readdir(DIR)) ) {
        unless ( $file eq ".." or $file eq "." ) {
            unlink($file) or S4P::perish(70, "clean_up(): Could not delete file: $file: $!");
        }
    }

    my($dir, $root) = fileparse($pwd);

    chdir("..");

    rmdir($dir) or S4P::perish(70, "clean_up(): Cannot remove directory $dir: $!");
}

