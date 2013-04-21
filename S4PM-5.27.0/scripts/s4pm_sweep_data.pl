#!/usr/bin/perl

=head1 NAME

s4pm_sweep_data.pl - removes files no longer needed & deallocates resources

=head1 SYNOPSIS

s4pm_sweep_data.pl 
B<[-a]>
[B<-d[b]> I<allocate_disk_db>]
[B<-l[ink]>]
B<-c[onfig]> I<allocate_disk config>
B<-e[xpire]> I<expiration_file>
I<SWEEP Workorder>

=head1 DESCRIPTION

This program is based upon DMcleanup.pl.

B<s4pm_sweep_data.pl> reads the input SWEEP work order and deletes the 
files contained therein. The SWEEP work order file name is assumed to be:
DO.SWEEP.I<jobid>.wo and this file is assumed to only contain a list of 
full pathnames of files to delete, one per line. 

From the input SWEEP work order, B<s4pm_sweep_data.pl> will delete each 
granules. For multi-file granules (such as MOD000), B<s4pm_sweep_data.pl> will 
first delete the individual files and then the directory containing those 
files.

After successful deletion of each granule, the disk resource for that granule
is deallocated from the disk pool. This is done via a call to the ResPool
module.

For those granules associated with a Request Data station stub file, the 
associated stub file is deleted as well.

No output work order is produced. 

B<s4pm_sweep_data.pl> uses the Allocate Disk configuration file and the 
Allocate Disk database file, both of which are assumed to be in the station 
directory (or, at least links to them).

=head1 ARGUMENTS

=over 4

=item B<-a> or B<-actual>

Use actual sizes in allocation. In this mode, the actual size of the file is 
deallocated instead of the maximum size specified in the configuration file.

=item B<-d[b]> I<allocate_disk_db>

Database (DBM) file to deallocate from. This option is required if
B<-actual> is set. It is otherwise ignored.

=item B<-l[ink]>

Causes no deallocation to be performed for files that are really just symbolic 
links, presumably because these files are never allocated space in the first
place (see the -l option of s4pm_request_data.pl).

=item B<-c[onfig]> I<allocate_disk config>

Configuration file with allocation mappings and maximum sizes.

=item B<-e[xpire]> I<expiration_file>

File with granules that are ripe for expiration, as they are all used up.
Generally speaking, this will be just files in the INPUT pool. (Yes, it's a 
"magic string"; we'll override it later if necessary. For now, YAGNI.)
The program gets the URs from the request_data/REQUESTS directory and writes
them to this file.

=back

=head1 AUTHORS

Bob Mack, NASA/GSFC, Code 902, Greenbelt, MD 20771

Stephen Berrick, NASA/GSFC, Code 610.2

Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_sweep_data_handles.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use S4P::MetFile;
use S4PM;
use S4PM::Handles;
use Getopt::Long;
use S4P::ResPool;
use DB_File;
use File::Basename;
use Safe;

################################################################################
# Global variables                                                             #
################################################################################
 
use vars qw($InputWorkorder
            $ConfigFile
            $ALLOCDB
            $req_stub_dir
            $use_actual_size
            $obey_links
            $expiration_file
);
 
# Relative directory where the request stub files reside

$req_stub_dir = "../../request_data/REQUESTS";

$ALLOCDB = undef;

################################################################################

# Process command-line arguments

GetOptions( "config=s" => \$ConfigFile,
            "db=s"     => \$ALLOCDB,
            "actual"   => \$use_actual_size,
            "link"     => \$obey_links,
            "expire=s" => \$expiration_file,
 );

unless ( $ConfigFile ) {
    S4P::perish(10, "main: No Allocate Disk configuration file was specified on the command line with the -c argument.");
}
unless ( -e $ConfigFile ) {
    S4P::perish(30, "main: Allocate Disk configuration file: $ConfigFile doesn't seem to exist!");
}
unless ( $ALLOCDB ) {
    S4P::perish(10, "main: No Allocate Disk database file was specified on the command line with the -d argument.");
}
unless ( -e $ALLOCDB ) {
    S4P::perish(30, "main: The Allocate Disk database file: $ALLOCDB doesn't seem to exist!");
}
$use_actual_size = 1 if ($use_actual_size);  # Make sure value is 1, not some other non-zero value

my $compartment = new Safe 'CFG';
$compartment->share('%datatype_pool_map','%datatype_pool', '%proxy_esdt_map', '%datatype_maxsize');
$compartment->rdo($ConfigFile) or 
    S4P::perish(1, "main: Cannot read config file $ConfigFile in safe mode: $!");
my $rh_pool = \%CFG::datatype_pool;
my $rh_pool_map = \%CFG::datatype_pool_map;
my $rh_proxy = \%CFG::proxy_esdt_map;
my $rh_maxsize = \%CFG::datatype_maxsize;
 
# Verify that an input work order (last command-line argument) has been
# specified and that it exists. Note that GetOptions (above) will remove
# from ARGV only those arguments it recognizes and leave the remaining in
# place. Since the input work order is not "recognized", it is guaranteed
# to be the only remaining argument and will thus be the first: $ARGV[0]

if ( !defined $ARGV[0] ) {
    S4P::perish(20, "main: No input work order specified!");
} else {
    $InputWorkorder = $ARGV[0];
    unless ( -e $InputWorkorder ) {
        S4P::perish(20, "main: Workorder $InputWorkorder doesn't seem to exist!");
    }
}
 
S4P::logger("INFO", "********** s4pm_sweep_data.pl starting for work order $InputWorkorder **********");

# Read in the input SWEEP work order

my $wo_string = S4P::read_file($InputWorkorder);
unless ( $wo_string ) {
     S4P::perish(20, "main: S4P::read_file: No string read from input work order: $InputWorkorder.");
}
 
### Parse work order string for file pathname

my @handles = split(/\n/, $wo_string);

my $actual_size;
foreach my $handle ( @handles ) {
    my ($datatype, undef, undef, undef) = S4PM::parse_patterned_filename($handle);

### If data type not found via file handle...
    unless ( $datatype ) {
        $datatype = get_metadata_shortname($handle);
    }

    my @files = S4PM::Handles::get_filenames_from_handle($handle);
    my $missing = 0;
    foreach my $file ( @files ) {
        next if ( $file eq '' );
        unless ( -e $file ) {
            $missing = 1;
        }
    }
    if ( $missing ) {
        unless ( S4PM::free_disk($handle, $datatype, $ALLOCDB, 
                       $rh_pool, $rh_pool_map, $rh_proxy, $rh_maxsize, 0) ) {
            S4P::perish(60, "main: Failed to de-allocate disk for files: @files");
        }
        next;
    }
    $actual_size = S4PM::Handles::get_granule_size($handle);
    unless ( wipe_handle($handle, $datatype, $expiration_file) ) {
        if ( $obey_links ) {
            if ( ! -l $handle ) {
                unless ( S4PM::free_disk($handle, $datatype, $ALLOCDB, $rh_pool, 
                    $rh_pool_map, $rh_proxy, $rh_maxsize, $use_actual_size, 
                    $actual_size) ) {
                    S4P::perish(60, "main: Failed to de-allocate disk for $handle");
                }
            } else {
                S4P::logger("INFO", "main: Handle $handle is only a symbolic link and -l option is set. Therefore, no re-allocation being performed.");
            }
        } else {
            unless ( S4PM::free_disk($handle, $datatype, $ALLOCDB, $rh_pool, 
                $rh_pool_map, $rh_proxy, $rh_maxsize, $use_actual_size, 
                $actual_size) ) {
                S4P::perish(60, "main: Failed to de-allocate disk for $handle");
            }
        }
    } else {
        S4P::logger("DEBUG", "main: Directory: [$handle] is NOT being de-allocated.");
    }

}

S4P::logger("INFO", "********** s4pm_sweep_data.pl completed successfully! **********");

exit 0;

sub compare_urs {

################################################################################
#                                  compare_urs                                 #
################################################################################
# PURPOSE: To compare two URs to see if they are equal                         #
################################################################################
# DESCRIPTION: compare_ur compares two URs by comparing the last portion of    #
#              each UR. This last portion contains the ESDT ShortName,         #
#              VersionID, and database ID. To qualify as being equal, this     #
#              last portion must match. The front part of the UR may not match #
#              due to ':DSSDSRV' being replaced by 'GSF:DSSDSRV' or numbers    #
#              being different.                                                #
################################################################################
# RETURN: 1 - URs are equal                                                    #
#         0 - URs are NOT equal                                                #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
################################################################################
# CALLED BY: wipe_request                                                      #
################################################################################

    my($file_ur, $req_ur) = @_;

    my $file_dbid = "";
    my $req_dbid;

    S4P::logger("DEBUG", "compare_urs(): File UR: [$file_ur]");
    S4P::logger("DEBUG", "compare_urs(): Request UR: [$req_ur]");

### If URs are URLs (ftp or http), we compare directly. But if they are ECS
### style URs, we can't since request stub files contain [:DSSDSRV] while URs
### in .ur files contain [GSF:DSSDSRV] (GSF for Goddard DAAC, in this case).
### Thus, for ECS style URs, we focus specifically on the database IDs which
### will match if the two are equivalent.

### There's probably a better way to do this.

    if ( $file_ur =~ /^ftp/ or $file_ur =~ /^http/ ) {
        $file_dbid = $file_ur;
    } elsif ( $file_ur =~ /^.*\:(.*\.[0-9][0-9][0-9]\:.*)$/ ) {
        $file_dbid = $1;
        $file_dbid =~ s/^\s+//;
        $file_dbid =~ s/\s+$//;
        S4P::logger("DEBUG", "compare_urs(): File DBid: [$file_dbid]");
    }

    if ( $req_ur =~ /^ftp/ or $req_ur =~ /^http/ ) {
        $req_dbid = $req_ur;
    } elsif ( $req_ur =~ /^.*\:(.*\.[0-9][0-9][0-9]\:.*)$/ ) {
        $req_dbid = $1;
        $req_dbid =~ s/^\s+//;
        $req_dbid =~ s/\s+$//;
        S4P::logger("DEBUG", "compare_urs(): Request DBid: [$req_dbid]");
    } else {
        S4P::perish(40, "compare_urs(): Could not parse file UR: $req_ur");
    }

    if ( $file_dbid eq $req_dbid ) {
        S4P::logger("DEBUG", "compare_urs(): Request and File URs are equal");
        return 1;
    } else {
        S4P::logger("DEBUG", "compare_urs(): Request and File URs are NOT equal");
        return 0;
    }
}

sub wipe_handle {

    my ($handle, $datatype, $expiration_file) = @_;

    my @suffixlist = ('ur', 'met', 'hdf');
    my (undef, undef, $ext) = fileparse($handle, @suffixlist);

### First, obtain the list of data files and then delete them

    my @files = S4PM::Handles::get_filenames_from_handle($handle);
    foreach my $file ( @files ) {
        if ( unlink($file) ) {
            S4P::logger("INFO", "wipe_handle(): Data file $file deleted.");
        } else {
            S4P::logger("FATAL", "wipe_handle(): Failed to delete data file [$file]: $!");
            return 1;
        }
    }

### Second, obtain the ODL metadata file and then delete it

    my $met   = S4PM::Handles::get_metadata_from_handle($handle);
    if ( unlink($met) ) {
        S4P::logger("INFO", "wipe_handle(): ODL metadata $met deleted.");
    } else {
        S4P::logger("FATAL", "wipe_handle(): Failed to delete ODL metadata file [$met]: $!");
        return 1;
    }

### Third, if there is an XML metadata file, delete it

    unless ( $InputWorkorder =~ /QC_BLOCK/ ) {     # We assume that there are no XML files for QC_BLOCK
        my $xmlfile = S4PM::Handles::get_xml_from_handle($handle);
        if ( $xmlfile and -e $xmlfile ) {
            if ( unlink($xmlfile) ) {
                S4P::logger("INFO", "wipe_handle(): XML metadata file $xmlfile deleted.");
            } else {
                S4P::logger("FATAL", "wipe_handle(): Failed to delete XML metadata file [$xmlfile]: $!");
                return 1;
            }
        }
    }
    
### Fourth, if there is a Browse file, delete it

    my $browsefile = S4PM::Handles::get_browse_from_handle($handle);
    if ( $browsefile and -e $browsefile ) {
        if ( unlink($browsefile) ) {
            S4P::logger("INFO", "wipe_handle(): Browse file $browsefile deleted.");
        } else {
            S4P::logger("FATAL", "wipe_handle(): Failed to delete Browse file [$browsefile]: $!");
            return 1;
        }
    }

### Fifth, remove any request stub files and delete the handles themselves

### (A) Old-style handles (i.e. handles are data files themselves)

    if ( $ext ne "ur" ) {
        my $newhandle = $handle . ".ur";

####### If QC_BLOCK, then we know that there can't be a request/acquire stub
####### file and besides, there won't be a UR file (which wipe_request() needs)

        if ( $InputWorkorder =~ /QC_BLOCK/ ) {
            S4P::logger("INFO", "wipe_handle(): Work order type is QC_BLOCK. Ignoring deletion of request/acquire stub file and deletion of .ur file.");
        } else {
            wipe_request($newhandle, $datatype, $expiration_file);
            if ( unlink($newhandle) ) {
                S4P::logger("INFO", "wipe_handle(): Handle file $newhandle deleted.");
            } else {
                S4P::logger("FATAL", "wipe_handle(): Failed to delete handle file [$newhandle]: $!");
                return 1;
            }
        }
    }

    if ( $ext ne "ur" and -d $handle ) {	# Remove directory container
        if ( rmdir($handle) ) {
            S4P::logger("INFO", "wipe_handle(): Handle directory $handle deleted.");
        } else {
            S4P::logger("FATAL", "wipe_handle(): Failed to delete handle directory [$handle]: $!");
            return 1;
        }
    }

### (B) New-style handles (i.e. handles are actually .ur files)

    if ( $ext eq "ur" ) {
        wipe_request($handle, $datatype, $expiration_file);
        if ( unlink($handle) ) {
            S4P::logger("INFO", "wipe_handle(): $handle deleted.");
        } else {
            S4P::logger("FATAL", "wipe_handle(): Failed to delete directory [$handle]: $!");
            return 1;
        }
    }

### New style handles with directory containers

    if ( $ext eq 'ur' ) {
        my $possible_dir = $met;
        $possible_dir =~ s/\.[^.]+$//;
        if ( -d $possible_dir ) {
            if ( rmdir($possible_dir) ) {
                S4P::logger("DEBUG", "wipe_handle(): $possible_dir deleted.");
            } else {
                S4P::logger("FATAL", "wipe_handle(): Failed to delete directory [$possible_dir]: $!");
                return 1;
            }
        }
    }

}

sub wipe_request {
 
################################################################################
#                                  wipe_request                                #
################################################################################
# PURPOSE: To delete a Request Data station request stub file                  #
################################################################################
# DESCRIPTION: wipe_request deletes the request stub file associated with the  #
#              file being deleted. Only files associated with data types being #
#              orderded via the Request Data station have request stub files.  #
################################################################################
# RETURN: 0 - Successful                                                       #
#         1 - Failure                                                          #
################################################################################
# CALLS: S4P::logger                                                           #
#        S4P::perish                                                           #
#        S4PM::parse_ur_file                                                   #
#        compare_urs                                                           #
################################################################################
# CALLED BY: wipe_file                                                         #
#            wipe_directory                                                    #
################################################################################

    my ($handle, $datatype, $expiration_file) = @_;
    my $req;

    S4P::logger("DEBUG", "wipe_request(): Handle file: $handle");
    S4P::logger("DEBUG", "wipe_request(): data type: [$datatype]");

    my $file_ur = S4PM::Handles::get_ur_from_handle($handle);

### With the UR in hand, examine the request stub files and find the one
### whose contents matches the UR. This will be the stub file we want to
### delete.

    opendir(DIR, "$req_stub_dir") or S4P::perish(100, "wipe_requests(): Cannot open directory $req_stub_dir: $!");
 
    while ( defined($req = readdir(DIR)) ) {
        next if ( $req eq "." or $req eq "..");
        if ( $req =~ /^$datatype/ ) {
            my $status = open(REQ, "$req_stub_dir/$req");
            unless ( $status ) {
                S4P::logger("ERROR", "wipe_request(): Failed to open file $req_stub_dir/$req: $!");
                next;
            }
            my $req_ur = <REQ>;
            chomp($req_ur);
            close(REQ) or S4P::perish(100, "wipe_request(): Cannot close file: [$req_stub_dir/$req]: $!");
            if ( compare_urs($file_ur, $req_ur) ) {
                if ( unlink("$req_stub_dir/$req") ) {
                    S4P::logger("INFO", "wipe_request(): Request stub file: $req_stub_dir/$req deleted."); 
                    write_expiration($file_ur, $expiration_file) if $expiration_file;
                } else {
                    S4P::logger("ERROR", "wipe_request(): Failed to delete Request stub file: $req_stub_dir/$req: $!");
                }
                last;
            } else {
                S4P::logger("DEBUG", "wipe_request(): [$req_ur] is NOT equal to [$file_ur]. Therefore, NOT deleting a request stub file.");
            }
        }
    }

    closedir(DIR) or S4P::perish(70, "wipe_request(): Cannot closedir: [$req_stub_dir]: $!");
}

sub write_expiration {
    my ($ur, $expiration_file) = @_;
    open EXPIRE, ">>$expiration_file" or 
        S4P::logger('ERROR', "Cannot update expiration file $expiration_file: $!");
    print EXPIRE "$ur\n";
    close EXPIRE;
}

sub get_metadata_shortname {

    my $handle = shift;

    my @needed_objects = ('SHORTNAME');
    my $met = S4PM::Handles::get_metadata_from_handle($handle);
    my %found_objects = S4P::MetFile::get_from_met($met, @needed_objects);

    unless ( exists $found_objects{'SHORTNAME'} ) {
        S4P::perish(40, "get_metadata_shortname(): Failed to locate SHORTNAME in handle file: $handle");
    }

    return $found_objects{'SHORTNAME'};
}
