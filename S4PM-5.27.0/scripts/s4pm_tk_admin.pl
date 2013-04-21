#!/usr/bin/perl

=head1 NAME

s4pm_tk_admin.pl - Tk tool to perform various S4PM maintenance activities

=head1 SYNOPSIS

s4pm_tk_admin.pl
[B<-l[og]>]
[B<[-t[ask]> I<task>]
[B<[-f[orce]> I<force>]
[B<-m[ulti]>]

=head1 DESCRIPTION

B<s4pm_tk_admin.pl> is a tool for performing various S4PM management or 
maintenance tasks. This tool must be invoked from the stations directory.

If the B<-l[og]> is specified, a log file will be generated containing all
messages that appear on the screen during running.

If B<-m[ulti]> is set, the script will treat the string as a multi-user 
string.

This script can be run non-interactively by using the B<-t[ask]> and specifying
a task. Currently, valid choices are FullCleanOut, Archive, PH, Blocks, Data,
ExtraFiles, FailedJobs, ResetDatabases, StationLogFiles, and OtherLogFiles.  
Only one task can be run at a time. Output normally going to the window will go 
to STDOUT instead.

The B<-f[orce]> flag turns off the warning banner and prompt for a response
that normally precedes running the task indicated with the B<-t[ask]>
argument. Thus, it will just run. So BE CAREFUL!

The current tasks that can be performed are:

=over 4

=item StationLogFiles - Clean out station log files

Cleans out all station.log and station_counter.log files from all station 
directories. The Subscription Notify station is NOT touched.

=item OtherLogFiles - Clean out and backup of other log files

Cleans out the find_data.log and cbr.log files after backing them up and
compressing them. 

=item ResetDatabases - Reset databases

Cleans out the Track Data and Allocate Disk databases; and deletes the 
Allocated Disk database transaction log file.

=item FailedJobs - Removed Failed Jobs

Cleans out all failed job directories from all stations. The Subscription 
Notify station is not touched.

=item ExtraFiles - Find and Delete Extra Files

Locates unrecognized files and gives the user the opportunity to delete them
on a case-by-case basis.

=item Archive - Clean Archive

Cleans out all files under the stations/ARCHIVE directory.

=item PH - Clean out Production History Files

Cleans out all Production History (PH) tar files from the data area.

=item Blocks - Clean Out Blocks

Cleans out all existing blocks in Register Local Data, Register Data, and Export 
stations.

=item FullCleanOut - Clean out all S4PM

This is a full clean up of all of S4PM. It does everything above plus 
everything else. This includes cleaning up any zombie running processes (i.e.
RUNNING.* directories without job.status files.

=back

=head1 AUTHOR

Stephen Berrick, SSAI/NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_admin.pl,v 1.9 2008/04/24 14:37:15 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Long;
use S4P::S4PTk;
use S4P;
use S4PM;
use Tk;
use Tk::ROText;
use Tk::Balloon;
use Cwd;
use File::Basename;
use Safe;
use Sys::Hostname;
require 5.6.0;

################################################################################
# Global variables                                                             #
################################################################################

use vars qw(%VALUES
            $MAIN
            $LOGFN
            $TEXT_WINDOW
            $USELOG
            @STATIONS
            $TASK
            $FORCE
            $HELP
            $DATETIME
            $MULTIUSER
           );

$USELOG = undef;

$LOGFN = "s4pm_tk_admin.$$.log";

%VALUES = ();	# Holds the checkbox settings

$TASK = undef;

$FORCE = undef;

$HELP = undef;

$MULTIUSER = undef;

$DATETIME = `date '+%Y-%m-%dT%T'`;
chomp($DATETIME);

################################################################################

# Read in the command line arguments

GetOptions( "log"    => \$USELOG,
            "task=s" => \$TASK,
            "force"  => \$FORCE,
            "help"   => \$HELP,
            "multi"  => \$MULTIUSER,
          );

if ( $HELP ) {
    print "\nUsage: s4pm_tk_admin.pl [ -l[og] ] [ -t[ask] task ] [ -f[orce] ]\n";
    print "\n\twhere task is one of: FullCleanOut, Archive, PH, Blocks, Data,\n";
    print "\t                      ExtraFiles, FailedJobs, ResetDatabases,\n";
    print "\t                      StationLogFiles, and OtherLogFiles\n\n";
    exit 0;
}

if ( $USELOG ) {
    print "USELOG defined.\n";
    open(LOG, ">$LOGFN") or die "Cannot open log file for writing: $LOGFN: $!";
}

my ($machine, $mode, $instance, $gear, $str) = get_instance();

# Fill up the @STATIONS array

opendir(CUR, ".") or errorbox("Opendir Failure", "Failed to opendir current working directory: $!");
while ( defined(my $file = readdir(CUR)) ) {
    next if ( $file eq "sub_notify" );		# sub_notify is special 
    next if ( $file eq "split_services" );	# split_services is special 
    next if ( $file =~ /^repeat_/ );		# repeat_* stations are special 
    next unless ( -e "$file/station.cfg" );	# ignore any non-station dirs
    push(@STATIONS, $file);
}

if ( $TASK ) {

    my ($machine, $mode, $instance, $gear, $str) = get_instance();
    my $ans;
    if ( $FORCE ) {
        $ans = "Y";
    } else {
        print "\n";
        print "################################################################################\n";
        print "                                   WARNING!!!\nTask '$TASK' will be done on $str.\n";
        print "################################################################################\n";
        print "\nContinue? (y/n) [n] ";
        $ans = <STDIN>;
        chomp($ans);
    }

    if ( $ans eq "Y" or $ans eq "y" ) {
        my $res = noninteractive($TASK);
        print "\nThe result of task '$TASK' was: $res\n\n";
        exit 0;
    } else {
        print "\nOk. Good-bye.\n\n";
        exit 0;
    }
}

$MAIN = MainWindow->new();

$MAIN->title("S4PM Administration Tool: $str");
S4P::S4PTk::read_options($MAIN);

my $intro_msg = "Select the task(s) you wish to run and then click on the 'Submit' button. To find out more about a particular task, click on the 'Question Head' button.";

my $label_frame = $MAIN->Frame->pack(-expand => 1, 
                                     -fill   => 'both', 
                                     -pady   => 20, 
                                     -padx   => 20,
                                    );

my $label = $label_frame->Label(-wraplength => 800, 
                                -justify    => 'left', 
                                -text       => $intro_msg, 
                                -relief     => 'flat',
                                -background => 'white',
                                -foreground => 'blue',
                               )->grid( -row        => 0, 
                                        -columnspan => 2, 
                                        -sticky     => 'nsew',
                                      );

# Now for the checkbox items

add_checkbox("StationLogFiles", 
             "Clean Station Log Files", 
             "Remove all station.log and station_counter.log files from all station directories (except Subscription Notify).", 
             "normal");

add_checkbox("OtherLogFiles",
             "Clean Other Log Files",
             "Clean out find_data.log and cbr.log files after backing them up and compressing them.",
             "normal");

add_checkbox("FailedJobs", 
             "Clean Failed Jobs", 
             "Remove all failed jobs from all stations (except Subscription Notify). Note that this task will delete failed jobs in the Run Algorithm station preventing the ability to punt them.", 
             "normal");

add_checkbox("ResetDatabases", 
             "Reset Databases", 
             "Reset the Track Data and Allocate Disk databases to an empty state and delete the Allocate Disk transaction log. The data area will be cleaned out as well).", 
             "normal");

add_checkbox("ExtraFiles", 
             "Delete Unrecognized Files", 
             "Locate and optionally delete unrecognized files from all of the station directories. You will be asked to confirm the deletion of each file as it is found.", 
             "normal");

add_checkbox("Archive", 
             "Clean Out ARCHIVE", 
             "Clean out all files under the ARCHIVE directory. This includes PDRs, archived PH files, and log files for Sweep Data, Export, and Receive PAN, etc.", 
             "normal");

add_checkbox("PH", 
             "Clean Out PH Files", 
             "Clean out all Production History (PH) tar files files under the DATA/PH directory.",   
             "normal");

add_checkbox("Blocks", 
             "Clean Out Blocks", 
             "Clean out all time-based blocks in Register Local Data, Register Data, and Export stations",   
             "normal");

add_checkbox("FullCleanOut", 
             "Full Clean Out", 
             "This is a full clean out of S4PM. It resets S4PM to the pristine state it had when first deployed. The activities include:\n\n- Deletion of all pending work orders\n\n- Deletion of all failed job directories\n\n- Deletion of all files in the ARCHIVE directory\n\n- Deletion of all data, PDRs, and PANs\n\n-" .
             "Reset of the Track Data and Allocate Disk databases and removal of the transaction log\n\n- Deletion of all station.log and station_counter.log files\n\n- Deletion of all request/acquire stub files\n\n- Deletion of all time-based blocks\n\n- Search for unrecognized files and, optionally, delete them",
             "normal");

# Add the Submit, Select All, and Reset buttons

add_buttons();

# Now add the text frame window

my $text_frame = $MAIN->Frame->pack(-expand => 1, -fill => 'both');
$TEXT_WINDOW = $text_frame->Scrolled("ROText")->pack(-expand => 1, -fill => 'both');

# And finally, an exit button

my $exit_frame = $MAIN->Frame->pack(-expand => 1, -fill => 'both');
my $exit_button = $exit_frame->Button(-text => 'Exit',
        -command => sub { goodbye(); },)->pack(-side => 'top', 
        -expand => 1, -padx => 10, -pady => 5);


# Have all logger messages redirected to pop-up boxes

S4P::S4PTk::redirect_logger($MAIN);

MainLoop();

################################################################################
# Subroutines                                                                  #
################################################################################

sub add_buttons {

    my $button_frame    = $MAIN->Frame->pack(-expand => 1, -fill => 'both');

    my $submit_button = $button_frame->Button(-text => 'Submit', 
        -command => \&submit,)->pack(-side => 'left', -expand => 1,
        -fill => 'both', -padx => 10, -pady => 5);
    my $balloon1 = $button_frame->Balloon();
    $balloon1->attach($submit_button, -balloonmsg => "Submit the selected tasks");


    my $select_button = $button_frame->Button(-text => 'Select All', 
        -command => \&select_all_checkboxes,)->pack(-side => 'left', 
        -expand => 1, -fill => 'both', -padx => 10, -pady => 5);
    my $balloon2 = $button_frame->Balloon();
    $balloon2->attach($select_button, -balloonmsg => "Select all tasks");

    my $reset_button = $button_frame->Button(-text => 'Reset',
        -command => \&reset_checkboxes,)->pack(-side => 'left', -expand => 1,
        -fill => 'both', -padx => 10, -pady => 5);
    my $balloon3 = $button_frame->Balloon();
    $balloon3->attach($reset_button, -balloonmsg => "Deselect all tasks");

}

sub add_checkbox {
    
    my($name, $description, $help, $state) = @_;

    $VALUES{$name} = 0;
    my $checkbox_frame = $MAIN->Frame->pack(-expand => 1, -fill => 'both');

    my $button = $checkbox_frame->Button(-bitmap => 'questhead',
        -command => sub { infobox("Help for $name", "$help"); })->pack
            (-side => 'left', -padx => 20);
    my $balloon = $button->Balloon();
    $balloon->attach($button, -balloonmsg => "Click for a full description of this task");

    $checkbox_frame->Checkbutton(-text=> $description,
        -variable => \$VALUES{$name}, -justify => 'left', 
        -command => [\&verify_checkbox_logic, $name],
        -highlightthickness => 0, -state => $state,)->pack(-side => 'left');

}

sub Archive {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out all files in ARCHIVE                                            #\n################################################################################\n\n");

    my $on_demand;
    if ( -e "$s4pm/ARCHIVE/ORDERS" ) { $on_demand = 1; }

    write_output("Eradicating $s4pm/ARCHIVE directory...\n\n");
    eradicate("$s4pm/ARCHIVE");
    write_output("Now, reconstituting all of $s4pm/ARCHIVE anew...\n\n");
    umask(022);

    unless ( make_dir("$s4pm/ARCHIVE", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE: $!");
        $status = "ERRORS";
    }
    if ( $on_demand ) {
        write_output("Reconstituting $s4pm/ARCHIVE/ORDERS...\n\n");
        unless ( make_dir("$s4pm/ARCHIVE/ORDERS", "0755") ) {
            errorbox("Failed to mkdir $s4pm/ARCHIVE/ORDERS: $!");
            $status = "ERRORS";
        }
    }
    write_output("Reconstituting $s4pm/ARCHIVE/PDR...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/PDR", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/PDR: $!");
        $status = "ERRORS";
    }
    write_output("Reconstituting $s4pm/ARCHIVE/PH...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/PH", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/PH: $!");
        $status = "ERRORS";
    }
    write_output("Reconstituting $s4pm/ARCHIVE/logs...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/logs", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/logs: $!");
        $status = "ERRORS";
    }
    if ( $on_demand ) {
         write_output("Reconstituting $s4pm/ARCHIVE/logs/track_requests...\n\n");
         unless ( make_dir("$s4pm/ARCHIVE/logs/track_requests", "0755") ) {
             errorbox("Failed to mkdir $s4pm/ARCHIVE/logs/track_requests: $!");
             $status = "ERRORS";
         }
    }
    write_output("Reconstituting $s4pm/ARCHIVE/logs/sweep_data...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/logs/sweep_data", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/logs/sweep_data: $!");
        $status = "ERRORS";
    }
    write_output("Reconstituting $s4pm/ARCHIVE/logs/export...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/logs/export", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/logs/export: $!");
        $status = "ERRORS";
    }
    write_output("Reconstituting $s4pm/ARCHIVE/logs/receive_pan...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/logs/receive_pan", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/logs/receive_pan: $!");
        $status = "ERRORS";
    }
    write_output("Reconstituting $s4pm/ARCHIVE/REQUESTS...\n\n");
    unless ( make_dir("$s4pm/ARCHIVE/REQUESTS", "0755") ) {
        errorbox("Failed to mkdir $s4pm/ARCHIVE/REQUESTS: $!");
        $status = "ERRORS";
    }

    return $status;
}

sub Blocks {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out all time-based blocks                                           #\n################################################################################\n\n");

    write_output("Eradicating $s4pm/register_local_data/Blocks directory...\n\n");
    eradicate("$s4pm/register_local_data/Blocks");
    write_output("Eradicating $s4pm/register_data/Blocks directory...\n\n");
    eradicate("$s4pm/register_data/Blocks");
    write_output("Eradicating $s4pm/export/Blocks directory...\n\n");
    eradicate("$s4pm/export/Blocks");

    return $status;
}

sub eradicate {

    my $dir = shift;

    my ($rs, $rc) = S4P::exec_system("/bin/rm -fr $dir");
    if ( $rc ) {
        errorbox("Directory Deletion Failed", "Failed to remove directory $dir with this error message: $rs");
    }
        

}

sub Data {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out all data files                                                  #\n################################################################################\n\n");

    my @ESDTs = glob("$s4ins/*");

    foreach my $ESDT (@ESDTs) {
        next unless ( -d $ESDT ); 
        next if ( $ESDT =~ /hold/i );
        write_output("Eradicating $ESDT directory...\n\n");
        eradicate("$ESDT");
        write_output("Now, reconstituting $ESDT anew...\n\n");
        umask(002);
        unless ( make_dir("$ESDT", "0775") ) {
            errorbox("Failed to mkdir $ESDT: $!");
            $status = "ERRORS";
        }
    }

    return $status;

}

sub errorbox {

    my ($title, $msg) = @_;

    if ( $TASK ) {
        print "\nERROR! $msg\n";
    } else {
        $MAIN->messageBox(-title => "ERROR! $title",
                          -message => "ERROR!\n\n$msg",
                          -type    => "OK",
                          -icon    => 'error',
                          -default => 'ok',
                         );
    }

}

sub ExtraFiles {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";
    my %delete_files = ();
    my $delete = 0;

    write_output("\n\n################################################################################\n# Looking now for unrecognized files that you may want to delete               #\n################################################################################\n\n");

    foreach my $station ( @STATIONS ) {
        unless ( opendir(DIR, "$s4pm/$station") ) {
            errorbox("Opendir Failed", "Failed to opendir $s4pm/$station: $!");
            return "ERRORS";
        }
        while ( defined(my $file = readdir(DIR)) ) {
            next if ( skip("$s4pm/$station/$file") );
            $delete_files{"$s4pm/$station/$file"} = 0;

        }
    }

    unless ( (keys %delete_files) ) {
        write_output("No unrecognized files were found.");
        infobox("Clean Out Of Unrecognized Files", "No unrecognized files were found.");
        return "SUCCESS";
    }

    if ( $TASK ) {
        print "\nThese files were not recognized and you may want to delete them: \n\n";
        foreach my $key ( keys %delete_files ) {
            print "\t$key\n";
            print "\t\tDelete? (y/n) [n] ";
            my $ans = <STDIN>;
            chomp($ans);
            if ( $ans eq "y" or $ans eq "Y" ) {
                unless ( unlink($key) ) {
                    write_output("Failed to delete $key: $!" . "\n");
                } else {
                    write_output("Successfully deleted $key\n");
                }
            } else {
                print "... Ok. Skipping.\n";
            }
        }
        print "\n";
    } else {
        my $top = $MAIN->Toplevel(-takefocus => 1);
        $top->title("Clean Out Of Unrecognized Files");
        my $msg = "Below are files that I do not recognize. Select those that you want me to delete and then click 'Delete Selected Files'. Otherwise, select 'Cancel' to cancel.                   ";
        my $label = $top->Label(-wraplength => 500, -justify => 'left', 
            -text => $msg, -relief => 'flat')->pack(-padx => 20, -pady => 10);

        foreach my $file ( keys %delete_files ) {
            $top->Checkbutton(-text => $file,
                -variable => \$delete_files{$file}, -justify => 'left', 
                -highlightthickness => 0, -state => 'normal')->pack(
                -anchor => 'w', -padx => 10, -pady => 5);
        }
        my $delete_button = $top->Button(-text => 'Delete Selected Files', 
            -command => sub { $delete = 1; $top->destroy; })->pack(-side => 'left',
            -padx => 20, -pady => 20);
        my $cancel_button = $top->Button(-text => 'Cancel', 
            -command => sub { $top->destroy })->pack(-side => 'left',
            -padx => 20, -pady => 20);
        $top->grab;
        $top->waitWindow;

        if ( $delete ) {
            my $kounter = 0;
            foreach my $file ( keys %delete_files ) {
                if ( $delete_files{$file} ) {
                    unless ( unlink($file) ) {
                        $status = "ERRORS";
                        write_output("Failed to delete $file: $!" . "\n");
                    } else {
                        write_output("Successfully deleted $file\n");
                        $kounter++;
                    }
                }
            }
            infobox("Files Deleted", "$kounter files successfully deleted.");
        } else {
            write_output("Deletion of unrecognized files was cancelled by user.\n");
            $status = "CANCELLED";
        }
    }

    return $status;

}

sub FailedJobs {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out failed jobs                                                     #\n################################################################################\n\n");

    foreach my $station ( @STATIONS ) {
        if ( wipe_files("$s4pm/$station", "FAILED.*") ) {
            $status = "WARNINGS";
        }
    }

    return $status;
}

sub FullCleanOut {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

### In on-demand processing, clean out any active requests

    if ( -e "$s4pm/track_requests/ACTIVE_REQUESTS" ) {

        write_output("\n\n################################################################################\n# Removing all active requests                                                 #\n################################################################################\n\n");
        write_output("Eradicating $s4pm/track_requests/ACTIVE_REQUESTS...\n\n");
        eradicate("$s4pm/track_requests/ACTIVE_REQUESTS");
        umask(002);
        unless ( make_dir("$s4pm/track_requests/ACTIVE_REQUESTS", "0775") ) {
            errorbox("Failed to mkdir $s4pm/track_requests/ACTIVE_REQUESTS: $!");
            $status = "ERRORS";
        }
    }
    
### Clean out data files
    
    $status = Data($s4pm, $s4ins);

### Clean out PANs and PDRs

    write_output("\n\n################################################################################\n# Cleaning out PDRs and PANs                                                   #\n################################################################################\n\n");

### PDR locations are specified in the %datatype_destination hash of the
### s4pm_export.cfg file.

    my $compartment = new Safe 'CFG';
    $compartment->share('%datatype_destination');
    my $export_cfg = "export/s4pm_export.cfg";
    $compartment->rdo($export_cfg) or S4P::perish("FullCleanOut(): Failed to import Export station configuration file: $export_cfg: $!");
    foreach my $dir ( values %CFG::datatype_destination ) {
        if ( wipe_files($dir, "*") ) {
            $status = "WARNINGS";
        }
    }

### Clean out request files from Request/Acquire Data stations

    write_output("\n\n################################################################################\n# Cleaning out data request stub files                                         #\n################################################################################\n\n");

    if ( wipe_files("$s4pm/request_data/REQUESTS", "*.req") ) {
        $status = "WARNINGS";
    }
    if ( wipe_files("$s4pm/acquire_data/ACQUIRES", "*.req") ) {
        $status = "WARNINGS";
    }

### Clean out unprocessed (pending) work orders

    write_output("\n\n################################################################################\n# Cleaning out unprocessed (pending) work orders                               #\n################################################################################\n\n");

    if ( wipe_files("$s4pm/receive_dn", "S4PM.DN.*") ) {
        $status = "WARNINGS";
    }
    foreach my $station ( @STATIONS ) {
        if ( wipe_files("$s4pm/$station", "DO.*.wo") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/$station", "DO.*.pcf") ) {
            $status = "WARNINGS";
        }
    }

### Clean out failed jobs

    $status = FailedJobs($s4pm, $s4ins);

### Clean out any zombie jobs

    $status = ZombieJobs($s4pm, $s4ins);

### Clean out various log files

    write_output("\n\n################################################################################\n# Cleaning out various station log files                                       #\n################################################################################\n\n");

    foreach my $station ( @STATIONS ) {
        if ( wipe_files("$s4pm/$station", "*MoPGE*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/$station", "*MyPGE*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/$station", "*Ai*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/$station", "INSERT*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/sweep_data", "SWEEP*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/export", "EXPORT_PH*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/export", "EXPORT.*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_data", "UPDATE.*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_data", "EXPECT.*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/find_data", "FIND_*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/allocate_disk", "ALLOCATE*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/register_data", "REGISTER*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/prepare_run", "PREPARE*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/select_data", "SELECT*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_requests", "ORDER_FAILURE*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_requests", "TRACK_REQUEST*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_requests", "REQUEST_DATA*.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/track_requests", "EXPORT.*.log") ) {
            $status = "WARNINGS";
        }
    }

### Clean out station log files

    $status = StationLogFiles($s4pm, $s4ins);

### Clean out other log files

    $status = OtherLogFiles($s4pm, $s4ins);

### Clean out archived files

    $status = Archive($s4pm, $s4ins);

### Clean out any blocks

    $status = Blocks($s4pm, $s4ins);

### Clean out the databases

    $status = ResetDatabases($s4pm, $s4ins);

### Look for extra unrecognized files lying about

    unless ( $FORCE ) {
        $status = ExtraFiles($s4pm, $s4ins);
        if ( $status eq "CANCELLED" ) { $status = "SUCCESS"; }
    }

    write_output("\n\n################################################################################\n################################################################################\n#                                  Complete!                                   #\n################################################################################\n################################################################################\n\n");

    return $status;

}

sub get_instance {

    my ($machine, $mode, $instance, $gear) = S4PM::get_box_mode_instance_gear();

    if ( $mode eq "ERROR" ) {
        $machine = hostname();
        return (undef, undef, undef, undef, $machine);
    }

    my $g = $gear;
    $g = ucfirst($gear);
    my $i = $instance;
    $i =~ s/_/ /;
    $i =~ tr/a-z/A-Z/;
    $i =~ s/TERRA/Terra/;
    $i =~ s/AQUA/Aqua/;
    my $str = "$machine in $i $g in $mode mode";

    return ($machine, $mode, $instance, $gear, $str);
}

sub infobox {

    my ($title, $msg) = @_;

    if ( $TASK ) {
        print "INFO: $msg\n";
    } else {
        $MAIN->messageBox(-title => "$title",
                          -message => "$msg",
                          -type    => "OK",
                          -icon    => 'info',
                          -default => 'ok',
                         );
    }
}

sub noninteractive {

    my $task = shift;

    my ($machine, $mode, $instance, $gear, $str) = get_instance();

    my $s4pm  = cwd();
    my $s4ins = "$s4pm/DATA";

    my $result;
    if ( $task eq "FullCleanOut" ) {
        $result = FullCleanOut($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "FailedJobs" ) {
        $result = FailedJobs($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "ExtraFiles" ) {
        $result = ExtraFiles($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "Archive" ) {
        $result = Archive($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "PH" ) {
        $result = PH($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "Blocks" ) {
        $result = Blocks($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "Data" ) {
        $result = Data($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "ResetDatabases" ) {
        $result = ResetDatabases($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "StationLogFiles" ) {
        $result = StationLogFiles($s4pm, $s4ins);
        return $result;
    } elsif ( $task eq "OtherLogFiles" ) {
        $result = OtherLogFiles($s4pm, $s4ins);
        return $result;
    } else {
        errorbox(undef, "I do not recognize that task: '$task'. Valid tasks are:\n\n\t\tFullCleanOut\n\t\tArchive\n\t\tPH\n\t\tBlocks\n\t\tData\n\t\ttExtraFiles\n\t\tFailedJobs\n\t\tResetDatabases\n\t\tStationLogFiles and\n\t\tOtherLogFiles");
        return "ERROR";
    }
}

sub PH {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("Eradicating $s4ins/PH directory...\n\n");
    eradicate("$s4ins/PH");
    write_output("Now, reconstituting $s4ins/PH anew...\n\n");
    umask(002);
    unless ( make_dir("$s4ins/PH", "0775") ) {
        errorbox("Failed to mkdir $s4ins/PH: $!");
        $status = "ERRORS";
    }

    return $status;
}

sub skip {

    my $path = shift;

    my ($file, $dir) = fileparse($path);

    my @skip_patterns = qw(\.pl$ \.cfg$ \.log$ \.db$ \.lock$ \.gz$ \.ksh$
                           ^tk?\.ksh$ nohup\.out acquire ACQParmfile
                           README.* ^\. ^STATS_ ^DO\. RELEASE_JOB_NOW
                           EXPIRE_CURRENT_TIMER rollup_stats\.ksh
                           TROUBLESHOOTING ACME_TROUBLESHOOTING VERSION
                           transaction.log S4PM_(TS1|TS2|OPS)
                           check_n_spec.ksh station.pid 
                           s4pm_configurator.cfg.backup oldlist.txt
                          );

    if ( -d $path ) {
        return 1;
    }

    if ( $file eq "." or $file eq "..") {
        return 1;
    }

### Some exceptions to the rule that all *.pl files are OK

    if ( $file =~ /^.*\.working\.pl$/i ) { return 0; }
    if ( $file =~ /^.*_working\.pl$/i ) { return 0; }
    if ( $file =~ /^.*\.baseline\.pl$/i ) { return 0; }
    if ( $file =~ /^.*_baseline\.pl$/i ) { return 0; }
    if ( $file =~ /^.*_te\.pl$/i ) { return 0; }
    if ( $file =~ /^.*\.te\.pl$/i ) { return 0; }

    foreach my $pattern ( @skip_patterns ) {
        if ( $file =~ /$pattern/i ) {
            return 1;
        }
    }

    return 0;

}

sub ResetDatabases {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    my $topline   = "GranuleID,Location,Uses";
    my $path_db   = "$s4pm/track_data/path.db";
    my $uses_db   = "$s4pm/track_data/uses.db";
    my $expect_db = "$s4pm/track_data/expect.db";

    write_output("\n\n################################################################################\n# Resetting the Track Data database                                            #\n################################################################################\n\n");

    if ( -e "$path_db" ) {
        unless ( unlink("$path_db") ) {
            $status = "FAILED";
            write_output("Failed to unlink pre-existing Track Data DBM file: $path_db: $!\n\n");
            return $status;
        } else {
            write_output("Successfully deleted pre-existing Track Data DBM file: $path_db\n\n");
        }
    } else {
        write_output("No pre-existing $path_db found. Good, I won't have to delete it.\n\n");
    }

    if ( -e "$uses_db" ) {
        unless ( unlink("$uses_db") ) {
            $status = "FAILED";
            write_output("Failed to unlink pre-existing Track Data DBM file: $uses_db: $!\n\n");
            return $status;
        } else {
            write_output("Successfully deleted pre-existing Track Data DBM file: $uses_db\n\n");
        }
    } else {
        write_output("No pre-existing $uses_db found. Good, I won't have to delete it.\n\n");
    }

    if ( -e "$expect_db" ) {
        unless ( unlink("$expect_db") ) {
            $status = "FAILED";
            write_output("Failed to unlink pre-existing Track Data DBM file: $expect_db: $!\n\n");
            return $status;
        } else {
            write_output("Successfully deleted pre-existing Track Data DBM file: $expect_db\n\n");
        }
    } else {
        write_output("No pre-existing $expect_db found. Good, I won't have to delete it.\n\n");
    }

### Second, reset the Allocate Disk database file

    write_output("\n\n################################################################################\n# Resetting the Allocate Disk database                                         #\n################################################################################\n\n");

    my $allocdisk_db = "$s4pm/allocate_disk/s4pm_allocate_disk.db";

    if ( -e "$allocdisk_db" ) {
        unless ( unlink("$allocdisk_db") ) {
            $status = "FAILED";
            write_output("Failed to unlink pre-existing Allocate Disk DBM file: $allocdisk_db: $!\n\n");
            return $status;
        } else {
            write_output("Successfully deleted pre-existing Allocate Disk DBM file: $allocdisk_db\n\n");
        }
    } else {
        write_output("No pre-existing $allocdisk_db found. Good, I won't have to delete it.\n\n");
    }

    my $cmd;
    if ( $MULTIUSER ) {
        $cmd = "umask 0002 && s4p_pooldb.pl -m -n $s4pm/allocate_disk/s4pm_allocate_disk_pool.cfg -d $allocdisk_db";
    } else {
        $cmd = "s4p_pooldb.pl -n $s4pm/allocate_disk/s4pm_allocate_disk_pool.cfg -d $allocdisk_db";
    }
    print "cmd: [$cmd]\n";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        $status = "FAILED";
        write_output("Failed to create new disk pool: $errstr\n\n");
        return $status;
    } else {
        write_output("Successfully created a new disk pool file: $allocdisk_db\n\n");
    }
    my ($errstr, $rc) = S4P::exec_system("lspooldb.pl $allocdisk_db");
    if ($rc) {
        $status = "FAILED";
        write_output("Failed to list out current disk pool settings: $errstr\n\n");
        return $status;
    }

### Get rid of the transaction log file

    write_output("\n\n################################################################################\n# Removing any current Allocate Disk database transaction log                  #\n################################################################################\n\n");

    my $tlog = "$s4pm/track_data/transaction.log";

    if ( -e "$tlog" ) {
        unless ( unlink("$tlog") ) {
            $status = "FAILED";
            write_output("Failed to unlink pre-existing Track Data transaction log file: $tlog: $!\n\n");
            return $status;
        } else {
            write_output("Pre-existing Track Data transaction log file successfully deleted: $tlog\n\n");
        }
    }

### Lastly, clean out data files
    
    $status = Data($s4pm, $s4ins);

    return $status;
}

sub OtherLogFiles {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out other log files after backing up                                #\n################################################################################\n\n");

    my $res;
    if ( -e "$s4pm/cbr.log" ) {
        $res = compress_and_backup("$s4pm/cbr.log");
        if ( $res ) { $status = "FAILED"; }
    }
    if ( -e "$s4pm/find_data/find_data.log" ) {
        $res = compress_and_backup("$s4pm/find_data/find_data.log");
        if ( $res ) { $status = "FAILED"; }
    }
    return $status;

}

sub compress_and_backup {

    my $file = shift;
    
    rename($file, "$file.$DATETIME");
    my ($rs, $rc) = S4P::exec_system("gzip $file.$DATETIME");
    if ( $rc ) {
        errorbox("File Compression Failed", "Failed to gzip file $file with this error message: $rs");
        return 1;
    }
    ($rs, $rc) = S4P::exec_system("touch $file");
    if ( $rc ) {
        errorbox("Touch File Failed", "Failed to touch file $file with this error message: $rs");
        return 1;
    }

    return 0;
}

sub StationLogFiles {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out station.log and station_counter.log files                       #\n################################################################################\n\n");

    foreach my $station ( @STATIONS ) {
        if ( wipe_files("$s4pm/$station", "station.log") ) {
            $status = "WARNINGS";
        }
        if ( wipe_files("$s4pm/$station", "station_counter.log") ) {
            $status = "WARNINGS";
        }
    }

    return $status;
}

sub wipe_files {

    my ($dir, $pattern) = @_;
    my $file;
    my $status = 0;

    my $ret = `echo $dir/$pattern`;

    my @filelist = split(/\s+/, $ret);

    foreach my $file (@filelist) {
        next if ( $file =~ /\.RECONFIG\./ );
        next if ( $file =~ /\.UPDATE_POOLS\./ );
        next if ( $file =~ /\.POLL\./ );
        next if ( $file =~ /\.pl$/ );
        next if ( $file =~ /\.cfg$/ );
        next if ( $file =~ /\.log$/ );
        if ( ! -e $file ) {
            write_output("File(s) with pattern: [$file] don't seem to exist.\nSkipping ...\n");
            next;
        }
        if ( unlink($file) ) {
            write_output("Successfully deleted: $file\n");
        } else {
            my $cmd = "/bin/rm -fr $file";
            my ($errstr, $rc) = S4P::exec_system("$cmd");
            if ( $rc ) {
                $status = 1;
                print "$errstr\n";
            }
        }
    }

    return $status;
}

sub write_output {

    my $str = shift;

    print LOG "$str" if ( $USELOG );
    if ( $TASK ) {
        print "$str\n";
    } else {
        $TEXT_WINDOW->insert("end", "$str");
        $TEXT_WINDOW->see('end');
        $TEXT_WINDOW->update;
    }

}

################################################################################
# Callbacks                                                                    #
################################################################################

sub goodbye {

    if ( $USELOG ) {
        close(LOG);
        unlink($LOGFN) or warn "Could not unlink $LOGFN: $!";
    }
    exit;

}

sub reset_checkboxes {

    foreach my $key ( keys %VALUES ) {
        $VALUES{$key} = 0;
    }
}

sub select_all_checkboxes {

    foreach my $key ( keys %VALUES ) {
        next if ( $key eq "FullCleanOut" );
        $VALUES{$key} = 1;
    }
}

sub submit {

    $MAIN->configure(-cursor => 'watch');

    my($machine, $mode, $instance, $gear, $str) = get_instance();

    if ( $mode eq "ERROR" ) {
        write_output("This directory is not a station directory. \n\nYou need to be in a S4PM station directory to run this.\n\n");
        errorbox("Not A Station Directory", "This directory is not a station directory.\n\nYou need to be in a S4PM station directory to run this.");
        goodbye();
    }

### Get verification that changes will be done in the intended instance of
### S4PM and in the correct mode.

    my $msg = "\nAbout to perform these tasks:\n\n";
    my $mode_msg = "\nWARNING!!!\nThe above tasks will be done on $str.\n";
    my $result_msg = "\nThese tasks have been completed with the following results:\n\n";
    my $selections = 0;

    if ( $mode eq 'ERROR' or $instance eq 'ERROR' or $gear eq 'ERROR' ) {
        S4P::perish(1, "You must run this from the stations directory.");
    }

    if ( $VALUES{'StationLogFiles'} == 1 ) {
        $msg .= " - Clean out all station log files.\n\n";
        $selections++;
    }

    if ( $VALUES{'OtherLogFiles'} == 1 ) {
        $msg .= " - Clean out and backup of other log files.\n\n";
        $selections++;
    }

    if ( $VALUES{'FailedJobs'} == 1 ) {
        $msg .= "- Remove all failed job directories.\n\n";
        $selections++;
    }

    if ( $VALUES{'ResetDatabases'} == 1 ) {
        $msg .= "- Reset Find Data and Allocate Disk databases.\n\n";
        $selections++;
    }

    if ( $VALUES{'ExtraFiles'} == 1 ) {
        $msg .= "- Locate and optionally delete unknown files.\n\n";
        $selections++;
    }

    if ( $VALUES{'Archive'} == 1 ) {
        $msg .= "- Remove all ARCHIVE files.\n\n";
        $selections++;
    }

    if ( $VALUES{'PH'} == 1 ) {
        $msg .= "- Remove all Production History files.\n\n";
        $selections++;
    }

    if ( $VALUES{'Blocks'} == 1 ) {
        $msg .= "- Remove all time-based blocks.\n\n";
        $selections++;
    }

    if ( $VALUES{'FullCleanOut'} == 1 ) {
        $msg .= "- Full clean out of S4PM (aka THE BIG ONE).\n\n";
        $selections++;
    }

    if ( $selections > 0 ) {
        my $dialog = $MAIN->DialogBox( -title => "Confirmation Box",
            -buttons => ['OK', 'Cancel', 'Quit Altogether']);
        $dialog->add('Label', -text => $msg)->
            pack(-side => 'top', -expand => 1);
        $dialog->add('Label', -text => $mode_msg, -background => 'white',
            -foreground => 'red', -wraplength => 300)->
            pack(-side => 'top', -expand => 1);
        my $answer = $dialog->Show();
        if ( $answer eq "Cancel" ) {
            write_output("Submission cancelled by user.\n\n");
            $MAIN->configure(-cursor => 'left_ptr');
            infobox("Submission Cancelled", "Submission cancelled. Submit again when ready.");
            return;
        }
        if ( $answer eq "Quit Altogether" ) {
            goodbye();
        }

    } else {
        write_output("User didn't select anything to submit. User was appropriately castigated.\n\n");
        $MAIN->configure(-cursor => 'left_ptr');
        errorbox("Empty Selection List", "Well, ah duh, you haven't selected anything for me to do.");
        return;
    }

### Now, do the work

    my $s4pm  = cwd();
    my $s4ins = "$s4pm/DATA";

    if ( $VALUES{'StationLogFiles'} == 1 ) {
        my $result = StationLogFiles($s4pm, $s4ins);
        $result_msg .= "Clean out of station logs - $result\n\n";
    }
    if ( $VALUES{'OtherLogFiles'} == 1 ) {
        my $result = OtherLogFiles($s4pm, $s4ins);
        $result_msg .= "Clean out and backup of other logs - $result\n\n";
    }
    if ( $VALUES{'FailedJobs'} == 1 ) {
        my $result = FailedJobs($s4pm, $s4ins);
        $result_msg .= "Removal of failed job directories - $result\n\n";
    }
    if ( $VALUES{'ResetDatabases'} == 1 ) {
        my $result = ResetDatabases($s4pm, $s4ins);
        $result_msg .= "Reset of databases - $result\n\n";
    }
    if ( $VALUES{'ExtraFiles'} == 1 ) {
        my $result = ExtraFiles($s4pm, $s4ins);
        $result_msg .= "Clean up of extra files - $result\n\n";
    }
    if ( $VALUES{'Archive'} == 1 ) {
        my $result = Archive($s4pm, $s4ins);
        $result_msg .= "Clean up of Archive files - $result\n\n";
    }
    if ( $VALUES{'PH'} == 1 ) {
        my $result = PH($s4pm, $s4ins);
        $result_msg .= "Clean up of PH files - $result\n\n";
    }
    if ( $VALUES{'Blocks'} == 1 ) {
        my $result = Blocks($s4pm, $s4ins);
        $result_msg .= "Remove time-based blocks- $result\n\n";
    }
    if ( $VALUES{'FullCleanOut'} == 1 ) {
        my $result = FullCleanOut($s4pm, $s4ins);
        $result_msg .= "Full clean out of S4PM - $result\n\n";
    }

    if ( $USELOG ) {
        infobox("Results", "$result_msg\n\nReview $LOGFN for logged results.");
        close LOG;
    } else {
        infobox("Results", "$result_msg");
    }

    $MAIN->configure(-cursor => 'left_ptr');

}

sub verify_checkbox_logic {

    my $name = shift;

    if ( $name eq "FullCleanOut" ) {
        reset_checkboxes();
        $VALUES{'FullCleanOut'} = 1;
    } else {
        $VALUES{'FullCleanOut'} = 0;
    }

}

sub ZombieJobs {

    my ($s4pm, $s4ins) = @_;
    my $status = "SUCCESSFUL";

    write_output("\n\n################################################################################\n# Cleaning out zombie jobs                                                     #\n################################################################################\n\n");

    foreach my $station ( @STATIONS ) {
        my @jobs = glob("$s4pm/$station/RUNNING*");      # Possible zombies
        foreach my $job ( @jobs ) {
            unless ( -e "$job/job.status" ) {           # If no job.status file, get rid of it
                my $cmd = "/bin/rm -fr $job";
                my ($errstr, $rc) = S4P::exec_system("$cmd");
                if ( $rc ) {
                    print "$errstr\n";
                    $status = "WARNINGS";
                }
            }
               
        }
    }

    return $status;
}

sub make_dir {

    my $item = shift;
    my $mode = shift;

    my @bits = split(//, $mode);

    if ( $MULTIUSER ) {
#       $bits[0] = 2;
        $bits[2] = $bits[1];
    }

    my $newmode = join("",@bits);

    my $cmd = "umask 0002 && mkdir -m $newmode $item";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        return 0;
    }

    return 1;   # Like UNIX mkdir, we return 1 if we succeed
}

