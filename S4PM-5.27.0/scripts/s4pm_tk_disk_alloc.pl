#!/usr/bin/perl

=head1 NAME

s4pm_tk_disk_alloc.pl - watches the Allocate Disk database

=head1 SYNOPSIS

s4pm_tk_disk_alloc.pl 
B<[-h]> 
[B<-d> I<allocdisk.db>] 
[B<-f> I<allocdisk.cfg>] 
[B<-p> I<allocdisk_pool.cfg>]
[B<-a> I<application_name>]
[B<-r> I<max_rows>]
[B<-g> I<format>]

=head1 DESCRIPTION

B<s4pm_tk_disk_alloc.pl> provides a graphical monitor for the Allocate Disk
database, which shows the available space in the data pools. 

For each pool, it shows how much of the pool has been allocated in terms of
files.  There is both a bar-type graphical display, as well as the actual
numbers of files used (allocated), left (space available), and total.
The color of the bar turns yellow at 70% used, orange at 85% and red at 95%.

=head1 ARGUMENTS

=over 4

=item B<-d> I<s4pm_allocate_disk.db>

The Allocate Disk database.  Default is ./s4pm_allocate_disk.db.

=item B<-f> I<s4pm_allocate_disk.cfg>

The Allocate Disk pool configuration file.  Default is 
./s4pm_allocate_disk_pool.cfg.

=item B<-p> I<s4pm_allocate_disk_pool.cfg>

The Allocate Disk pool configuration file.  Default is 
./s4pm_allocate_disk_pool.cfg.

=item B<-a> I<application_name>

Used to get around a bug in X resource handling by Perl/Tk.
Specifying different application names for different sets of resources
can avoid the problem where two instances of the same app cause only
the first to obtain the X resources.

=item B<-r> I<max_rows>

Maximum number of rows per column.

=item [B<-g> I<format>]

Show numbers in gigabytes, using the format specified (but without the % sign), e.g. '.3f' or 'd'.

=item [B<-x> I<exclude>]

Comma separated list of pools to be included from display (e.g., 'INPUT');

=item B<-h>

Prints usage statement.

=back

=head1 X RESOURCES

A number of X Resources can be used to override several parameters, such as
refresh rate and alert levels and colors.

=over 4

=item refreshRate (RefreshRate)

Rate in seconds at which application refreshes (default is 60 seconds).

=item alertLevel1 (AlertLevel1)

Percent usage at which first alert color (alertColor1) appears.
Default is 70.

=item alertColor1 (AlertColor1)

Color to use for first alert level (alertLevel1).  Default is #ffff00 (yellow).

=item alertLevel2 (AlertLevel2)

Percent usage at which second alert color (alertColor2) appears.
Default is 85.

=item alertColor2 (AlertColor2)

Color to use for second alert level (alertLevel2).  Default is orange.

=item alertLevel3 (AlertLevel3)

Percent usage at which third alert color (alertColor3) appears.
Default is 95.

=item alertColor3 (AlertColor3)

Color to use for third alert level (alertLevel2).  Default is red.

=item nominalColor (NominalColor)

Color to use below first alert level (alertLevel1). Default is navy.

=back

=head1 SEE ALSO

L<allocdisk_getdisk>.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_disk_alloc.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use DB_File;
use vars qw($opt_a $opt_d $opt_f $opt_g $opt_h $opt_p $opt_r $opt_x);
use Getopt::Std;
use Tk;
use Tk::ProgressBar;
use Tk::ROText;
use S4P::S4PTk;
use Safe;

# Process command line (some options exist, but are not advertised in man page
# as they are primarily for testing purposes.)

getopts('a:d:f:g:hp:r:x:');
usage() if $opt_h;
my $max_rows = $opt_r || 20;

# Read config file and process defaults

my $allocdisk_cfg = $opt_f || 's4pm_allocate_disk.cfg';
my $pool_cfg = $opt_p || 's4pm_allocate_disk_pool.cfg';
my $allocdisk_db = $opt_d || 's4pm_allocate_disk.db';
my %exclude = map {($_, 1)} split(/,/, $opt_x);

my $compartment1 = new Safe 'ALLOC';
$compartment1->share('%datatype_maxsize', '%datatype_pool_map', '%datatype_pool');
$compartment1->rdo($allocdisk_cfg) or
    S4P::perish(30, "main: Failed to read in configuration file $allocdisk_cfg in safe mode: $!");
my $compartment2 = new Safe 'POOL';
$compartment2->share('%pool_size');
$compartment2->rdo($pool_cfg) or
    S4P::perish(30, "main: Failed to read in configuration file $pool_cfg in safe mode: $!");

# Construct main window so we can get X defaults and logger redirect going

my $main_window = new MainWindow;
S4P::S4PTk::read_options($main_window);
S4P::S4PTk::redirect_logger($main_window);
$main_window->title("View Disk Allocation and Usage");
$main_window->appname($opt_a) if $opt_a;

# Set alert levels and colors, refresh rate

my $refresh_rate = $main_window->optionGet('refreshRate','RefreshRate') || 60;
my @colors;
$colors[0] = 0;
$colors[1] = $main_window->optionGet('nominalColor','NominalColor') || 'navy';
$colors[2] = $main_window->optionGet('alertLevel1','AlertLevel1') || 70;
$colors[3] = $main_window->optionGet('alertColor1','AlertColor1') || '#ffff00';
$colors[4] = $main_window->optionGet('alertLevel2','AlertLevel2') || 85;
$colors[5] = $main_window->optionGet('alertColor2','AlertColor2') || 'orange';
$colors[6] = $main_window->optionGet('alertLevel3','AlertLevel3') || 95;
$colors[7] = $main_window->optionGet('alertColor3','AlertColor3') || 'red';

# Create Main Window

my @frame;
my $n_pools = scalar(keys %ALLOC::datatype_pool);
my $n_cols = $n_pools / $max_rows + 1;
my ($used_header, $left_header, $max_header);
if ($opt_g) {
    $used_header = "GB\nUsed";
    $left_header = "GB\nLeft";
    $max_header = "Max\nGB";
}
else {
    $used_header = "Files\nUsed";
    $left_header = "Files\nLeft";
    $max_header = "Max\nFiles";
}
foreach my $i(0..($n_cols - 1)) {
    $frame[$i] = $main_window->Frame(-relief=>'groove',-borderwidth=>4);
    # Create header row with labels and grid
    my $esdt_label = $frame[$i]->Label(-text=>'Pool');
    my $bar_label = $frame[$i]->Label(-text=>'Disk Usage');
    my $used_label = $frame[$i]->Label(-text=>$used_header);
    my $left_label = $frame[$i]->Label(-text=>$left_header);
    my $max_label = $frame[$i]->Label(-text=>$max_header);
    $esdt_label->grid($bar_label, $used_label, $left_label, $max_label);
}

# Create pool rows

my (%pool_gransize, %pool_used, %pool_left);
my $i = 0;
foreach my $pool(sort keys %ALLOC::datatype_pool) {
    next if ($exclude{$pool});
    my $path = $ALLOC::datatype_pool{$pool};
    my $size = $POOL::pool_size{$path};
    my $max_size = 0;
    my $frame = $frame[$i % $n_cols];

    # Find the max size of a file in this pool
    # We'll use that to convert bytes to files
    my $max_units;
    if ($opt_g) {
        $max_units = sprintf("%$opt_g", $size / (1024*1024*1024));
    }
    else {
        foreach my $dt (keys %ALLOC::datatype_pool_map) {
            my $dt_pool = $ALLOC::datatype_pool_map{$dt};
            next if ($dt_pool ne $pool);
            $max_size = $ALLOC::datatype_maxsize{$dt} if ($ALLOC::datatype_maxsize{$dt} > $max_size);
        }
        $max_units = int($size / $max_size);
    }

    # Set up variable references for textvariable linkages in 
    my $current_units = 0;
    my $current_left = 0;

    # These hashes are used by the refresh() routine
    $pool_gransize{$path} = $opt_g ? (1024*1024*1024) : $max_size;
    $pool_used{$path} = \$current_units;
    $pool_left{$path} = \$current_left;
    make_pool_grid($frame, $pool, $max_units, \$current_units, \$current_left, \@colors);
    $i++;
}
my $frame = shift @frame;
$frame->grid(@frame, -sticky => 'ew');

# Make the grid elements resizable
foreach $i(0..($n_cols - 1)) {
    $main_window->gridColumnconfigure($i, -weight => 1);
}

# Command buttons at bottom

my $cmd_frame = $main_window->Frame();
my $refresh_cmd = [\&refresh, \%pool_gransize, \%pool_used, \%pool_left, \%POOL::pool_size, $allocdisk_db];
$cmd_frame->Button(-text=>'Refresh', -command => $refresh_cmd)->pack(-side=>'left',-anchor=>'w');
$cmd_frame->Button(-text=>'Exit', -command => sub{exit 0})->pack(-side=>'right',-anchor=>'e');
$cmd_frame->grid(-sticky => 'ew', -columnspan => $n_cols);

# Set up automatic refresh...
$main_window->repeat($refresh_rate * 1000, $refresh_cmd);
# ...and get things started with a refresh
refresh(\%pool_gransize, \%pool_used, \%pool_left, \%POOL::pool_size, $allocdisk_db);

MainLoop;
exit(0);

##############################################################################
# G U I   C O N S T R U C T I O N
##############################################################################

sub make_pool_grid {
    my ($parent, $pool, $max_units, $rs_current_units, $rs_current_left, $ra_colors) = @_;

    my @colors = @$ra_colors;
    $colors[2] *= ($max_units / 100);
    $colors[4] *= ($max_units / 100);
    $colors[6] *= ($max_units / 100);
    # One grid row for each pool:  Pool, Progress Bar, Used label, Left label, Total label
    my $label = $parent->Label(-text=>$pool);
    my $nblocks = ($max_units < 300) ? $max_units : ($max_units / 10);
    my $progress = $parent->ProgressBar(-borderwidth=>2, -troughcolor =>'grey65',
        -to => $max_units, -variable=>$rs_current_units, -length => 250,
        -blocks=>$nblocks, -gap => 1, -colors => \@colors);
    my $units_used = $parent->Label(-anchor => 'e', -textvariable => $rs_current_units);
    my $units_left = $parent->Label(-anchor => 'e', -textvariable => $rs_current_left);
    my $units_max = $parent->Label(-anchor => 'e', -text => $max_units);
    $label->grid($progress, $units_used, $units_left, $units_max, -sticky => 'ew');

    # Expand/contract ProgressBar if window is resized
    $parent->gridColumnconfigure(1, -weight => 1, -minsize=>50);
    return(1);
}    
sub refresh {
    my ($rh_gransize, $rh_used, $rh_left, $rh_pool_size, $allocdisk_db) = @_;
    my %left;
    # Open up database
    my $db = tie (%left, "DB_File", $allocdisk_db, O_RDONLY) or die ("Cannot open $allocdisk_db: $!");
    foreach my $pool(keys %$rh_gransize) {
        next if ($exclude{$pool});
        my $rs_used = $rh_used->{$pool};
        my $rs_left = $rh_left->{$pool};
        my $units_left = $left{$pool} / $rh_gransize->{$pool};
        my $units_used = ($rh_pool_size->{$pool} - $left{$pool}) / $rh_gransize->{$pool};
        if ($opt_g) {
            $$rs_left = sprintf("%$opt_g", $units_left);
            $$rs_used = sprintf("%$opt_g", $units_used);
        }
        else {
            $$rs_left = int($units_left);
            $$rs_used = int($units_used);
        }
    }
}
