#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_max_children.pl - manage S4PM max children settings

=head1 SYNOPSIS

s4pm_tk_max_children.pl 
[B<-t>]

=head1 DESCRIPTION

B<s4pm_tk_max_children.pl> presents the user with a list of stations from 
the I<s4pm_stringmaker_jobs.cfg> file which is assumed to reside in the
Configurator station directory. Information presented includes the S4PM 
instance, station, current %cfg_max_children setting, and an entry box for 
updating this field. 

Input into the GUI is validated by the Perl compiler when the update
button is clicked. Valid inputs are appended to the I<s4pm_stringmaker_jobs.cfg>
file.

Once inputs are validated, the I<s4pm_stringmaker_jobs.cfg> file in the 
Configurator station is modified with the updates along with time stamps. 
<string>_MAX_JOBS work orders are then dropped into Configurator station which 
sits across all strings and are processed accordingly.

Actual Stringmaking does not take place with this script. It is up to the 
Configurator station to process the <string>_MAX_JOBS work orders that 
it may have received before the changes are committed.

Note: This script will use $cfg_checkout and $cfg_checkin parameters set in
I<s4pm_configurator.cfg> to check out and check in the file 
I<s4pm_stringmaker_jobs.cfg>. If I<s4pm_stringmaker_jobs.cfg> is not checked
into any CM, then set $cfg_checkout and $cfg_checkin to empty strings.

The -t option is really only meant when this is run at the GES DAAC, but it
may be useful, after tweaking, to other organizations. Basically, it added
an extra button to have a Trouble Ticket submitted listing the strings that
have been modified. Unfortunately, this functionality assumes a particular
schema in Remedy. 

=head1 AUTHOR

Bruce Vollmer, NASA/GSFC, Code 610.2
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_max_children.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Tk;
use Env;
use Sys::Hostname;
use S4P;
use S4PM;
use S4PM::Configurator;
use Cwd;
use Getopt::Std;

use strict;
use vars qw ($rh_max_children
             %max_children
             $file
             $string_label
             $station_label
             $current_max
             $string_number
             %stringmaker
             $opt_t
);

getopts('t');

### Initialize stringmaker hash (contains strings that have been updated)

my %stringmaker = ();

# Get hostname

my $mymachine = hostname();

# The s4pm_stringmaker_jobs.cfg file is assumed to reside in the Configurator
# station directory (or a link to it).

# Read in s4pm_stringmaker_jobs.cfg config file

$file = 's4pm_stringmaker_jobs.cfg';
do $file or die "Error importing s4pm_stringmaker_jobs.cfg config file";

$rh_max_children = \%max_children;

my $main = MainWindow->new();

$main->title("Modify Max Jobs");

my $grid_frame = $main->Frame->grid();

# Create grid header *stringname, station, max children settings

my $string_header = $grid_frame -> Label(-text => "String Name",
                                         -width => '18',
                                         -relief => 'groove',
                                         -borderwidth => '6') ;

my $station_header = $grid_frame -> Label(-text => "Station",
                                          -width => '12',
                                          -relief => 'groove',
                                          -borderwidth => '6');

my $current_max_header = $grid_frame -> Label(-text => "Current Max",
                                              -width => '12',
                                              -relief => 'groove',
                                              -borderwidth => '6');

my $new_max_header = $grid_frame-> Label(-text => "New Max",
                                         -width => '8',
                                         -relief => 'groove',
                                         -borderwidth => '6');

$string_header->grid($station_header, $current_max_header, $new_max_header);

my $setting2;
my $current_max;
my %new_max_hash;

foreach my $string (sort keys %max_children) {

    my $machine = S4PM::Configurator::get_machine_name("s4pm_configurator.cfg", $string);

### Run for strings on this host only

    next unless($machine eq $mymachine);
     
    foreach my $station (sort keys %{$max_children{$string} }) {

        my $new_max;

        $current_max = $max_children{$string}{$station};

        $string_label = $grid_frame->Label(-text => "$string",  
                                           -width => '18',
                                           -anchor => 'e',            
                                           -relief => 'raised');
        $station_label = $grid_frame->Label(-text => "$station",
                                            -width => '12',
                                            -anchor => 'e',
                                            -relief => 'raised');
        $current_max = $grid_frame->Label(-text => "$current_max",
                                          -width => '12',
                                          -relief => 'sunken');
        $setting2 = $grid_frame->Entry(-width => '8',
                                       -textvariable => \$new_max);

        $string_label->grid($station_label, $current_max, $setting2); 

        $new_max_hash{$string}{$station} = \$new_max;

    }
}

################################################################################

my $button_frame = $main->Frame->grid();
 
my $update_button = $button_frame->Button(-text => "Update",
                    -background => 'black', -foreground => 'white',
                    -command => [\&update, \%new_max_hash])->pack(-side => 'left',
                    -fill => 'both', -padx => 10, -pady => 10);
 
my $ttexit_button;
my $exit_button;
if ( $opt_t ) {
    $ttexit_button = $button_frame->Button(-text => "Submit TT Then Exit", 
        -command => [\&submit_tt_then_exit, \%new_max_hash])->pack(
        -side => 'right', -fill => 'both', -padx => 10, -pady => 10);
    $exit_button = $button_frame->Button(-text => "Exit Only", 
        -command => sub { exit 0; }) ->pack(-side => 'right',
        -fill => 'both', -padx => 10, -pady => 10);
} else {
    $exit_button = $button_frame->Button(-text => "Exit", 
        -command => sub { exit 0; }) ->pack(-side => 'right',
        -fill => 'both', -padx => 10, -pady => 10);
}

MainLoop;

sub submit_tt_then_exit {

    my $ref_new_max_hash = shift;

    my %new_max_hash = %$ref_new_max_hash;
    my $string_list = "";
    my $home = $ENV{'HOME'};
    my $pwd = cwd();
    my $mode;

    if ( $pwd =~ /OPS/i ) { $mode = "OPS"; }
    if ( $pwd =~ /TS1/i ) { $mode = "TS1"; }
    if ( $pwd =~ /TS2/i ) { $mode = "TS2"; }

    my $prob_str = "The following changes were made in $mode via Tk_Max_Children GUI:\n\n";

    foreach my $string ( keys %new_max_hash ) {
        foreach my $station ( keys %{$new_max_hash{$string}} ) {
            my $r_nval = $new_max_hash{$string}{$station};
            my $nval = $$r_nval;
            my $oval = $max_children{$string}{$station};
            my $direction;
            if ( $nval > $oval ) {
                $direction = "increased";
            } elsif ( $nval < $oval ) {
                $direction = "decreased";
            } else {
                $direction  = "unchanged";
            }
            if ( $nval ) {
                $prob_str .= "    In $string, station $station was $direction from $oval to $nval max children.\n";
            }
        }
    }

    foreach my $string ( keys %stringmaker ) {
        $string_list .= " $string,";
    }
    if ( $string_list eq "" ) {
        infobox("Can't Submit Trouble Ticket", "A Trouble Ticket cannot be submitted since no S4PM instances have been modified.");
        exit 0;
    }

    infobox("Submitting A Trouble Ticket",
            "The Tk_Max_Children changes have completed successfully.\n\nI will now help you submit a Trouble Ticket to the Master Change Log. You will need your Remedy account userid and password for logging onto g0mss10.\n\nIf this part fails, remember that you do NOT have to run Tk_Max_Children again. You will, however, have to manually submit and close a Trouble Ticket.\n\nNow, click 'OK' then respond to the command line prompts in the xterm entitled 'Submit Trouble Ticket'.");

    print "\nEnter Remedy user name (it must already exist): ";
    my $user = <STDIN>;
    chomp $user;
    my $short_descrip = "MCL: Tk_Max_Children was run on:$string_list in $mode";

    my $tt_open_parms =<<EOF;
$user
L
A
$user
L
$short_descrip
$prob_str
.
Problem requires that String Master be run on the listed S4PM instances.
.
$user
bvollmer

EOF

    my $tt_close_parms =<<EOF;
C
Implemented
configuration
String Master was run.
.
S4PM

EOF

    print STDERR "\nSubmitting and closing Trouble Ticket to the Master Change Log on your behalf.\nYou'll be asked to enter in your password twice, so don't panic.\n\n";

    my $status = S4PM::submit_trouble_ticket($mode, $user, "g0mss10", $tt_open_parms, $tt_close_parms);

    if ( $status ) {
        infobox("ERROR!", "Submitting and closing a Trouble Ticket on your behalf has failed. I'm afraid that you will have to do so manually.");
    }
    exit 0;
  
}

sub update {

### Set reference to new max children hash

    my $ref_new_max_hash = shift;

    %new_max_hash = %$ref_new_max_hash;

### Loop through new max children entries for integrity check

    foreach my $string (sort keys %new_max_hash) {

        foreach my $station (sort keys %{$new_max_hash{$string} }) {

            my $rs_new_max = $new_max_hash{$string}{$station};

            my $new_max_value = $$rs_new_max;

##############  Check for invalid entries, decimals and NULL entries OK

            unless ($new_max_value =~ m/^\d{1,3}$/ or $new_max_value eq undef) {
                errorbox("INVALID ENTRY", "Entry must be a 1, 2 or 3-digit number between 1-999");
                return;
            }
        }
    }

### Get date and time stamp - time stamp current update in cfg file

    my $date = localtime(time);

    $file = "s4pm_stringmaker_jobs.cfg";

### First, check file out

    S4PM::Configurator::cm_checkout_file("s4pm_configurator.cfg", $file);

### Then, open file for appending information

    open(OUT,">>$file") or errorbox("FILE OPEN ERROR",
                        "Cannot open $file: file must be checked out of SCCS")
                        && return;

    print OUT "# FILE UPDATED\n\n";

### Initialize change counter

    my $change_counter = 0;

### Go through hash and write only new entries to s4pm_stringmaker_jobs.cfg

    my $output_wo_content = "";
    foreach my $string (sort keys %new_max_hash) {
        foreach my $station (sort keys %{$new_max_hash{$string} }) {
            my $rs_new_max = $new_max_hash{$string}{$station};
            my $new_max_value = $$rs_new_max;

########### Only write fields that have been updated

            if ($new_max_value =~ m/^\d{1,3}$/) {
                $change_counter++;
                printf OUT "     \$max_children{'%s'}{'%s'} = %d;\n", $string,$station,$new_max_value;
                $output_wo_content .= "$string $station $new_max_value\n";
                $stringmaker{$string} = 1;
            }
        }

    }

### Write number of changes made to cfg file

    print OUT "\n# UPDATE: $date : $change_counter changes made\n\n";

### Put the mystical "1" at the bottom of the cfg file and close the file   

    print OUT "1;\n";
    close OUT;

### Now, check the file back in

    S4PM::Configurator::cm_checkin_file("s4pm_configurator.cfg", $file);

### Compile updated s4pm_stringmaker_jobs.cfg file for integrity check 
### Show result of integrity check in popup box

    my $rc = system "perl -c $file";

    if ($rc) {
        errorbox("INTEGRITY CHECK",
             "Compile Error: Check contents of s4pm_stringmaker_jobs.cfg");
    }
    
### Loop through strings that have been updated and drop in work orders

### Write out output work order

    my $out_wo = "DO.MODIFY_MAX_JOBS.$$.wo";
    open(WO, ">$out_wo") or S4P::perish(30, "Failed to open output work order: $out_wo: $!");
    print WO $output_wo_content;
    infobox("Stringmaker Success", "This work order was dropped into the Configurator station:\n$output_wo_content");

}

sub infobox {

    my ($title, $msg) = @_;

    $main->messageBox(-title => "$title",
                      -message => "$msg",
                      -type    => "OK",
                      -icon    => 'info',
                      -default => 'ok',
                     );
}

sub errorbox {

    my ($title, $msg) = @_;

    $main->messageBox(-title => "ERROR! $title",
                      -message => "ERROR!\n\n$msg",
                      -type    => "OK",
                      -icon    => 'error',
                      -default => 'ok',
                     );
}

