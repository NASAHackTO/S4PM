#!/usr/bin/perl

=head1 NAME

s4pm_tk_pwbox.pl - pop-up box for user password before running command

=head1 SYNOPSIS

s4pm_pw_box.pl 
B<-c[ommand]> I<command>
B<-t[ask]> I<taskname>
[ B<-l[abel]> I<label> ]
[ B<-a[cl]> I<acl_file> ]

=head1 DESCRIPTION

This script displays a pop-up box prompting a user for a password before
running the command I<command>. If the password is correct, the command
I<command> is run; otherwise it is not. The username and password entered
must be a valid entry in the /etc/passwd file of the UNIX system on which
it is being run.

=head1 ARGUMENTS

=over 4

=item B<-c[ommand]> I<command>

This is the command to execute if the user is authorized to run this task
and the password entered by that user is correct.

=item B<-t[ask]> I<taskname>

This specifies name of a task for which a list of authorized users has been 
specified in the access control list.

=item B<-l[abel]> I<label>

This is used in the pop-up box a label for the task being executed. The 
default is to use I<taskname> as the label.

=item B<-a[cl]> I<acl_list>

This specifies the fullpath of the access control list file. This file is
assumed to contain a hash, %cfg_authorize, which lists the authorized users
for each task. The default is "../.acl" which works fine when this script
is applied to station failure handlers (%cfg_failure_handlers) or manual 
overrides (%cfg_manual_overrides).

The user 'any' may be used to indicate that any user has authorization to 
perform the associated task (a password will still be required).

=back

=head1 AUTHOR

Stephen berrick

=cut

################################################################################
# s4pm_tk_pwbox.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

#
# Initialize
#

use Tk;
use strict;
use S4P;
use Safe;
use Getopt::Long;
use vars qw($STATUS
            $PASS_IN
            $USER_IN
            $MAIN
            $COMMAND
            $TASKNAME
            $LABEL
            $ACL
           );

$PASS_IN = "";
$USER_IN = "";
$COMMAND = undef;
$TASKNAME = undef;
$LABEL   = undef;
$ACL = "../.acl";
my $retc="";

GetOptions("command=s" => \$COMMAND,
           "task=s"    => \$TASKNAME,
           "label=s"   => \$LABEL,
           "acl=s"     => \$ACL,
);

unless ( $COMMAND and $TASKNAME ) {
    S4P::perish("$0 requires two arguments.\nUsage: $0 command task");
}

# Read in authorized users from station.cfg file safely

my $compartment = new Safe 'CFG';
$compartment->share('%cfg_authorize');
$compartment->rdo($ACL) or S4P::perish(1, "Cannot read access control list in safe mode: $!\n");

# Verfiy that the task is recognizable

unless ( exists $CFG::cfg_authorize{$TASKNAME} ) {
    S4P::perish("Task [$TASKNAME] is not in \%cfg_authorize hash of access control list file.");
}

$MAIN = MainWindow->new;

# First, set up frame widgets

my $label_frame  = $MAIN->Frame->pack();
my $us_frame     = $MAIN->Frame->pack();
my $pw_frame     = $MAIN->Frame->pack();
my $button_frame = $MAIN->Frame->pack();

# Widget for the label

$MAIN->title("Enter Username and Password");
if ( $LABEL ne "" ) {
    $label_frame->Label(
        -text => "Task '$LABEL' requires a username and password.",
        -background => "white", -foreground => "red", -padx => 20, -pady => 10, 
        -wraplength => 200)->pack(-expand => 1);
} else {
    $label_frame->Label(
        -text => "Task '$TASKNAME' requires a username and password.",
        -background => "white", -foreground => "red", -padx => 20, -pady => 10, 
        -wraplength => 200)->pack(-expand => 1);
}

# Widget for entering in the password. Here, we allow hitting the return key
# to be the same as hitting the Submit button.

$us_frame->Label(-text => 'User Name:')->pack(-side => 'left', -anchor => 'e');
my $us_entry = $us_frame->Entry(-textvariable => \$USER_IN)->pack(
    -expand => 1, -side => "left", -padx => 5, -pady => 5, -anchor => 'w');

$pw_frame->Label(-text => ' Password:')->pack(-side => 'left', -anchor => 'e');
my $pw_entry = $pw_frame->Entry(-textvariable => \$PASS_IN, -show => "*")->pack(
    -expand => 1, -side => "left", -padx => 5, -pady => 5, -anchor => 'w');
$pw_entry->bind( '<Key-Return>', [ \&submit, Ev('K') ] );

# Finally, add the buttons 

$button_frame->Button( -text => "Submit", -command => \&submit )->pack(-expand => 1, -side => 'left', -padx => 10, -pady => 10, -fill => 'both');

$button_frame->Button(-text => "Cancel", -command => sub { 
        $STATUS = 0; $MAIN->destroy(); 
    }
)->pack(-side => 'left', -fill => 'both', -padx => 10, -pady => 10, 
    -expand => 1);

MainLoop;

# The $STATUS variable tells us whether or not the correct password was
# entered and hence, whether or not to execute the command

if ( $STATUS ) {
    print "Executing: $COMMAND ...\n";
    system("$COMMAND");
    exit 0;
} else {
    print "Failed to Execute: $COMMAND ...\n";
    exit 1;
}

sub authenticate {

    my $task = $CFG::cfg_authorize{$TASKNAME};

### Make sure entered user is authorized for said task

    my $found = 0;
    foreach my $user ( @{$CFG::cfg_authorize{$TASKNAME}} ) {
        if ( $user =~ /^any$/i or $USER_IN eq $user ) {
            $found = 1;
            last;
        }
    }
    unless ( $found ) {
        my $answer = $MAIN->messageBox(-title => "Not Authorized!",
                -message => "User $USER_IN is not authorized to perform task $TASKNAME.",
                 -type => 'Ok',
                 -icon => 'error',
               );
        if ( $answer eq "Ok" ) {
            $STATUS = 0;
            $MAIN->destroy();
        }
    }

    if ( S4P::authenticate_unix_user($USER_IN, $PASS_IN) ) {
        return 1;
    } else {
        return 0;
    }
}

sub submit {

### Check if password entered was correct

    $retc = authenticate(); 

    if($retc == 0){	# Incorrect password entered

        my $answer = $MAIN->messageBox(-title => "Incorrect Username or Password!",
                         -message => "The username or password you entered was not correct.",
                         -type => 'RetryCancel',
                         -icon => 'error',
                         -default => "Retry",
                     );

        if ( $answer eq "Retry" ) {
            $STATUS = 0;
        } elsif ( $answer eq "Cancel" ) {
            $STATUS = 0;
            $MAIN->destroy();
        }
        case_log_it("Execution of [$COMMAND] failed for user $USER_IN");

    } else {		# Correct username and password entered

        $MAIN->destroy();
        $STATUS = 1;
        case_log_it("Execution of [$COMMAND] succeeded for user $USER_IN");
    }
}

sub case_log_it {

    my $msg = shift;

    if ( $ENV{'CBR_LOG_FILE'} ) {

        my $logfile = $ENV{'CBR_LOG_FILE'};

        if ( $logfile and $logfile ne "" ) {
            S4P::log_case($logfile, "M", 0, $msg);
        }
    }
}

