#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_create_string.pl - create, modify and control a S4PM string

=head1 SYNOPSIS

s4pm_create_string.pl
[B<-m> I<email>]
[B<-n>]
I<work_order>

=head1 DESCRIPTION

B<s4pm_create_string.pl> is the station code for the Stringmaster station.
Allowable work order types are CREATE_STRING, MODIFY_STRING, HALT_STRING,
and START_STRING. 

The CREATE_STRING and MODIFY_STRING work orders are complete Stringmaker
string-specific configuration files, so complete, in fact, that the nominal
s4pm_stringmaker_datatypes.cfg file is rendered superfluous. In other words, 
the input work order string-specific configuration file contains everything 
that is needed in a string-specific configuration file plus what ever is 
normally contained in the s4pm_stringmaker_datatypes.cfg file for that 
particular string.

Because the input work order string-specific configuration file is so
complete, the s4pm_stringmaker_datatypes.cfg file can be essentially an empty 
file containing only a line with a 1 on it.

B<s4pm_create_string.pl> will shutdown all stations and kill any running
jobs when a MODIFY_STRING work order is received. Therefore, this work
order should not be sent if there is work in the string still going on.

The HALT_STRING and START_STRING work orders are not Stringmaker configuration
files like the CREATE_STRING and MODIFY_STRING. Instead, they contain a
single line with the fullpath of the station root directory. The HALT_STRING
causes all stations to be shutdown and all jobs therein to be killed. The
START_STRING starts up all stations in a string.

If the -m option is invoked, an e-mail message will be set to the e-mail 
address(es) listed to inform the recepient(s) of the new string. This option is 
intended to be used with the CREATE_STRING work order type to notify Ops that
a new string will need to be monitored. If including more than one e-mail
address, separate using commas and enclose within quotes.

All input work orders are archived in an ARCHIVE subdirectory under the station
directory. There is no downstream station.

Upon successful Stringmaker-ing of a new or existing string, a _STATUS.<jobid> 
file is deposited in the 'status' subdirectory under the station directory. 
This file contains only a 0 if the processing was successful and a 1 if not.

By default, after running Stringmaker, this script will run a full clean out
of the string using s4pm_tk_admin.pl. The -n option disables this action. That
is, a full clean out will NOT be done after running Stringmaker.

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2.

=cut

################################################################################
# s4pm_create_string.pl,v 1.3 2007/02/05 21:00:40 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use Cwd;
use Safe;
use File::Copy;
use Getopt::Std;
use File::Basename;
use Sys::Hostname;
require 5.6.0;
use strict;
use vars qw($passfail_file $opt_m $opt_n);

getopts('m:n');

my $machine = hostname();
my $hostcfg = $machine . ".cfg";

my @config_files = ('s4pm_stringmaker_datatypes.cfg',
                    's4pm_stringmaker_derived.cfg',
                    's4pm_stringmaker_static.cfg',
                    's4pm_stringmaker_global.cfg',
                    $hostcfg,
);

# Extract job type and job id from input work order

my $wo = $ARGV[0];
unless ( -e $wo ) {
    S4P::perish(30, "main: Work order [$wo] doesn't seem to exist!");
}

my ($jobtype, $jobid) = parse_wo($wo);
unless ( $jobtype and $jobid ) {
    S4P::perish(30, "main: Failure to parse jobid from input work order: $wo");
}
my $passfail_file = "../status/STATUS.$jobid";

# Construct output work order name and file

my $out_wo = $wo;
$out_wo =~ s/^DO\.//;
$out_wo .= ".wo";
copy($wo, $out_wo);

# Setup symlinks to configuration files

make_links(@config_files);

if ( $jobtype eq 'HALT_STRING' ) {
    if ( halt_string($wo) ) {
        passfail(1);
        exit 1;
    } else {
        my $stat = clean_up(@config_files);
        if ( $stat eq 1 ) {
            passfail(1);
            exit 1;
        } else {
            passfail(0);
            exit 0;
        }
    }
}
   
if ( $jobtype eq 'START_STRING' ) {
    if ( start_string($wo) ) {
        passfail(1);
        exit 1;
    } else {
        my $stat = clean_up(@config_files);
        if ( $stat eq 1 ) {
            passfail(1);
            exit 1;
        } else {
            passfail(0);
            exit 0;
        }
    }
}
   
# If we're modifying an existing string, shut everything down first

if ( $jobtype eq 'MODIFY_STRING' ) {
    S4P::logger("INFO", "Shutting down string before modifying.");
    if ( halt_string($wo) ) {
        passfail(1);
        exit 1;
    }
    S4P::logger("INFO", "String has been successfully shut down.");
}

if ( $jobtype eq 'CREATE_STRING' ) {
    unless ( $opt_m ) {
        S4P::perish(40, "main: CREATE_STRING job type requires that email address be passed via the -m option.");
    }
}

# Form Stringmaker command to run

my $cmd = "s4pm_stringmaker.pl -c -s $wo";
S4P::logger("INFO", "Running this Stringmaker command: [$cmd]");
my ($errstr, $rc) = S4P::exec_system("$cmd");
if ($rc) {
    S4P::logger("ERROR", "main: command: [$cmd] failed: $errstr");
    passfail(1);
    exit 1;
}

# Do a full initialization of the string

intialize_string($wo) unless ( $opt_n );

# Clean up

my $stat = clean_up(@config_files);
if ( $stat eq 1 ) {
    passfail(1);
    exit 1;
}

passfail(0);
exit 0;

sub intialize_string {

    my $wo = shift;

    my $pwd = cwd();	# Save location

    my $root = get_s4pm_root($wo);
    my ($user, undef) = fileparse($root);

### Send email to Ops letting them know of the new string if it's new
  
    if ( $opt_m ) {
        my $message = "A new string has been created in S4PM-DME on g0spp12 for user $user. The root directory is $root.\n\nHappy monitoring!";
        my $sendmail = "/usr/sbin/sendmail";
        if (! open(MAIL, "| $sendmail -t -i")) {
            S4P::perish(40, "intialize_string(): Failed to open $sendmail. Cannot send email to $opt_m notifying them of new string: $!");
        }
        print MAIL "To: $opt_m\n";
        print MAIL "Subject: New DME String for user $user created\n";
        print MAIL "\n";
        print MAIL "\n$message";
        close MAIL;
    }

    chdir($root) or S4P::perish(100, "Failed to chdir to $root: $!");
    my $cmd = "s4pm_tk_admin.pl -task FullCleanOut -force";
    S4P::logger("INFO", "Running this Stringmaker command: [$cmd]");
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger("ERROR", "intialize_string(): command: [$cmd] failed: $errstr");
        passfail(1);
        exit 1;
    }

    chdir($pwd);

}

sub clean_up {

    my @files = @_;

    foreach my $f ( @files ) {
        unlink($f) or S4P::perish(10, "clean_up(): Failed to unlink file [$f]: $!");
    }
   
    if ( -e "stringmaker.log" ) {
        unlink("stringmaker.log") or S4P::perish(10, "clean_up(): Failed to unlink file stringmaker.log: $!");
    }
    
    my ($errstr, $rc) = S4P::exec_system("/bin/rm -fr tmp");
    if ($rc) {
        S4P::logger("ERROR", "clean_up(): command: [/bin/rm -fr tmp] failed: $errstr");
        return 1;
    }
    return 0;
}

sub make_links {

    my @files = @_;

    my $from;
    my $to;

    foreach my $f ( @files ) {
        my $from = "../" . $f;
        my $to   = "./" . $f;
        link($from, $to) or S4P::perish(100, "make_links(): Failed to symlink $from to $to: $!");
    }

    return 0;

}

sub passfail {

    my $status = shift;

    open(PFF, ">$passfail_file") or S4P::perish(100, "passfail(): Failed to open file $passfail_file for write: $!");
    print PFF "$status\n";
    close PFF;
}

sub get_s4pm_root {

    my $wo = shift;

### Test to see which work order type we have

    open(TMP, $wo) or S4P::perish(30, "get_s4pm_root: Failed to open work order [$wo] for read: $!");
    my $line = <TMP>;
    close TMP;
    chomp($line);
    if ( $line =~ /^\s*(\/|\\)/ ) {
        $line =~ s/^\s+//;
        $line =~ s/\s+$//;
        return $line;
    }

    my $compartment1 = new Safe 'CFG1';
    my $compartment2 = new Safe 'CFG2';

    $compartment1->share('$s4pm_root');
    $compartment2->share('$data_source', '$instance');

    $compartment1->rdo($machine . ".cfg");
    $compartment2->rdo($wo);

    print "s4pm_root: [$CFG1::s4pm_root], data_source: [$CFG2::data_source]\n";
    return $CFG1::s4pm_root . "/" . $CFG2::data_source . "/stations/" . $CFG2::instance;

}

sub parse_wo {

    my $wo = shift;

    if ( $wo =~ /^DO\.([^\.]+)\.([0-9]+)$/ ) {
        return ($1, $2);
    } else {
        return undef;
    }
}

sub halt_string {

    my $wo = shift;

    my $root = get_s4pm_root($wo);

    S4P::logger("INFO", "Shutting down string at root $root.");
    my $pwd = cwd();
    chdir $root;
    my $cmd = "s4pshutdown.pl -r";
    my ($errstr, $rc) = S4P::exec_system($cmd);
    if ($rc) {
        S4P::logger("ERROR", "clean_up(): command: [$cmd] failed: $errstr");
        return 1;
    }
    S4P::logger("INFO", "String at root $root has been successfully shut down.");
    chdir $pwd;

    return 0;
}

sub start_string {

    my $wo = shift;

    my $root = get_s4pm_root($wo);

    S4P::logger("INFO", "Starting up string at root $root.");
    my $pwd = cwd();
    chdir $root;
    my $cmd = "s4pstart.ksh >& /dev/null";
    my ($errstr, $rc) = S4P::exec_system($cmd);
    if ($rc) {
        S4P::logger("ERROR", "clean_up(): command: [$cmd] failed: $errstr");
        return 1;
    }
    chdir $pwd;
    S4P::logger("INFO", "String at root $root has been successfully started up.");

    return 0;
}

