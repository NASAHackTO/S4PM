#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_stringmakerall.pl - run Stringmaker on all S4PM strings on a box

=head1 SYNOPSIS

s4pm_stringmakerall.pl
B<-m[ode]> I<run_mode>
B<-t[est]>
B<-s[kip]> I<skip_list>
string_config_files

=head1 DESCRIPTION

B<s4pm_stringmakerall.pl> eases the burden or running s4pm_stringmaker.pl 
on multiple instances on the same box. When run, B<s4pm_stringmakerall.pl> 
attempts to run Stringmaker on all Stringmaker string configuration files 
provided in string_config_files.

B<s4pm_stringmakerall.pl> must be run within a directory where all the
Stringmaker configuration files reside.

B<s4pm_stringmakerall.pl> can interrogate each file in the list to determine
if it is a proper Stringmaker string configuration file and skip any that
are not. In addition, Stringmaker string configuration files that are for host 
machines other than the current machine are automatically skipped. 

Because of the automatic checking that B<s4pm_stringmakerall.pl> performs,
one can simply pass * as the list of files even if not all files in the
directory are configuration files at all. For example:

s4pm_stringmakerall.pl *

If Stringmaker fails on a particular instance, the script pauses asking
for a response as to continue with any other instances or not.

The output is a summary of all those instances successfully Stringmaker-ed.
those skipped (via the -skip option) and those that failed (if any).

=head1 ARGUMENTS

=over 4

=item B<-m[ode]> I<run_mode>

Sets how Stringmaker will be run. Valid options for I<run_mode> are c for
create, u for update, and a for append, thta is, same as is used for
B<s4pm_stringmaker.pl> directly. The default is c.

=item B<-s[kip]> I<skip_list>

Space delimited list of patterns which when matched by a string are skipped
from built using Stringmaker. For example, to run on all strings except DPREP
strings, use -skip "DP". To skip DPREP and AIRS, use -skip "DP AI".

item B<-t[est]>

Do not Stringmaker anything. Just show which strings would have been built.

=back

=head1 AUTHOR

Stephen W Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_stringmakerall.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use S4P;
use File::Basename;
use Getopt::Long;
use Safe;

use vars qw($SKIP $TEST $RUNMODE);

$SKIP = undef;
$TEST = undef;
$RUNMODE = "c";

GetOptions("skip=s" => \$SKIP,
           "test"   => \$TEST,
           "mode=s" => \$RUNMODE,
);

unless ( $RUNMODE eq "c" or $RUNMODE eq "a" or $RUNMODE eq "u" ) {
    die "\nInvalid mode: Mode must be one of: c (for create), u (for update), or\na (for append)\n\n";
}
my $mode;
if ( $RUNMODE eq "c" ) { $mode = "create"; }
if ( $RUNMODE eq "u" ) { $mode = "update"; }
if ( $RUNMODE eq "a" ) { $mode = "append"; }

my $dir = ".";	# Assume everything is in current directory
my @completed = ();
my @skipped   = ();
my @failed    = ();

my @config_files = @ARGV;

foreach my $file ( @config_files ) {
    unless ( is_bad($file) ) {
        if ( is_skip($file) ) {
            push(@skipped, $file);
            next;
        }
        my ($rs, $rc);
        my $cmd = "s4pm_stringmaker.pl -" . $RUNMODE . " -s $file";
        ($rs, $rc) = S4P::exec_system("$cmd") unless ( $TEST );
        if ( $rc ) {
            warn "Failed to successfully Stringmaker $file\n\n";
            print "Continue? (y/n): ";
            my $res = <STDIN>;
            chomp($res);
            if ( $res =~ /^n/i ) {
                exit 1;
            } else {
                ($rs, $rc) = S4P::exec_system("/bin/rm -fr ./tmp");
                push(@failed, $file);
                print "Continuing...\n";
                sleep 1;
            }
        } else {
            push(@completed, $file);
        }
    } else {
        push(@failed, $file);
        print "Continuing...\n";
        sleep 1;
    }
}

if ( $TEST ) {
    print "\n################################################################################\n";
    print "NOTE: Stringmakerall was run in TEST mode. The following results only show\nwhat WOULD have happened. No strings have actually been modified.\n";
    print "################################################################################\n\n";
}
print "\nStrings with the following Stringmaker string configuration files have been\nsuccessfully rebuilt using Stringmaker in $mode mode:\n\n";
foreach my $instance ( @completed ) {
    my ($fn, $d) = fileparse($instance);
    print "\t$fn\n";
}
if ( scalar(@skipped) > 0 ) {
    print "\nThe following configuration files on this machine have been skipped:\n\n";
    foreach my $instance ( @skipped ) {
        my ($fn, $d) = fileparse($instance);
        print "\t$fn\n";
    }
}
if ( scalar(@failed) > 0 ) {
    print "\nThe following configuration files have failed:\n\n";
    foreach my $instance ( @failed ) {
        my ($fn, $d) = fileparse($instance);
        print "\t$fn\n";
    }
}
print "\n";

sub is_skip {
    my $file = shift;

    my @skip = split(/ /, $SKIP);

    foreach my $s ( @skip ) {
        if ( $file =~ /$s/ ) { return 1; }
    }
    return 0;
}

sub is_bad {

    my $file = shift;

    my $machine = `uname -n`;
    chomp($machine);

    my $compartment = new Safe 'CFG';
    $compartment->share('$host', '$instance', '$string_id', 
                        '$data_source', '@run_sorted_algorithms');
 
    $CFG::host = undef;
    $CFG::instance = undef;
    $CFG::string_id = undef;
    $CFG::data_source = undef;
    @CFG::run_sorted_algorithms = undef;

    $compartment->rdo($file) or ( print "\nERROR: $file failed to get imported safely.\n\n" and return 1 );

    unless ( defined $CFG::host and defined $CFG::instance and
             defined $CFG::string_id and defined $CFG::data_source and
             defined @CFG::run_sorted_algorithms ) {
        print "\nERROR: $file is not a valid Stringmaker string configuration file.\n\n";
        return 1;
    }

    unless ( $CFG::host eq $machine ) {
        print "\nERROR: $file has host set to $CFG::host, but this machine is $machine.\n\n";
        return 1;
    } else {
        return 0;
    }
}
