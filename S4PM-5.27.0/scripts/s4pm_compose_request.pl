#!/usr/bin/perl

=head1 NAME

s4pm_compose_request.pl - formulate a request for data

=head1 SYNOPSIS

s4pm_compose_request.pl
B<-b> I<YYYY-MM-DDTHH:MM:SSZ>
[B<-e> I<YYYY-MM-DDTHH:MM:SSZ>]
[B<-f> I<config_file>]
[B<-i> I<increment>]
[B<-u> I<ur_server>]
[B<-h>]

=head1 DESCRIPTION

B<s4pm_compose_request.pl> is a command-line driver for 
S4PM::DataRequest::compose_request.  It takes as input a begin time, end time, 
time increment (in seconds), configuration file name and UR server prefix.  
It outputs a PDR for each increment between start and stop.  The PDR contents 
are FILE_GROUPs which I<represent> the granules (they have bogus filenames
and directory names). The output file names are of the form 
DO.REQUEST_DATA.YYYYDDDHHMM.wo, i.e., a valid work order for the reprocess
station.

=head1 ARGUMENTS

=over 4

=item B<-b> I<YYYY-MM-DDTHH:MM:SSZ>

Start time for period to make request for, in CCSDSa format.
This is required and has no default.

=item B<-e> I<YYYY-MM-DDTHH:MM:SSZ>

Start time for period to make request for, in CCSDSa format.
Default is the start time + increment.

=item B<-f> I<config_file>

The configuration file defaults to I<s4pm_compose_request.cfg> if not 
specified.

=item B<-i> I<increment>

Time increment in seconds.  Default is 7200 (2 hours).
Can be specified in configuration file.

=item B<-u> I<ur_server>

UR server-prefix-thing.  Default is 
'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]'.
Can be specified in configuration file.

=head1 FILES

Its configuration file contains:

=over 4

=item @cfg_required_datatypes

Required for processing a certain time window.  The distinction between 
required and optional is not yet used, owing to complexities related to 
differing time windows for different datatypes.

=item @cfg_optional_datatypes

Optional for processing a certain time window.

=back

The time increment can also be specified as $cfg_increment, and UR 
server-prefix-thing can be specified as $cfg_ur_srvr.

=head1 SEE ALSO

L<tkcompreq>, L<DataRequest>

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_compose_request.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use S4P;
use S4P::TimeTools;
use S4P::PDR;
use S4PM::DataRequest;
use Safe;
use vars qw($opt_b $opt_e $opt_f $opt_i $opt_h $opt_u);

# Parse command line and set defaults

getopts('b:e:f:hi:u:');
my @ccsds_times;
$ccsds_times[0] = $opt_b or usage();
S4P::TimeTools::CCSDSa_DateParse($ccsds_times[0]) or 
    S4P::perish(10, "main: Begin date is not in proper format YYYY-MM-DDTHH:MM:SSZ");

# Read in configuration file

my $config_file = $opt_f || 's4pm_compose_request.cfg';
my $compartment = new Safe 'CFG';
$compartment->share('$cfg_ur_srvr', '$cfg_increment','@cfg_datatypes', '@cfg_required_datatypes', '@cfg_optional_datatypes');
$compartment->rdo($config_file) or
    S4P::perish(30, "main: Failed to read in configuration file $config_file in safe mode: $!");

my $ur_srvr = $opt_u || $CFG::cfg_ur_srvr 
                 || 'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]';

# Time increment for requests in seconds
my $increment = $opt_i || $CFG::cfg_increment || 7200;

# End time
$ccsds_times[1] = $opt_e || S4P::TimeTools::CCSDSa_DateAdd($opt_b, $increment);
S4P::TimeTools::CCSDSa_DateParse($ccsds_times[1]) or 
    S4P::perish(11, "main: End date is not in proper format YYYY-MM-DDTHH:MM:SSZ");

# Get datatypes from config file
my @datatypes = @CFG::cfg_datatypes;
push(@datatypes, @CFG::cfg_required_datatypes, @CFG::cfg_optional_datatypes) if ! @datatypes;

my %results = S4PM::DataRequest::compose_request(@ccsds_times, $increment,
    \@datatypes, $ur_srvr);

# Write out results
my @filenames = S4PM::DataRequest::write_requests(\%results) if %results;
exit(0);

sub usage {
    print STDERR << "EOF";
Usage:
$0
-b YYYY-MM-DDTHH:MM:SSZ (Required)
-e YYYY-MM-DDTHH:MM:SSZ (Default: begin + increment)
-f config_file (Default: s4pm_compose_request.cfg)
-i increment (Default: 7200 seconds)
-u ur_server (Default: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV])
-h (Prints this statement)
EOF
    exit(1);
}
