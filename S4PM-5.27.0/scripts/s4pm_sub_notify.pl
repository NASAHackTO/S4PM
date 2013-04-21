#!/usr/bin/perl

=head1 NAME

s4pm_sub_notify.pl - convert an ECS subscription notification into an S4PM request_data work order

=head1 SYNOPSIS

s4pm_sub_notify.pl B<[-F]> I<filename>

=head1 DESCRIPTION

This converts an ECS subscription notification into an output REQUEST_DATA
work order.  It parses the Qualifier List to get the UR, which it then
breaks down into ESDT and version.  It also looks for all date fields in the
Qualifier List.  Unfortunately, it doesn't say which date is which, so it
time-orders them and chooses the earliest under the presumption that the
earliest date represents the BeginningDateTime.  This is used to form the
job_id of the output work order.

As a default behaviour, it will fail for a MODIS L0 granule (MOD000 or MODPML0),
or an Aqua carryout file (PMCO_HK, PMCOGBAD), that does not start on the even
hour, or end 1 minute before an even hour.  For the MODIS L0 granules, a
failure will be thrown if the granule size is not > 5000 MB, for PMCO_HK
granules, if the volume is not > 1.85 MB, and for PMCOGBAD, if the volume is
not > 5 MB.

Ragged or incomplete granule rejection can be overriden by specifying the
B<-F> (fix/force) option.

This version first looks for a UR in the Subscription Notification: pattern 
indicative of Spatial Subscription Server notifications.  If not found, it
goes on to look for the Qualifier List used in the old-style subscription 
server notifications.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_sub_notify.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_F);
use Getopt::Std;
use S4PM;
use S4PM::GdDbi;
use S4PM::DataRequest;
use S4P;
use S4P::PDR;

getopts('F');
open IN, $ARGV[0] or 
    S4P::perish(20, "Failed to open work order file $ARGV[0]: $!");
$/ = undef;  # Slurp whole file
my $string = <IN>;
close IN;
# First look for pattern indicative of Spatial Subscription Server
my ($ur) = ($string =~ /Subscription Notification:\s+(UR:.*)\n/);
# If we didn't find one, then look for Qualifier List in old-style 
# subscription
if (! $ur) {
    $string =~ s/.*\nQualifier List:\s*\n//s;

    # Match UR and strip off the front
    $string =~ s/^(UR:\S+)//;
    $ur = $1;
}
S4P::perish(40, "Failed to find UR in subscription notification") if (! $ur);

# Parse UR to get dbID
my ($ur_srvr, $short_name, $version_id, $dbID)=S4PM::ur2dbid($ur);

# Connect to database to get info for making local filename
# Local filename will be used by request_data for making a proper stub file
my $dbh = S4PM::GdDbi::db_connect();
S4P::perish(4, "No database connection obtained") if (! $dbh);

# Query by db_id (no order_by clause needed)
my ($rh_ur, $rh_gran_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_et_al($dbh,
    "dbID = $dbID", '', $ur_srvr, 'ProductionDateTime,SizeMBECSDataGranule');
    
if (! $rh_ur || ! %{$rh_ur}) {
    S4P::perish(5, "Cannot find any granules matching dbID $dbID");
}

# Extract time info
my @keys = keys %{$rh_ur};
my $key = $keys[0];
my ($dt, $begin, $end) = split('/', $key);

# Check for short MODIS L0 granules
if ($short_name =~ /^MOD(000|PML0)/) {
    my ($error);
    if ($begin !~ /\d[02468]:00:\d\d/) {
        $error = "Bad Granule begin time ($begin): probably a short granule";
    }
    elsif ($end !~ /\d[13579]:59:\d\d/) {
        $error = "Bad Granule end time ($end): probably a short granule";
    }
    elsif ($rh_add_attrs->{$key}->[1] < 5000) {
        $error = "L0 size ($rh_add_attrs->{$key}->[1]) is < 5000 MB: probably a short granule";
    }
    # if -F is specified, force them through
    if ($error) {
        if ($opt_F) {
            S4P::logger('WARN', $error . ", but letting it go anyway");
        }
        else {
            S4P::perish(50, $error);
        }
    }
}

# Check for short Aqua CO granules
if ($short_name =~ /PMCO(_HK|GBAD)/) {
    my ($error);
    if ($begin !~ /\d[02468]:00:\d\d/) {
        $error = "Bad Granule begin time ($begin): probably a short granule";
    }
    elsif ($end !~ /\d[02468]:00:\d\d/) {
        $error = "Bad Granule end time ($end): probably a short granule";
    }
# Changed PMCO_HK threshold from 2.5 to 1.85 MB due to file content change
# made by EMOS on 2004/037:1000
    elsif ( ($short_name =~ /^PMCO_HK/) and ($rh_add_attrs->{$key}->[1] < 1.85) ) {
        $error = "PMCO_HK size ($rh_add_attrs->{$key}->[1]) is < 1.85 MB: probably a short granule";
    }
    elsif ( ($short_name =~ /^PMCOGBAD/) and ($rh_add_attrs->{$key}->[1] < 5) ) {
        $error = "PMCOGBAD size ($rh_add_attrs->{$key}->[1]) is < 5 MB: probably a short granule";
    }
    # if -F is specified, force them through
    if ($error) {
        if ($opt_F) {
            S4P::logger('WARN', $error . ", but letting it go anyway");
        }
        else {
            S4P::perish(50, $error);
        }
    }
}

$begin = S4P::TimeTools::timestamp2CCSDSa($begin);
$end = S4P::TimeTools::timestamp2CCSDSa($end);
my $production_time = $rh_add_attrs->{$key}->[0];
$production_time =~ s/\//-/g;  # Account for Sybase/Oracle difference

# Construct file_group object
my $file_group = S4PM::DataRequest::granule2file_group($rh_ur->{$key}, $begin,
    $end, S4P::TimeTools::timestamp2CCSDSa($production_time),
    $rh_gran_size->{$key} * 1024. * 1024.);

#$short_name = 'AIRS' if ($short_name =~ /^AIR/);
my $output_file = sprintf("REQUEST_DATA_%s.%d.wo", $short_name, $dbID);
my $pdr = new S4P::PDR;
$pdr->originating_system('S4PM');
$pdr->file_groups([$file_group]);
my $rc = $pdr->write_pdr($output_file);
if ($rc) {
    S4P::perish(110, "Failed to write output work order $output_file: $!");
}
else {
    S4P::logger('INFO', "Wrote output work order $output_file");
}
exit(0);
