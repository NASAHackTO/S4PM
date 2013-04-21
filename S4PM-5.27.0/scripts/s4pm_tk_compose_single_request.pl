#!/usr/bin/perl

=head1 NAME

s4pm_tk_compose_single_request.pl - formulate request for one file at a time

=head1 SYNOPSIS

s4pm_tk_compose_single_request.pl
[B<-f> I<config_file>]
[B<-h>]

=head1 DESCRIPTION

B<s4pm_tk_compose_single_request.pl> is a GUI for composing Data Requests for 
single files to be submitted by the Request Data station.

From the command line it can take configuration file name and UR server prefix.
From the screen, it takes as input year, day of year, hours and minutes.
ESDT is selected from valids.  It outputs a PDR for the file.  The PDR 
contents is a FILE_GROUP which I<represents> the file (it has a bogus 
filename and directory name). The output file name is of the form 
DO.REQUEST_DATA.YYYYDDDHHMM.wo, i.e., a valid work order for the reprocess
station.

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

The configuration file defaults to I<compose_request.cfg> if not specified.

=item B<-h>

Prints usage statement.

=back

=head1 SEE ALSO

L<tkcompreq>, L<DataRequest>

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_compose_single_request.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_f $opt_h $opt_u);
use Getopt::Std;
use Tk;
use S4P::TimeTools;
use S4P::PDR;
use S4P::S4PTk;
use S4PM::DataRequest;
use Safe;

# Process command line (some options exist, but are not advertised in man page
# as they are primarily for testing purposes.)

getopt('f:hu:');
usage() if $opt_h;

# Read config file and process defaults

my $compartment;
if ( $opt_f ) {
    $compartment = new Safe 'CFG';
    $compartment->share('$cfg_ur_srvr', '@cfg_datatypes');
    $compartment->rdo($opt_f) or
        S4P::perish(30, "main: Failed to read in configuration file $opt_f in safe mode: $!");
}

my $ur_srvr = $opt_u || $CFG::cfg_ur_srvr
                 || 'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]';

my @datatypes = @CFG::cfg_datatypes;
@datatypes = (   'MOD01.003', 
                 'MOD03.003', 
                 'MOD021KM.003', 
                 'MOD02QKM.003', 
                 'MOD000.001', 
                 'AM1EPHN0.001',
                 'AM1ATTNF.001',
                 'GDAS_0ZF.001',
                 'NISE.001',
                 'OZ_DAILY.001',
                 'REYNSST.001',
                 'SEA_ICE.001'
                ) if (! @datatypes);

# Construct main window so we can get X defaults and logger redirect going

my $main_window = new MainWindow;
$main_window->title("Compose Single Data File Request Tool");
S4P::S4PTk::read_options($main_window);
S4P::S4PTk::redirect_logger($main_window);

# Entries for entering year and day of year

my ($year, $doy, $hhmm1, $hhmm2, $esdt_listbox);
my $entry_frame = make_entry_frame($main_window, \@datatypes, \$year, \$doy, 
    \$hhmm1, \$hhmm2, \$esdt_listbox);
$entry_frame->pack(-side=>'top',-fill=>'x');

# Listbox for query results

my $listbox = $main_window->Scrolled('Listbox', -width=>90, -scrollbars=>'e', 
    -selectmode=>'extended');

# Add database query button
my @granules;
$entry_frame->Button(-text=>'Query Archive',
    -command=>[\&query_db, \$year, \$doy, \$hhmm1, \$hhmm2, $esdt_listbox,
               $ur_srvr, \@granules, $listbox]
    )->pack(-side=>'bottom');

$listbox->pack(-expand=>1, -fill=>'both');

# Frame with Submit and Exit buttons
my $submit_frame = make_submit_frame($main_window, \@granules, $listbox);
$submit_frame->pack(-side=>'top');

MainLoop;
exit(0);
##############################################################################
# G U I   C O N S T R U C T I O N
##############################################################################
sub make_entry_frame{
    my ($parent, $ra_datatypes, $rs_year, $rs_doy, $rs_hhmm1, $rs_hhmm2, $rs_esdt_listbox) = @_;
    my $frame = $parent->Frame;

    # Year/day entry widget
    my $yr_frame = $frame->Frame->pack(-anchor=>'w', -side=>'left');
    $yr_frame->Label(-text=>'Year:')->pack(-side=>'left');
    $yr_frame->Entry(-width=>4, -textvariable => $rs_year)->pack(-side=>'left');

    # Day of year entry widget
    my $day_frame = $frame->Frame->pack(-anchor=>'w', -side=>'left');
    $day_frame->Label(-text=>'Day of Year:')->pack(-side=>'left');
    $day_frame->Entry(-width=>3, -textvariable => $rs_doy)->pack(-side=>'right');

    # Time entry widget
    my $start_frame = $frame->Frame->pack(-anchor=>'w', -side=>'left');
    $start_frame->Label(-text=>'Start (HHMM):')->pack(-side=>'left');
    $start_frame->Entry(-width=>4, -textvariable => $rs_hhmm1)->pack(-side=>'right');
    my $stop_frame = $frame->Frame->pack(-anchor=>'w', -side=>'left');
    $stop_frame->Label(-text=>'Stop (HHMM):')->pack(-side=>'left');
    $stop_frame->Entry(-width=>4, -textvariable => $rs_hhmm2)->pack(-side=>'right');
    # ESDT Listbox
    my $listbox = $frame->Listbox(-selectmode=>'extended')->pack(-anchor=>'ne',
        -side=>'left');
    foreach (@$ra_datatypes) {
        $listbox->insert('end', $_);
    }
    $$rs_esdt_listbox = $listbox;

    return $frame;
}
sub make_submit_frame {
    my ($parent, $ra_granules, $listbox) = @_;
    my $frame = $parent->Frame;

    # Submit Request button
    $frame->Button(-text=>'Submit Request', 
        -command => [\&submit_request, $parent, $ra_granules, $listbox]
        )->pack(-side=>'left');

    # Exit Button
    $frame->Button(-text => 'Exit', -command => [\&exit_program, $parent]
        )->pack(-side=>'right');
    return $frame;
}
###########################################################################
# C A L L B A C K S
###########################################################################
sub query_db {
    my ($rs_year, $rs_doy, $rs_hhmm1, $rs_hhmm2, $esdt_listbox,
        $ur_srvr, $ra_granules, $listbox) = @_;
    
    validate_day_input($rs_year, $rs_doy) or return 0;

    # Make yyyydddhhmmss time formats out of them so we can convert
    my $t1 = sprintf("%04d%03d%04d", $$rs_year, $$rs_doy, $$rs_hhmm1);
    my $ccsds1 = S4P::TimeTools::yyyydddhhmmss2CCSDSa($t1);
    my $t2 = sprintf("%04d%03d%04d", $$rs_year, $$rs_doy, $$rs_hhmm2);
    my $ccsds2 = S4P::TimeTools::yyyydddhhmmss2CCSDSa($t2);

    my $dbh = S4PM::GdDbi::db_connect();
    if (! $dbh) {
        S4P::logger('ERROR', "query_db(): Cannot open database connection");
        return 0;
    }

    $listbox->delete(0, 'end');

    # Find out which datatypes were selected by user
    my @esdts = map {$esdt_listbox->get($_)} $esdt_listbox->curselection();
    # Initialize granule array
    @{$ra_granules} = ();
    foreach (@esdts) {
        my ($esdt, $version_id) = split('\.', $_);
        my ($rh_ur, $rh_size, $rh_add_attrs) = S4PM::GdDbi::get_ur_by_datatime($dbh, 
            $esdt, $version_id, $ccsds1, $ccsds2, $ur_srvr, 
            'ProductionDateTime, LocalGranuleID, dbID');
        foreach my $gran_key(sort keys %{$rh_ur}) {
            my $ur = $rh_ur->{$gran_key};
            my $lgid = $rh_add_attrs->{$gran_key}->[1];
            my $dbid = $rh_add_attrs->{$gran_key}->[2];
            my ($dt, $begin, $end) = split('/', $gran_key);
            $begin = S4P::TimeTools::timestamp2CCSDSa($begin);
            $end = S4P::TimeTools::timestamp2CCSDSa($end);
            my $production_time = $rh_add_attrs->{$gran_key}->[0];
            $production_time =~ s/\//-/g;  # Account for Sybase/Oracle difference
            # Set $lgid to SOMETHING recognizable if we don't have one
            $lgid ||= sprintf("%s.%03d/%s", $esdt, $version_id, $begin);
            $listbox->insert('end', sprintf("%-60s (%d)", $lgid, $dbid));
            push @{$ra_granules}, S4PM::DataRequest::granule2file_group($ur, $begin,
            S4P::TimeTools::timestamp2CCSDSa($end),
                S4P::TimeTools::timestamp2CCSDSa($production_time),
                $rh_size->{$gran_key} * 1024. * 1024.);
        }
    }
    return 1;
}
sub exit_program {
    my $parent = shift;
    # Confirm before quitting
    (S4P::S4PTk::confirm($parent, "Are you sure you want to exit?")) ? exit(0) : return 0;
}
sub submit_request {
    my ($parent, $ra_granules, $listbox) = @_;
    my $datatype;
    my %granules;
    my ($fg, $bin, $time, @selected_granules, @files);

    my @selections = $listbox->curselection();
    my @selected_urs;
    foreach (@selections) {
        push @selected_granules, $ra_granules->[$_];
        push @selected_urs, $ra_granules->[$_]->ur;
    }
    # Get confirmation from user
    my $msg = sprintf("Do you want to create a request for %s?", join("\n", @selected_urs));
    return 0 if (! S4P::S4PTk::confirm($parent, $msg));
    
    # User says OK, so...
    # Construct PDR
    my $pdr = new S4P::PDR;
    $pdr->file_groups(\@selected_granules);
    $pdr->recount;

    # Convert to YYYYDDDHHMMSS format for filename
    my $timestamp = 
    my $file = sprintf("DO.REQUEST_DATA.%s_%s.wo", $ra_granules->[0]->data_type,
        S4P::TimeTools::CCSDSa2yyyydddhhmmss($ra_granules->[0]->data_start));
    if ($pdr->write_pdr($file)) {
        S4P::logger('ERROR', "submit_request(): Failed to write PDR to $file");
        return 0;
    }
    S4P::logger('INFO', "submit_request(): Data Request was written to $file");
    return 1;
}
#########################################################################
# S U P P O R T    R O U T I N E S
#########################################################################
sub usage {
   print STDERR << "EOF";
Usage:
$0
-f config_file (Default: compose_request.cfg)
-u ur_server (Default: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV])
-h (Prints this statement)
EOF
    exit(1);
}
sub validate_day_input {
    my ($rs_year, $rs_doy) = @_;
    my @err;
    # Check year
    my ($min_year, $max_year) = (1999, 2010);
    if (! $$rs_year) {
        push (@err, "You must specify a year.");
    }
    elsif ($$rs_year < $min_year || $$rs_year > $max_year) {
        push (@err, "Year must be between $min_year and $max_year.");
    }

    # Check day of year
    if (! $$rs_doy) {
        push (@err, "You must specify a day of year.");
    }
    elsif ($$rs_doy < 1 || $$rs_doy > 366) {
        push (@err, "Day of year must be between 1 and 366.");
    }
    if (@err) {
        S4P::logger('ERROR', join("validate_day_input():\n", '', @err));
        return 0;
    }
    else {
        return 1;
    }
}
