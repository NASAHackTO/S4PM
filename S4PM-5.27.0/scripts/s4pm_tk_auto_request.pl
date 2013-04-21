#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_auto_request.pl - submit orders to auto_request work station

=head1 SYNOPSIS

s4pm_tk_auto_request.pl 
[B<-d> I<datatypes>]
[B<-u> I<ur_srvr>]
[B<-o> I<output_jobtype>]
[B<-x> I<max_rate>]
[B<-t> I<max_days>]
[B<-D> I<output_dir>]
request_data_dir
request_data_dir
...

=head1 DESCRIPTION

s4pm_tk_auto_request is a simple interface for creating AUTO_REQUEST work orders.
It reads the s4pm_tk_compose_request.cfg file in order to get the datatypes
available and the typical request increment.

=head1 ARGUMENTS

=over 4

=item B<-d> I<datatypes>

Comma (or whitespace) separated list of datatypes, both shortname and version, 
e.g. MOD000.001,AM1EPHND.001.  If specified, this will override the datatypes
in the s4pm_tk_compose_request.cfg file in the request_data station.

=item B<-u> I<ur_srvr>

Used for constructing the AUTO_REQUEST and then REQUEST_DATA work orders.
Defaults to UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV].

=item B<-o> I<output_jobtype>

Jobtype to use for output work orders. Default is AUTO_REQUEST.

=item B<-m> I<max_rate>

Maximum X rate.  Default is 10.0.  BE CAREFUL when overriding with higher
values.

=item B<-t> I<max_timespan>

Maximum timespan for an AUTO_REQUEST/AUTO_ACQUIRE work order in days.
Default is 31.

=item B<-D> I<output_dir>

Output directory for work orders. Default is ../auto_request, relative to
each directory specified on command line.

=back

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=head1 LAST REVISED

2006/08/07 12:20:21

=cut

################################################################################
# s4pm_tk_auto_request.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use File::Basename;
use Safe;
use Getopt::Std;
use Tk;
use Tk::LabEntry;
use S4P;
use S4P::S4PTk;
use S4P::TimeTools;
use vars qw($opt_d $opt_D $opt_o $opt_u $opt_t $opt_x %datatypes);

# Parse command line
getopts('d:D:o:t:u:x:');
$opt_t ||= 31;
my @dirs = @ARGV;
if ($opt_d) {
    my @datatypes = split(/[,\s]+/, $opt_d);
    %datatypes = map {($_, 1)} @datatypes;
}
my $output_jobtype = $opt_o || 'AUTO_REQUEST';

# Create Main Window
our $main = MainWindow->new;

# Loop through directories
# Create a frame for each one, with its own start and stop time, X rate 
# and datatypes
foreach (@dirs) {
    my $cpt = new Safe 'CFG';
    $cpt->share('%cfg_datatypes');
    unless ($cpt->rdo("$_/s4pm_tk_compose_request.cfg")) {
        warn "Cannot read $_/s4pm_tk_compose_request.cfg: $!";
        next;
    }
    %datatypes = %CFG::cfg_datatypes unless %datatypes;
    my $ur_srvr = $opt_u || $main::cfg_ur_srvr
              || 'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]';

    my $text = $_;
    # Truncate name for display if wildcarding a file in the directory
#   $text = dirname($_) if (m#request_data$/*#);  # What was I thinking?
    my $frame = $main->Frame(-relief => 'groove', -bd => 2)->grid();
    my $row = 0;
    my $col = 0;

    # Display Directory
    my $label = $frame->Label(-text => $text)->grid(-row=>$row, -column=>$col++);
    my $subframe = $frame->Frame()->grid(-row=>$row, -column=>$col++);

    # Display X rate
    my $x_rate = '1.0';
    my $max_x_rate = $opt_x || 10.;
    my $w = $subframe->LabEntry(
        -width => 5,
        -label => 'X Rate: ',
        -textvariable => \$x_rate,
        -validate        => 'focusout',
        -validatecommand => [\&validate_float, \$x_rate, 0.001, $max_x_rate],
        -labelPack => ['-side' => 'left', '-anchor' => 'w'],
    )->pack(-side=>'left');
    $w->configure(-invalidcommand => sub {$w->delete(0,'end')});

    # Display start and stop times
    my ($start_year, $start_month, $start_day, $start_hour, $start_minute, $start_second);
    my ($stop_year, $stop_month, $stop_day, $stop_hour, $stop_minute, $stop_second);
    my $subframe = date_time_frame($frame, "Start", \$start_year, \$start_month, \$start_day, 
        \$start_hour, \$start_minute, \$start_second);
    $subframe->grid(-row=>++$row);
    my $subframe = date_time_frame($frame, "Stop", \$stop_year, \$stop_month, \$stop_day, 
        \$stop_hour, \$stop_minute, \$stop_second);
    $subframe->grid(-row=>++$row);
    $row++;

    # Display datatypes
    # @rs_check_dt is an array with reference variables for storing the
    # checkbutton outputs
    my @rs_check_dt;
    my @subframe = ();

### Pre-define some extra button rows

    for ( my $j = 0; $j < 10; $j++) {
        $subframe[$j] = $frame->Frame()->grid(-row=>$row+$j);
    }

    my $kount = 0;
    foreach my $dt(sort keys %datatypes) {
        my $indx = int($kount/10);	# Limit buttons to 10 per row
        my @t = split('\.', $dt);
        my $check_dt;
        push @rs_check_dt, \$check_dt;
        my $cb = $subframe[$indx]->Checkbutton(
                     -text => "$t[0]\n$t[1]",
                     -onvalue => $dt,
                     -offvalue => '',
                     -variable => \$check_dt,
                     -indicatoron=>0
                 )->pack(-side=>'left');
        $cb->select() if $datatypes{$dt};
        $kount++;
    }

    # Create Work Orders button
    my $auto_request = $opt_D || "$_/../auto_request";
    $frame->Button(
        -text => 'Create Work Orders',
        -command => [\&create_work_orders, $auto_request, 
          $CFG::cfg_increment, $ur_srvr, \$x_rate, \@rs_check_dt,
          \$start_year, \$start_month, \$start_day, 
          \$start_hour, \$start_minute, \$start_second,
          \$stop_year, \$stop_month, \$stop_day, 
          \$stop_hour, \$stop_minute, \$stop_second],
    )->grid(-row => $row, -column => 1);
}

# Add exit button at bottom of screen
my $frame = $main->Frame(-relief => 'groove', -bd => 2)->grid();
$frame->Button(-text => 'Exit', -command=> sub{exit(0)})->pack(-side => 'left');
S4P::S4PTk::redirect_logger($main);
$main->MainLoop();

sub create_work_orders {
    my ($dir, $incr, $ur_srvr, $rs_x_rate, $ra_rs_check_dt,
        $rs_start_year, $rs_start_month, $rs_start_day, 
        $rs_start_hour, $rs_start_minute, $rs_second, 
        $rs_stop_year, $rs_stop_month, $rs_stop_day,
        $rs_stop_hour, $rs_stop_minute, $rs_second, 
    ) = @_;

    # Get datatypes from checkbutton:  dereference array, then each variable in
    # the array
    my @rs_check_dt = @$ra_rs_check_dt;
    my @datatypes;
    foreach my $rs_check_dt (@rs_check_dt) {
        push (@datatypes, $$rs_check_dt) if ($$rs_check_dt);
    }
    my $datatypes = join(' ', @datatypes);

    # Convert entry values into CCSDS date/times
    # Return if we don't get any (error was alerted in ccsds_datetime routine)
    my $start = ccsds_datetime($rs_start_year, $rs_start_month, $rs_start_day,
        $rs_start_hour, $rs_start_minute, $rs_second);
    return unless ($start);
    my $stop = ccsds_datetime($rs_stop_year, $rs_stop_month, $rs_stop_day,
        $rs_stop_hour, $rs_stop_minute, $rs_second);
    return unless ($stop);

    # Make sure start < stop: otherwise it's likely a typo
    if ($stop lt $start) {
        S4P::logger("ERROR", "Stop time is before start time\n$stop < $start");
        return;
    }

    # Compute timespan
    my $timespan = S4P::TimeTools::CCSDSa_Diff($start, $stop);
    $timespan /= 86400.;  # Convert to days
    if ($timespan > $opt_t) {
        S4P::logger("ERROR", "Timespan is too long:  $timespan > $opt_t");
        return;
    }

    # Build output work order
    my @out;
    push @out, "REQUEST_INCREMENT: $incr";
    push @out, "X_RATE: $$rs_x_rate";
    push @out, "BEGIN_TIME: $start";
    push @out, "END_TIME: $stop";
    push @out, "DATATYPES: $datatypes";
    push @out, "UR_SRVR: $ur_srvr";
    push @out, "=\n";
    my $out = join("\n", @out);

    my $confirm_msg =<< "EOF";
Are you sure you want to create the following work order 
with a timespan of $timespan days?

$out
EOF
    return unless (S4P::S4PTk::confirm($main, $confirm_msg));

    # Output work order in auto_request station
    my $outfile = "$dir/DO.$output_jobtype." . time() . ".wo";
    if (!open OUT, ">$outfile") {
        S4P::logger("ERROR", "Cannot write to $outfile: $!");
        return;
    }
    print OUT $out;
    if (!close OUT) {
        S4P::logger("ERROR", "Cannot close $outfile: $!");
        return;
    }
    else {
        show_info("Wrote the following to work order $outfile:\n$out");
    }
    return 1;
}
# Make a frame with a 6-component date/time
sub date_time_frame {
    my ($parent, $text, $rs_year, $rs_month, $rs_day, $rs_hour, $rs_minute, $rs_second) = @_;
    my $subframe = $parent->Frame();
    my @t = gmtime();
    my $max_year = $t[5] + 1900;
    $$rs_hour = '00';
    $$rs_minute = '00';
    $$rs_second = '00';
    my $w = label_entry($subframe, "$text  Year:", $rs_year, 4, \&validate_int, 2000, $max_year);
    my $w = label_entry($subframe, "Month:", $rs_month, 2, \&validate_int, 0, 12);
    my $w = label_entry($subframe, "Day:", $rs_day, 2, \&validate_int, 0, 31);
    my $w = label_entry($subframe, "Hour:", $rs_hour, 2, \&validate_int, 0, 24);
    my $w = label_entry($subframe, "Minute:", $rs_minute, 2, \&validate_int, 0, 60);
    my $w = label_entry($subframe, "Second:", $rs_second, 2, \&validate_int, 0, 60);
    return $subframe;
}

# Label_entry used for components of date/time frames
sub label_entry {
    my ($parent, $text, $rs_textvar, $width, $rf_validate, $min, $max) = @_;
    my $w = $parent->LabEntry(
        -label           => $text,
        -width           => $width,
        -validate        => 'focusout',
        -labelPack    => ['-side' => 'left', '-anchor' => 'w'],
    )->pack(-side=>'left');
    $w->configure(
        -textvariable    => $rs_textvar,
        -validatecommand => [$rf_validate, $rs_textvar, $min, $max],
        -invalidcommand  => sub {$w->delete(0,'end')},
    ) if ($rf_validate);
    return $w;
};

# Convert 6 components of date and time into a date/time stamp
sub ccsds_datetime {
    my ($rs_year, $rs_month, $rs_day, $rs_hour, $rs_minute, $rs_second) = @_;
    $$rs_hour = 0 unless defined $$rs_hour;
    $$rs_minute = 0 unless defined $$rs_minute;
    $$rs_second = 0 unless defined $$rs_second;
    unless ($$rs_year && $$rs_month && $$rs_day) {
        S4P::logger( "ERROR", "must fill in year, month and day");
        return;
    }
    return sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", $$rs_year, $$rs_month,
        $$rs_day, $$rs_hour, $$rs_minute, $$rs_second);
};

# Entry validation routine:  see if it is a positive int between min and max
sub validate_int { return validate_num('^\d+$', "all digits", @_); }
sub validate_float { return validate_num('^[\d\.]+$', "floating point", @_); }
sub validate_num {
    my $pattern = shift;
    my $criteria = shift;
    my $rs_num = shift;
    my $min = shift;
    my $max = shift;
    # Return if it's either blank or within the proper ranges
    return 1 unless $$rs_num;  # Let blanks go
    return 1 if ($$rs_num =~ /$pattern/ && $$rs_num >= $min && $$rs_num <= $max);
 
    # Everything below here is an error or warning
    $main->bell();
    if ($$rs_num !~ /$pattern/) {
        S4P::logger("ERROR", "Value must be $criteria\n");
    } elsif ($$rs_num < $min) {
        S4P::logger( "ERROR", "Value must be greater than $min\n");
    } elsif ($$rs_num > $max) {
        S4P::logger( "WARNING", "Value is greater than $max. I hope you know what you're doing.\n");
    }
    return 0;
}

# Display info after work order is written
sub show_info {
    my @text = @_;
    my $dialog = $main->DialogBox( -title => "$0 Info", -buttons => ['OK']);
    $dialog->add('Label', -text => join("\n", @text), -justify => 'left')->
        pack(-side => 'left', -expand => 1);
    $dialog->Show();
}

