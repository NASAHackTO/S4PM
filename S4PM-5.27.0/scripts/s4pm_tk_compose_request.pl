#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_tk_compose_request.pl - formulate a request for data

=head1 SYNOPSIS

s4pm_tk_compose_request.pl
[B<-f> I<config_file>]
[B<-o> I<output_jobtype>]
[B<-h>]

=head1 DESCRIPTION

B<s4pm_tk_compose_request.pl> is a GUI for composing Data Requests to be 
submitted by the Request Data station.

From the command line it can take configuration file name and UR server prefix.
From the screen, it takes as input year, day of year, and hours in that day.
The user can also select the datatypes to be queried.  It outputs a PDR for 
each increment (default 2 hours) between start and stop.  The PDR contents are 
FILE_GROUPs which I<represent> the files (they have bogus filenames and 
directory names). The output file names are of the form 
DO.REQUEST_DATA.YYYYDDDHHMM.wo, i.e., a valid work order for the reprocess
station.

It can query either the ECS database or a directory structure on local or remote
disk (but not both).  This latter query is triggered by the %cfg_location variable:

For non-ECS requests, the $cfg_location is a complex hash with information on where
the data are located on disk, having the following structure:

  $location = {$esdt =>
                {$version =>
                  {'ftphost' => $host,
                   'dirpat' => $pattern,
                   'filepat' => $pattern,
                   'filesize' => $typical_file_size,
                   'length' => $typical_length_in_secs
                  }
                }
              }

For example:

  $location = {'MOD04_L2' =>
                {'5' =>
                   {'ftphost' => 'ladsftp.nascom.nasa.gov',
                    'dirpat' => 'allData/^V/^E/^Y/^j',
                    'filepat' => '^E.A^Y^j.^H^M.00^V.',
                    'filesize' => 2000000,
                    'length' => 300
                   }
                }
              }

This will query data from the LADS online archive.

If you are querying a local archive, the 'ftphost' component should be omitted.
This will cause the program to use simple symlinks as well.

For restricted S4PA data that are remotely accessed via HTTP, the location info 
is specified as follows:

  $location = {$esdt =>
                {$version =>
                  {'mri_root' => $mri_root,
                   'http_root' => $http_root,
                   'dirpat' => $pattern,
                   'netrc_entry' => $name,
                  }
                }
              }

The $mri_root is the URL of an MRI request, up to the question mark, e.g.:
http://aurapar2u.ecs.nasa.gov/s4pt-ts2/test-bin/s4pa_m2m_cgi.pl?.
This is used to make an MRI query (long listing format) to find out what
data are available.

The $http_root is the URL of the root of the data.  This will be concatenated
with the directory and filename to form the URLs for acquisition.

The $dirpat is the same as used in the $ftphost case, since the long MRI listing
does not have this information.

The $netrc_entry is an "alias" to put in the .netrc file so that we can
get the username and password for S4PA restricted data.

For example:
%cfg_location = (
        'AM1ANC' => {
           1 => {
             'http_root' => 'http://aurapar2u.ecs.nasa.gov',
             'netrc_entry' => 's4pa_tads1_ts2',
             'mri_root' => 'http://aurapar2u.ecs.nasa.gov/s4pt-ts2/test-bin/s4pa_m2m_cgi.pl?',
             'dirpat' => 's4pt-ts2/data/TERRA/^E/^Y/^j/.hidden',
           }
        },
);



=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

The configuration file defaults to I<s4pm_tk_compose_request.cfg> if not 
specified.

=item B<-o> I<output_jobtype>

Output job type.  Default is REQUEST_DATA.

=item B<-h>

Prints usage statement.

=back

=head1 SEE ALSO

L<s4pm_tk_compose_request.pl>, L<DataRequest>, L<S4PM>

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_compose_request.pl,v 1.3 2007/12/31 22:15:29 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_f $opt_h $opt_i $opt_o $opt_u);
use Getopt::Std;
use Tk;
use S4P::TimeTools;
use S4P::PDR;
use S4P::S4PTk;
use S4PM::DataRequest;
use Safe;

# Process command line (some options exist, but are not advertised in man page
# as they are primarily for testing purposes.)

getopt('f:hi:o:u:');
usage() if $opt_h;

# Read config file and process defaults

my $compartment = new Safe 'CFG';
$compartment->share('$cfg_ur_srvr', '$cfg_increment','%cfg_datatypes', '@cfg_datatypes', 
    '%cfg_location');
$compartment->rdo($opt_f) or
    S4P::perish(30, "main: Failed to read configuration file $opt_f in safe mode: $!");

my $ur_srvr = $opt_u || $CFG::cfg_ur_srvr
                 || 'UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]';

# N.B.:  If %cfg_location is specified, $cfg_ur_srvr will effectively be ignored
my $data_location = (%CFG::cfg_location) ? \%CFG::cfg_location : $ur_srvr;

# Time increment for requests in seconds

my $increment = $opt_i || $CFG::cfg_increment || 7200;

# Output job type:  normally REQUEST_DATA or ACQUIRE_DATA
$opt_o ||= 'REQUEST_DATA';

my @datatypes = @CFG::cfg_datatypes;

# Set up hash based on datatypes

my %dt_selections = %CFG::cfg_datatypes;
map {$dt_selections{$_} = 1} @datatypes if (! keys %dt_selections);

# Construct main window so we can get X defaults and logger redirect going
my $main_window = new MainWindow;
$main_window->title("Compose Data Request Tool");
S4P::S4PTk::read_options($main_window);
S4P::S4PTk::redirect_logger($main_window);

# Entries for entering year and day of year
my ($year, $doy, $hour);
my $time_unit = get_time_unit($increment);
my $day_frame = make_day_frame($main_window, \$year, \$doy, \$hour, $time_unit);
$day_frame->pack(-side=>'top',-fill=>'x');

# Make hours running along top of grid
my ($hour_frame, $ra_time_refs, $ra_times) = make_hour_frame($main_window, $increment);
$hour_frame->pack(-side=>'top');

# Make data availability grid at 2-hour increments
my (%grid_selections);
my $ra_avail_cb = make_avail_grid($hour_frame, \@datatypes, $ra_times, 
    \%dt_selections, \%grid_selections);

# Add database query button
my %results;
$day_frame->Button(-text=>'Query Archive',
    -command=>[\&compose_request, $main_window, \$year, \$doy, \$hour, $ra_time_refs,
               $increment, \%dt_selections, $data_location, \%results, $ra_avail_cb]
    )->pack(-side=>'right');

# Frame with Submit and Exit buttons
my $submit_frame = make_submit_frame($main_window, \%grid_selections);
$submit_frame->pack(-side=>'top');

MainLoop;
exit(0);
##############################################################################
# G U I   C O N S T R U C T I O N
##############################################################################
sub make_avail_grid {
    my ($parent, $ra_datatypes, $ra_times, $rh_dt_selections, 
        $rh_grid_selections) = @_;
    my ($datatype, $time, %cb);

    # For each datatype, create one grid row for each 2 hour block
    foreach $datatype (@{$ra_datatypes}){
        my @cb;
        my $dt_cb = $parent->Checkbutton(-text=>$datatype, 
            -variable=>\$dt_selections{$datatype}, -indicatoron => 0);
        foreach $time (@{$ra_times}) {
            # Bind the checkbutton to the rh_grid_selections variable
            my $cb = $parent->Checkbutton(-indicatoron=>0, -state=>'disabled', 
                -variable=>\$rh_grid_selections->{$datatype}{$time});
            # Collect the checkbuttons for our grid call
            push @cb, $cb;
            $cb{$datatype}{$time} = $cb;
        }
        # Now grid 'em up
        $dt_cb->grid(@cb, -sticky => 'ew');
    }
    return(\%cb);
}    
sub make_day_frame{
    my ($parent, $rs_year, $rs_doy, $rs_hour, $time_unit) = @_;
    my $frame = $parent->Frame;

    # Year entry widget
    $frame->Label(-text=>'Year:')->pack(-side=>'left');
    $frame->Entry(-width=>4, -textvariable => $rs_year)->pack(-side=>'left');

    # Day of year entry widget
    $frame->Label(-text=>'Day of Year:')->pack(-side=>'left');
    $frame->Entry(-width=>3, -textvariable => $rs_doy)->pack(-side=>'left');

    if ($time_unit eq 'm') {
        # Hour entry widget
        $frame->Label(-text=>'Hour of Day:')->pack(-side=>'left');
        $frame->Entry(-width=>2, -textvariable => $rs_hour)->pack(-side=>'left');
    }
    return $frame;
}
sub make_hour_frame{
    my ($parent, $increment) = @_;
    my $t;
    my @cb_val_refs;
    my $frame = $parent->Frame;
    my $n_cols;

    # Array of two hour blocks (or 5-minute blocks for small increments)
    my @times;
    my $time_unit = get_time_unit($increment);
    if ($time_unit eq 'h') {
        $n_cols = (24 * 3600) / $increment;
        my $multiplier = $increment / 3600;
        @times = map {$_ * $multiplier} 0..($n_cols-1);
    }
    else {
        $n_cols = 3600 / $increment;
        my $multiplier = $increment / 60;
        @times = map {$_ * $multiplier} 0..($n_cols-1);
    }
    my @cb;
    foreach $t(@times) {
        my $cb_val = $t;
        # Bind the text string (time in hours) to on value
        push @cb, $frame->Checkbutton(-text => sprintf("%02d", $t), 
            -indicatoron => 0, -onvalue => $t, -offvalue => undef,
            -variable => \$cb_val);
        push (@cb_val_refs, \$cb_val);
    }
    $frame->Label(-text => "Data Type\tHH:")->grid(@cb);
    return($frame, \@cb_val_refs, \@times);
}
sub make_submit_frame {
    my ($parent, $rh_grid_selections) = @_;
    my $frame = $parent->Frame;

    # Submit Request button
    $frame->Button(-text=>'Submit Request', 
        -command => [\&submit_request, $parent, $rh_grid_selections]
        )->pack(-side=>'left');

    # Exit Button
    $frame->Button(-text => 'Exit', -command => sub { exit 0; }
        )->pack(-side=>'right');
    return $frame;
}
###########################################################################
# C A L L B A C K S
###########################################################################
sub compose_request {
    my ($main_window, $rs_year, $rs_doy, $rs_hour, $ra_time_refs, $increment,
        $rh_datatype_selections, $data_location, $rh_results, $rh_cb) = @_;
    
    # Decide if we are using hours or minutes
    my $time_unit = get_time_unit($increment);

    validate_day_input($rs_year, $rs_doy, $rs_hour, $time_unit) or return 0;

    # Find min and max times
    my $start = ($time_unit eq 'h') ? 24 : 60;
    my $stop = 0;
    my $rs_time;
    my %selected_times;
    foreach $rs_time(@{$ra_time_refs}) {
        next unless (defined $$rs_time);
        $start = $$rs_time if ($$rs_time < $start);
        $stop = $$rs_time if ($$rs_time > $stop);
        $selected_times{$$rs_time} = 1;
    }

    # Make yyyydddhhmmss time formats out of them so we can convert
    my ($t1, $t2);
    if ($time_unit eq 'h') {
        $t1 = sprintf("%04d%03d%02d00", $$rs_year, $$rs_doy, $start);
        $t2 = sprintf("%04d%03d%02d00", $$rs_year, $$rs_doy, $stop);
    }
    else {
        $t1 = sprintf("%04d%03d%02d%02d", $$rs_year, $$rs_doy, $$rs_hour, $start);
        $t2 = sprintf("%04d%03d%02d%02d", $$rs_year, $$rs_doy, $$rs_hour, $stop);
    }

    # Convert to CCSDSa formats
    my @ccsds_times = map {S4P::TimeTools::yyyydddhhmmss2CCSDSa($_)} ($t1, $t2);

    # Add increment to max time we found selected in order to get stop time
    $ccsds_times[1] = S4P::TimeTools::CCSDSa_DateAdd($ccsds_times[1], $increment);

    # Find out which datatypes were seleeted by user
    my ($datatype, @datatypes);
    foreach $datatype(keys %{$rh_datatype_selections}) {
        push (@datatypes, $datatype) if $rh_datatype_selections->{$datatype};
    }

    # Execute database query:  result is hash keyed on CCSDSa time bins
    $main_window->Busy(-recurse => 1);
    my %results = S4PM::DataRequest::compose_request(@ccsds_times, $increment,
        \@datatypes, $data_location);
    $main_window->Unbusy(-recurse => 1);

    # Go through results and put them in a hash of hashes
    %{$rh_results} = %results;
    my $total_size = 0;
    my $bin;
    my %avail;
    my $time_unit_start = ($time_unit eq 'h') ? 11 : 14;
    foreach $bin(keys %results) {
        my $pdr = $results{$bin};
        my $time = int(substr($bin, $time_unit_start, 2));
        my $fg;
        foreach $fg(@{$pdr->file_groups}) {
            my $dt = sprintf("%s.%03d", $fg->data_type, $fg->data_version);
            # We squirrel away both the file and the time bin it was in
            # Using an anonymous array
            # Not strictly necessary, but makes it easier to reconstruct when
            # we write it all out again in submit_request
            push @{$avail{$dt}{$time}}, [$fg, $bin];
            map {$total_size += $_->file_size} @{$fg->file_specs};
        }
    }
    # Now go through results hash-hash and configure checkbuttons:
    # If results obtained and time window selected:
    #    Make checkbutton active
    #    Set text to number of files (file_groups)
    #    Set onvalue to externalized string of file_groups
    #      (Used to use pointer to array of files (file_groups), 
    #      but were thwarted by obscure Perl bug)
    my ($datatype, $time);
    foreach $datatype(keys %{$rh_cb}) {
        foreach $time(keys %{$rh_cb->{$datatype}}) {
            if ($selected_times{$time} && exists $avail{$datatype}{$time}) {
                my @fg_bin = @{$avail{$datatype}{$time}};
                my $n_fg_bin = scalar(@fg_bin);
                my $fg_string = join('|', map {$_->[0]->sprint, $_->[1]} @fg_bin);
                $rh_cb->{$datatype}{$time}->configure(-text=>$n_fg_bin, 
                    -state => 'normal', -onvalue=> $fg_string);
                $rh_cb->{$datatype}{$time}->select;
            }
            else {
                $rh_cb->{$datatype}{$time}->configure(-text=>'', 
                    -state => 'disabled', -onvalue=>undef);
                $rh_cb->{$datatype}{$time}->deselect;
            }
        }
    }
    `echo "done configuring checkbuttons" >> tk.log`;
}
sub exit_program {
    my $parent = shift;
    # Confirm before quitting
    (S4P::S4PTk::confirm($parent, "Are you sure you want to exit?")) ? exit(0) : return 0;
}
sub submit_request {
    my ($parent, $rh_grid_selections) = @_;
    my $datatype;
    my %granules;
    my ($fg_string, $time, @files);

    # Go through and place the selected files (file_groups) in the right bin
    # Bin is the full CCSDSa time, not the abbreviated HH the hash is keyed on
    my %binsize;
    foreach $datatype(keys %{$rh_grid_selections}) {
        foreach $time(keys %{$rh_grid_selections->{$datatype}}) {
            my $fg_bin;
            next if ! $rh_grid_selections->{$datatype}{$time};

            # Checkbox value string: file_group|bin|file_group|bin...
            my $cb_value = $rh_grid_selections->{$datatype}{$time};
            my %fg_bin = split('\|', $cb_value);
            foreach $fg_string(keys %fg_bin) {
                my $bin = $fg_bin{$fg_string};

                # Convert FILE_GROUP string to a FileGroup object
                my $fg = S4P::FileGroup->new('text' => $fg_string);

                # Calculate total filesize for the bin
                map {$binsize{$bin} += $_->file_size} @{$fg->file_specs};

                push (@{$granules{$bin}}, $fg);
            }
        }
    }

    # Get confirmation from user
    my @msg = ("Do you want to create the following requests?");
    my $total_size;
    foreach my $bin(sort keys %binsize) {
        my $gb = $binsize{$bin}/(1024.*1024.*1024.);
        $total_size += $gb;
        push (@msg, sprintf("%s (%.1f GB)", $bin, $gb));
    }
    push (@msg, sprintf("Total size: %.1f GB", $total_size));
    return 0 if (! S4P::S4PTk::confirm($parent, join("\n", @msg)));
    
    # User says OK, so...
    # Foreach bin, write out a separate PDR
    foreach my $bin(sort keys %granules) {
        # Construct PDR
        my $pdr = new S4P::PDR;
        $pdr->file_groups($granules{$bin});
        $pdr->recount;

        # Convert to YYYYDDDHHMMSS format for filename
        my $timestamp = S4P::TimeTools::CCSDSa2yyyydddhhmmss($bin);
        my $file = "DO.$opt_o.$timestamp.wo";
        if ($pdr->write_pdr($file)) {
            S4P::logger('ERROR', "submit_request(): Failed to write PDR to $file");
            return 0;
        }
        else {
            push (@files, $file);
        }
    }
    S4P::logger('INFO', join("\n", 'submit_request(): Data Requests were written to the following files:', @files));
    return 1;
}
#########################################################################
# S U P P O R T    R O U T I N E S
#########################################################################
sub usage {
   print STDERR << "EOF";
Usage:
$0
-f config_file (Default: s4pm_tk_compose_request.cfg)
-i increment (Default: 7200 seconds)
-u ur_server (Default: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV])
-h (Prints this statement)
EOF
    exit(1);
}
sub validate_day_input {
    my ($rs_year, $rs_doy, $rs_hour, $time_unit) = @_;
    my @err;
    # Check year
    my ($min_year, $max_year) = (1996, 2010);
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
    # Check hour
    if ($time_unit eq 'm') {
        if (! $$rs_hour) {
            push (@err, "You must specify an hour.");
        }
        elsif ($$rs_hour < 0 || $$rs_hour > 23) {
            push (@err, "Day of year must be between 0 and 23.");
        }
    }
    if (@err) {
        S4P::logger('ERROR', join("validate_day_input():\n", '', @err));
        return 0;
    }
    else {
        return 1;
    }
}
sub get_time_unit {
    my $increment = shift;
    return ( ($increment >= 3600) ? 'h' : 'm' );
}
