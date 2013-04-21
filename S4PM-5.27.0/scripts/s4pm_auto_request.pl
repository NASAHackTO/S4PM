#!/tools/gdaac/COTS/bin/perl

=head1 NAME

s4pm_auto_request.pl - automatically generate data requests for reprocessing

=head1 SYNOPSIS

s4pm_auto_request.pl 
[B<-l>] 
[B<-f> I<config_file>] 
[B<-o> I<output_jobtype> 
[B<-t>]
workorder

=head1 ARGUMENTS

=over 4

=item B<-l>

Log data requests generated.

=item B<-f> I<config_file>

s4pm_tk_compose_request.cfg-style  configuration file. For ECS queries, this 
is not needed. However, for S4PA and MODAPS, this is needed for the 
%cfg_location parameter which specifies the host, directory patterns, filename 
patterns, etc.

=item B<-o> I<output_jobtype>

This overrides the default REQUEST_DATA output work order job type.

=item B[<-t>]

Turbo mode.  If no data are found, skip to the next interval right away.

=back

=head1 DESCRIPTION

s4pm_auto_request.pl queries the ECS database and dumps REQUEST_DATA work
orders into the request_data station at a fixed rate.  The AUTO_REQUEST
work order is run by s4p_repeat_work_order to drive the processing.
On each iteration it checks to see if it is time to submit another request.  
If so, it generates a REQUEST_DATA output work order and appends to the
input work order, updating it with the most recent request.  
If not, it simply exits.  

The output REQUEST_DATA work order has the first datatype appended to the
job_id to avoid collisions when more than one AUTO_REQUEST are running in the 
same station on different datatypes. 

The format of the AUTO_REQUEST work order is:

  X_RATE: <float>
  REQUEST_INCREMENT: <seconds>
  BEGIN_TIME: <CCSDS date/time> 
  END_TIME: <CCSDS date/time> 
  BBOX: <lon1,lat1,lon2,lat2>
  RUN_ID: <identifying string>
  DATATYPES:  <ESDT.VVV> <ESDT.VVV> ...
  =====
  <epoch> <CCSDS> <CCSDS>

BBOX is optional.

The lines following the '=' signs are previous orders:
  the time ordered
  begin_time of period
  end time of period

=head1 N.B.

Be careful if you run this multi-threaded in a station to avoid multiple 
processes requesting the same data and multiplying the actual X rate.

=head1 EXAMPLE

  X_RATE: 1.8
  REQUEST_INCREMENT: 7200
  BEGIN_TIME: 2005-01-02T00:00:00Z
  END_TIME:   2005-01-03T00:00:00Z
  RUN_ID:   Alpha
  DATATYPES: MOD000.001 AM1ATTN0.001 AM1EPHN0.001 GDAS_0ZF.001
  ==

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=head1 LAST REVISED

2006/08/30 15:44:08

=cut

################################################################################
# s4pm_auto_request.pl,v 1.6 2007/07/19 18:25:14 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Getopt::Std;
use vars qw($opt_l $opt_f $opt_o $opt_t);
use File::Basename;
use S4PM::DataRequest;
use S4P::TimeTools;

getopts('f:lo:t');

# Open up the work order file
my $file = shift @ARGV or die "No work order file specified";
open (F, $file) or die "Error opening work order $file: $!";

# Read and parse work order
my (@header, @requests);
my %param;
while (<F>) {
    push @header, $_;
    last if (/^=/);
    chomp;
    next if (/^\s*#/);   # Strip comments, if any
    my ($par, $val) = split(/:\s*/, $_, 2);
    $param{$par} = $val;
}

# Check for required parameters
my $err = 0;
foreach (qw(X_RATE REQUEST_INCREMENT BEGIN_TIME DATATYPES)) {
    unless (exists $param{$_}) {
        warn "Cannot find parameter $_ in $file\n";
        $err++;
    }
}

# Go through the recent history until we get to the last line
my $submitted = 0;
my $end = '';
my $comment = 'NO_DATA';  # Will print this if no data are found
my $begin;
while (<F>) {
    push @requests, $_;
    chomp;
    ($submitted, $begin, $end, $comment) = split;
}
close (F) or die "Failed to close file $file: $!";

# If we are at the end of the requested time period, then quit
if ($end ge $param{'END_TIME'}) {
    print STDERR "All requests have been submitted for time period in this work order\n";
    exit(0);
}

# Calculate time to submit the next request
my $since = $^T - $submitted;
my $interval = $param{'REQUEST_INCREMENT'} / $param{'X_RATE'};
print STDERR "Last submission: $submitted  Program start: $^T\n";
print STDERR "Submission interval:        $interval\n";
print STDERR "Time since last submission: $since\n";

# In turbo mode, if last comment was "NO_DATA", it doesn't count:
# we can submit the next order immediately
if ($opt_t && $comment eq 'NO_DATA') {
    print STDERR "Turbo mode: last order had NO_DATA; setting time since last submission to $interval\n";
    $since = $interval;
}

# Sleep until next submission, based on X rate
if ($since < $interval) {
    my $sleep = $interval - $since;
    my $sched = localtime($^T + $sleep);
    print STDERR "Sleeping for $sleep seconds\n";
    print STDERR "Will submit request at $sched\n";
    sleep($sleep);
}

my @files;
$begin = $end || $param{'BEGIN_TIME'};  # First time through
my $incr = $param{'REQUEST_INCREMENT'};
$begin =~ s/\.\d+Z/Z/;  # Chop off fractional seconds

# See if we have gotten to the end of the time period
my $calc_end = S4P::TimeTools::CCSDSa_DateAdd($begin, $incr);
my $stop = $param{'END_TIME'};
$end = ($calc_end > $stop) ? $stop : $calc_end;

my @bbox;
if ($param{'BBOX'}) {
    @bbox = split(/[,\s]/, $param{'BBOX'});
    die "Invalid BBOX: $param{'BBOX'}\n" unless (scalar(@bbox) == 4);
}

# Specify data location:  either ECS-style (UR_SRVR) or S4PA/MODAPS style (nested hash)
my $location = $opt_f ? read_config($opt_f) : $param{'UR_SRVR'};

# Finally, call submit_request
($end, @files) = submit_request($begin, $end, $incr, $param{'DATATYPES'}, $location, $param{'RUN_ID'}, \@bbox);

$submitted = time();
if (@files) {

    # There *should* be only one file as the increment covers the whole
    # query period
    $comment = $files[0];

    # Move request files to output work order names
    foreach (@files) {
        # Log request to STDOUT
        cat_request($_) if $opt_l;

        # Rename request files (which are input-work-order-style names, 
        # to output-work-order-style name (.wo suffix, no DO. prefix)
        my ($do, $job_type, $job_id, $job_subid, $other) = split('\.', basename($_));
        $job_type = $opt_o if $opt_o;
        my $newname = "$job_type.$job_id.$job_subid.wo";
        rename($_, $newname) or die "Failed to rename $_ to $newname: $!";
        print STDERR "Moved $_ to $newname\n";
    }
}
else {
    $comment = "NO_DATA";
}

# Append submitted request info to the end of the file
push (@requests, "$submitted $begin $end $comment\n");
# Take the first request off the list
# Otherwise, if we cycle 12 requests/day x 30 days (say), the log file will grow to
# a + 1 + 2 + 3 ... + 360 = a + 1 + (181 * 180) = a very big number
shift(@requests) if (scalar(@requests) > 2);

# Write out output work order
# We do our own instead of using s4p_repeat_work_order so we can keep
# the job_id the same.  This prevents other AUTO_REQUEST work orders
# from getting in ahead (in a single-threaded station). 

if ($end ge $param{'END_TIME'}) {
    print STDERR "All requests have been submitted for time period in this work order\n";
    exit(0);
}

# Write out AUTO_REQUEST work order to continue the process
my ($do, $job_type, $job_id, $other) = split('\.', basename($file));
my $outfile = "$job_type.$job_id.$^T.wo";
S4P::write_file($outfile, join('', @header, @requests)) or exit(50);
my $now = localtime();
print STDERR "Wrote output to $outfile: $now\n";

exit(0);

sub cat_request {
    my $file = shift;
    open REQ, $_ or die "Cannot open request file $_: $!";
    $/ = undef;
    my $req = <REQ>;
    close REQ or die "Cannot close request file $req: $!";
    print STDERR ('+' x 24), $file, ('+' x (48-length($file))), "\n";
    print STDERR $req;
    print STDERR ('-' x 72), "\n";
}
sub read_config {
    my $compartment = new Safe 'CFG';
    $compartment->share('%cfg_location');
    $compartment->rdo($_[0]) or
        S4P::perish(30, "main: Failed to read configuration file $_[0] in safe mode: $!");
    my %location = %CFG::cfg_location;
    return \%location;
}

sub submit_request {
    my ($start, $end, $incr, $datatypes, $location, $run_id, $ra_bbox) = @_;
    my $calc_end = S4P::TimeTools::CCSDSa_DateAdd($start, $incr);
    $end = $calc_end unless ($end && ($calc_end gt $end));

    # Query database and write requests
    my @datatypes = split(/\s+/, $datatypes);
    print STDERR "Querying from $start to $end.\n";
    my %results = S4PM::DataRequest::compose_request($start, $end, $incr,
        \@datatypes, $location, $ra_bbox);
    my @files;
    if (%results) {
        # Form JOB_ID from the first datatype, plus etc if more
        my $job_id = $run_id;
        unless ($job_id) {
            $job_id = shift @datatypes;
            $job_id =~ s/\.\d+$//;
            $job_id .= '_etc' if (@datatypes);
        }
        @files = S4PM::DataRequest::write_requests(\%results, $job_id, $run_id);
    }
    return ($end, @files);
}
