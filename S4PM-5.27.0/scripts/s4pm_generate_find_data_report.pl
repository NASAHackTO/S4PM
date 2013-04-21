#!/usr/bin/perl

=head1 NAME

s4pm_generate_find_data_report.pl - generate report from find_data.log

=head1 SYNOPSIS

s4pm_generate_find_data_report.pl
[B<-b[egin]> I<begin calendar date/time>]
[B<-e[nd]> I<end calendar date/time>]
[B<-t[oday]>]
[B<-y[esterday>]
[B<-s[hort>]
I<find_data_log> ...
 
=head1 DESCRIPTION
 
B<s4pm_generate_find_data_report.pl> examines the log file produced by the 
B<s4pm_find_data.pl> script in the Find Data station and generates a report 
on stdout showing: 

=over 4

=item 1.

Those algorithm runs that were made without the full compliment of input data
and for which there are no other subsequent entries showing that the run
was repeated with all input data.

=item 2.

The statistics for input usage showing the numbers and percentages of runs
made using the current, previous n, following n, or no granule. These stats
are listed for each algorithm.

The B<-today> produces a report for runs made and logged on the current day. 
The B<-yesterday> produces a report for runs made and logged during the 
previous day. The date range may be tailored using the B<-begin> and B<-end> 
arguments. If either is not specified, the default is to examine all runs from 
the earliest entry in the log file (if B<-begin> is omitted) to the last entry
in the log file (if B<-end> is omitted). If no time option is specified, then
all log entries are examined and included in the statistics.

The B<-short> option reduces the output by only showing the summary statistics
on input data usage.

=head1 AUTHOR
 
Stephen Berrick, NASA/GSFC, Code 610.2
 
=cut
 
################################################################################
# s4pm_generate_find_data_report.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################
 
use strict;
use S4P::TimeTools;
use Getopt::Long;

use vars qw(%ENTRIES
            %BAD
            $BEGIN
            $END
            $TODAY
            $YESTERDAY
            $SHORT
           );

$BEGIN     = undef;
$END       = undef;
$TODAY     = undef;
$SHORT     = undef;
$YESTERDAY = undef;

GetOptions( "begin=s"   => \$BEGIN,
            "end=s"     => \$END,
            "today"     => \$TODAY,
            "yesterday" => \$YESTERDAY,
            "short"     => \$SHORT,
          );

my ($begin, $end, $report_title);

if ( $YESTERDAY ) {
    my $date = `/bin/date '+%Y-%m-%d'`;
    chomp($date);
    my $b   = $date . "T00:00:00Z";
    my $e   = $date . "T23:59:59Z";
    $begin = S4P::TimeTools::CCSDSa_DateAdd($b, -86400);
    $end   = S4P::TimeTools::CCSDSa_DateAdd($e, -86400);
    my $ydate = $begin;
    $ydate =~ s/T.*Z$//;
    $report_title = "Yesterday, $ydate ";
} elsif ( $TODAY ) {
    my $date = `/bin/date '+%Y-%m-%d'`;
    chomp($date);
    $begin = $date . "T00:00:00";
    $end   = $date . "T23:59:59";
    $report_title = "Today, $date ";
} else {
    if ( $BEGIN ) {
        $begin = is_valid_time($BEGIN);
        unless ( $begin ) {
            die "\nInvalid begin date/time. Try again!\n\n";
        }
        $report_title = "$begin ";
    } else {
        $begin = "1900-01-01T00:00:00";
        $report_title = "Earliest Logged Entry ";
    }
    $end = is_valid_time($END);
    if ( $END ) {
        unless ( $end ) {
            die "\nInvalid end date/time. Try again!\n\n";
        }
        $report_title .= "To $end";
    } else {
        $end = "3000-01-01T00:00:00";
        $report_title .= "To Last Logged Entry";
    }
}

# Make sure the end date/time is AFTER the begin date/time

unless ( S4P::TimeTools::CCSDSa_DateCompare($begin, $end) == 1 ) {
    die "Begin date/time needs to be EARLIER than end date/time!\n\n";
}

unless ( $ARGV[0] ) {
    die "\nYou need to specify at least one Gran Find log file as an argument!\n\n";
}

my %totals = ();	# Hash to keep track of totals

foreach my $logfile ( @ARGV ) {

    open(QA, "$logfile") or die "Failed to open file: $logfile: $!\n\n";

### Read everything into a hash where the hash keys are algorithm names plus 
### data times and the hash values are the entries corresponding to those data 
### times

    while ( <QA> ) {
        if ( /^PGE=([^\s]+)\sDATADATE=([^\s]+)\s(.*)$/ ) {
            my $pge      = $1;
            my $datadate = $2;
            my $rest     = $3;
            my $proddate;
            if ( $rest =~ /PRODDATE=([^\s]+)/ ) {
                $proddate = $1;
            }
            if ( S4P::TimeTools::CCSDSa_DateCompare($proddate, $begin) == 1 or
                 S4P::TimeTools::CCSDSa_DateCompare($proddate, $end) == -1 ) {
                next;	# Skip this entry as it is out of range
            }
            if ( exists $totals{$pge} ) {
                $totals{$pge}++;
            } else {
                $totals{$pge} = 1;
            }
            $rest =~ s/\s+$//;
            $rest = tag_duplicates($rest);
            push( @{$ENTRIES{"$pge|$datadate"}}, $rest);
        } else {
            warn "\nFailed to parse this entry:\n\n$_\n\n";
        }
    }
    close QA or die "\nFailed to close file: $logfile: $!";
}

my %triage = ();	# Hash for data times where a "NONE" was detected.
			# These same data times may or may not have been run
			# again to correct missing input data.

foreach my $entry ( keys %ENTRIES ) {
    my $num_samples = scalar( @{$ENTRIES{$entry}} );
    my ($pge, $dd) = split(/\|/, $entry);
    my $bad = 0;
    my $sample;
    foreach $sample ( @{$ENTRIES{$entry}} ) {

####### If a "NONE" is detected in any of the entries, mark it as "bad" and
####### stash it away into a triage hash for later examination.

        if ( $sample =~ /NONE/ ) {
            $bad = 1;
        }
    }
    if ( $bad == 1 ) {
        push( @{$triage{$entry}}, @{$ENTRIES{$entry}} );
    }
}

# Get hash of those entries in which there is a NONE. See which, if any,
# had been run again to correct the missing input data.

get_bad(%triage);

print "Find Data Log Report For $report_title\n\n";

unless ( $SHORT ) {

### Print out the results

    print <<"EOF";
--------------------------------------------------------------------------------
Data Times With Missing Optional Input Data
--------------------------------------------------------------------------------

    The following are entries for which at least one optional input was missing
AND there is no indication that the run was done at a later time with the
missing input. The list is organized by algorithm name, then data time, and 
finally, each production time is shown with those inputs missing.
EOF

    my $curr_pge = "";
    foreach my $entry ( sort keys %BAD ) {
        my ($pge, $dd) = split(/\|/, $entry);
        $dd =~ s/Z$//;
        if ( $pge eq $curr_pge ) {
            print "    $dd:\n";
        } else {
            print "\n$pge\n\n    $dd:\n";
            $curr_pge = $pge;
        }
        foreach my $sample ( @{$BAD{$entry}} ) {
            my $none_list = get_none_list($sample);
            print "\t$none_list\n";
        }
    }
}

# Get statistical information on ancillary data usage

my %stats = get_stats();

print <<"EOF";

--------------------------------------------------------------------------------
Statistics of Input Data Usage For ALL Jobs
--------------------------------------------------------------------------------

    Below are listed the input data usage for ALL algorithm runs. This includes 
data times that were run more than once to, for example, pick up an ancillary 
input file that had been missing in a previous run. The data show the numbers 
for C (current), Pn (previous n), Fn (following n), and N (none) along with the
percentage in parentheses (relative to the total number of runs).
EOF

my ($datatype, $curr_str);

format PRETTY =
@<<<<<<< @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
$datatype, $curr_str
.

foreach my $pgename ( keys %stats ) {
#   print "\n--------------------------------------------------------------------------------\n";
    print "\n******** $pgename:\n";
#   print "--------------------------------------------------------------------------------\n";
    foreach $datatype ( keys %{$stats{$pgename}} ) {
        $curr_str = "";
        foreach my $currency ( keys %{$stats{$pgename}{$datatype}} ) {
            my $c = abbreviate($currency);
            my $frac = ($stats{$pgename}{$datatype}{$currency}/$totals{$pgename} ) * 100;
            my $num = sprintf("%5d (%5.1f)", $stats{$pgename}{$datatype}{$currency}, $frac);
            $curr_str .= "$c: $num ";
        }
        $~ = "PRETTY";
        $datatype =~ s/:[0-9]+$//;
        write;
    }
    $~ = "STDOUT";
}

sub get_bad {

    my %triage = @_;

    foreach my $entry ( keys %triage ) {
        my $num_samples = scalar( @{$triage{$entry}} );
        if ( $num_samples == 1 ) {
            push( @{$BAD{$entry}}, @{$ENTRIES{$entry}} );
        } else {
            my @sorted_ar = reverse sort by_proddate @{$triage{$entry}};
#           print "Sorted Array: \n\n";
            my $size = scalar(@sorted_ar);
#           for (my $j = 0; $j < $size; $j++) {
#               print "$sorted_ar[$j]\n";
#           }
#           print "size: [$size]\n";
            if ( $sorted_ar[0] =~ /NONE/ ) {
#               print "Entry is marked as BAD\n";
                push( @{$BAD{$entry}}, @{$ENTRIES{$entry}} );
            }
        }
    }
    return;
}

sub by_proddate {

    my $a_date;  
    my $b_date;

    if ( $a =~ /PRODDATE=([^\s]+)/ ) {
        $a_date = $1;
    } else {
        die "Unable to parse production date from: [$a_date]\n";
    }
    if ( $b =~ /PRODDATE=([^\s]+)/ ) {
        $b_date = $1;
    } else {
        die "Unable to parse production date from: [$b_date]\n";
    }

    my $res = $a_date cmp $b_date;
#   print "a_date: [$a_date], b_date: [$b_date], res: [$res]\n";
    return ($a_date cmp $b_date);
}

sub get_none_list {

    my $line = shift;
    my $str;
    my $no_none = 1;

    my @parts = split(/\s/, $line);
    foreach my $part (@parts) {
        my ($esdt, $currency) = split(/=/, $part);
        $esdt =~ s/:[0-9]+$//;
        if ( $esdt eq "PRODDATE" ) {
            $str .= " $currency:";
        } elsif ( $esdt ne "PRODDATE" and $currency eq "NONE" ) {
            $str .= " $esdt, ";
            $no_none = 0;
        }
    }

    if ( $no_none ) {
        $str .= " Nothing missing, but was superseded by later run.";
    }

    $str =~ s/,\s+$//;
    return $str;
}

sub get_stats {

    my %stats_hash = ();

#   print "\nsub get_stats\n\n";

    foreach my $entry ( keys %ENTRIES ) {
        my ($pge, $datadate) = split(/\|/, $entry);
#       print "pge: [$pge], datadate: [$datadate]\n";
        foreach my $sample ( @{$ENTRIES{$entry}} ) {
#           print "sample: [$sample]\n";
            my @parts = split(/\s/, $sample);
            foreach my $part (@parts) {
                my ($esdt, $currency) = split(/=/, $part);
#               print "esdt: [$esdt], currency: [$currency]\n";
                next if ( $esdt eq "PRODDATE" );
                if ( exists $stats_hash{$pge}{$esdt}{$currency} ) {
#                   print "$pge $esdt $currency already exists.\n";
                    $stats_hash{$pge}{$esdt}{$currency}++;
                } else {
#                   print "$pge $esdt $currency does NOT already exist.\n";
                    $stats_hash{$pge}{$esdt}{$currency} = 1;
                }
            }
        }
    }

    return %stats_hash;
}

sub abbreviate {

    my $str = shift;

    $str =~ s/NONE/ N/;
    $str =~ s/CURR/ C/;
    $str =~ s/PREV/P/;
    $str =~ s/FOLL/F/;

    return $str;
}

sub tag_duplicates {
    
    my $str = shift;
    my $newstr = "";
    my @seen = ();
    my $count = 0;

    my @components = split(/\s/, $str);
    foreach my $component (@components) {
        my ($esdt, $val) = split(/=/, $component);
        if ( is_element($esdt, @seen) ) {
            $esdt .= ":$count";
            $count++;
        } else {
            push(@seen, $esdt);
        }
        $newstr .= "$esdt=$val ";
    }

    $newstr =~ s/\s+$//;
    return $newstr;
}

sub is_element {

    my($element, @ar) = @_;

    foreach my $el (@ar) {
        if ( $element eq $el ) {
            return 1;
        }
    }
    
    return 0;
}

sub is_valid_time {

### These formats should be supported:
###
### 03/02/02 12:45
### 03/02/2002 12:45:00
### 03-02-02 12:45
### 3/2/2002 12:45
### 3-2-2002 11:30:00

    my $date = shift;

    if ( $date =~ /^\s*([0-9]{1,2})[\/\-]([0-9]{1,2})[\/\-]([0-9]{2,4})\s+([0-9]{1,2})?:?([0-9]{1,2})?:?([0-9]{1,2})?\s*$/ ) {
        my $month = $1;
        my $day   = $2;
        my $year  = $3;
        my $hour  = $4;
        my $min   = $5;
        my $sec   = $6;
        $sec  = "00" if ( $sec eq "" );
        $min  = "00" if ( $min eq "" );
        $hour = "00" if ( $hour eq "" );
        $year  = "20" . $year if ( length($year) == 2 );
        $month = "0" . $month if ( length($month) == 1 );
        $day   = "0" . $day if ( length($day) == 1 );
        $hour  = "0" . $hour if ( length($hour) == 1 );
        $min   = "0" . $min if ( length($min) == 1 );
        $sec   = "0" . $sec if ( length($sec) == 1 );
        return if ( length($year) != 2 and length($year) != 4 );
#       print "month: [$month], day: [$day], year: [$year], hour: [$hour], min: [$min], sec: [$sec]\n";
        my $str = $year . "-" . $month . "-" . $day . "T" . $hour . ":" . $min . ":" . $sec;
#       print "str: [$str]\n";
        return $str;
    } else {
        return;
    }

}
