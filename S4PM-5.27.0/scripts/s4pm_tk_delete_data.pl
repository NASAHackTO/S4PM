#!/usr/bin/perl

=head1 NAME

s4pm_tk_delete_data.pl - send work order to Track Data to delete data

=head1 SYNOPSIS

s4pm_tk_delete_data.pl -d database

=head1 DESCRIPTION

B<s4pm_tk_delete_data.pl> presents user with a list of the files in the 
database.  The user selectes one or many for deletion, then presses the 
Delete button.  This writes an UPDATE work order to the current directory.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_delete_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Tk;
use Getopt::Std;
use S4P::S4PTk;
use DB_File;
use strict;
use vars qw($opt_d);

getopts('d:');
my $database = $opt_d || "uses.db";
die "Database $database not found\n" unless (-f $database);

# Construct user interface
my @sorted_records;
my ($short_name_filter, $yyyyddd_filter);
my $main = MainWindow->new();
S4P::S4PTk::read_options($main);
$main->title("View/Delete Data");
my $button_frame = $main->Frame->pack(-anchor=>'n', -side=>'top',-expand=>0);
my $clean_button = $button_frame->Button(-text=>'Delete')->pack(-side=>'left');
my $refresh_button = $button_frame->Button(-text=>'Refresh')->pack(-side=>'right');
my $exit_button = $button_frame->Button(-text=>'Exit', -command=>sub {exit 0})->pack(-side=>'right');
my $filter_frame = filter_frame($main, \$short_name_filter, \$yyyyddd_filter);
my $listbox = $main->Scrolled('Listbox', -width=>60, -scrollbars=>'e',
        -selectmode=>'multiple')->pack(-anchor=>'n', -expand=>1, -fill=>'both');
$clean_button->configure(-command=>[\&clean_data, $main, $listbox, \@sorted_records]);
$refresh_button->configure(-command=>[\&refresh, $listbox, $database, 
    \@sorted_records, \$short_name_filter, \$yyyyddd_filter]);

S4P::S4PTk::redirect_logger($main);

$refresh_button->invoke;

MainLoop();

sub clean_data {
    my ($main, $listbox, $ra_data) = @_;

    # Get selected indices
    my @list = $listbox->curselection();

    # Go through list of selected items, forming work order text as we go
    my @lines;
    foreach (@list) {
        my $record = $ra_data->[$_];
        my ($gid, $uses, $dir) = @$record;
        push(@lines, sprintf("FileId=%s/%s Uses=%d\n", $dir, $gid, -$uses));
    }

    # Output work order file
    if (S4P::S4PTk::confirm($main, "OK to submit work order?\n" . join('', @lines))) {
        my $filename = sprintf("DO.UPDATE.MANUAL.%d.wo", time());
        if (!open WO, ">$filename") {
            S4P::logger('ERROR', "Cannot open output work order $filename: $!");
            return 0;
        }
        print WO join("\n", @lines), "\n";
        close WO;
        S4P::logger('INFO', "File $filename successfully written!");
        $listbox->selectionClear(0, 'end');
    }
    return 1;
}

sub filter_frame {
    my ($main, $rs_short_name_filter, $rs_yyyyddd_filter) = @_;
    my $frame = $main->Frame->pack(-anchor=>'n', -side=>'top',-expand=>0);
    $frame->Label(-text=>'Filter On: ')->pack(-side=>'left');
    $frame->Label(-text=>'Data Type: ')->pack(-side=>'left');
    $frame->Entry(-textvariable=>$rs_short_name_filter)->pack(-side=>'left');
    $frame->Label(-text=>'YYYYDDD: ')->pack(-side=>'left');
    $frame->Entry(-textvariable=>$rs_yyyyddd_filter)->pack(-side=>'left');
}

sub refresh {
    my ($listbox, $database, $ra_records, $rs_shortname_filter, 
        $rs_yyyyddd_filter) = @_;
    if ($database =~ /\.csv$/) {
        refresh_csv(@_);
    }
    else {
        refresh_dbm(@_);
    }
}
sub refresh_dbm {
    my ($listbox, $database, $ra_records, $rs_shortname_filter, 
        $rs_yyyyddd_filter) = @_;
    my (%path, %uses, @lines);
    if (!tie(%uses, 'DB_File', 'uses.db')) {
        S4P::logger('Error', "Cannot open DBM file uses.db: $!");
    }
    elsif (!tie(%path, 'DB_File', 'path.db')) {
        S4P::logger('Error', "Cannot open DBM file path.db: $!");
    }
    else {
        my $filter = grep_filter($$rs_shortname_filter, $$rs_yyyyddd_filter);
        @$ra_records = ();
        foreach my $granule(grep /$filter/, sort keys %path) {
            push @$ra_records, [$granule, $uses{$granule}, $path{$granule}];
            push(@lines, sprintf("%-50.50s %3d", $granule, $uses{$granule}));
        }
    }
    $listbox->delete(0, 'end');
    $listbox->insert('end', @lines);
    untie %uses;
    untie %path;
}
sub grep_filter{
    my ($shortname_filter, $yyyyddd_filter) = @_;
    # Build up filter for grep function
    my @filter;
    push(@filter, "^$shortname_filter\\b") if defined $shortname_filter;
    push(@filter, "\\.[AP]$yyyyddd_filter\\.") if defined $yyyyddd_filter;
    my $filter = join('.*', @filter) if (@filter);
    $filter .= '.*';
    return $filter;
}
