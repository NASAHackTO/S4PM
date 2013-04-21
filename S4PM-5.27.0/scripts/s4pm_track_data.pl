#!/usr/bin/perl
#######################################################################

=head1 NAME

s4pm_track_data.pl - DB_File based granule tracking database

=head1 SYNOPSIS

s4pm_track_data.pl
B<-t> I<transaction>
[B<-l> I<logfile>]
[B<-p> I<path_db>]
[B<-u> I<uses_db>]
[B<-e> I<expect_db>]
[B<-H>]

=head1 DESCRIPTION

Track Data uses two databases based on DB_File to keep track of a file's 
"uses" and path (directory).  The hash key is the granule_id, which is 
normally the file name. However, for multi-file files, the granule_id is the 
basename of the directory where the files reside. Uses is the number of times 
a file must be used for either downstream processing or export before 
it can be cleaned off the system.

Track Data accepts three kinds of transactions, Insert, Update, and Expect.  
The Insert transaction adds the file to the uses and path databases.
The Update transaction decrements the uses for a file according to the
number specified in the input work order.  If an Update decrements the uses
a number less than or equal to zero, the file is deleted from the 
uses and path databases, and a SWEEP work order is created, to be sent
to the downstream Sweep Data station.

File names in the input work orders may be full qualified path and file names
or may include wildcard '*' characters. When a wildcard character is included,
any file in the uses.db database file matching that pattern will be affected
by the changes. Currently, the only wildcard character supported is '*'
representing zero or more of any character (any legal file name character).

=head2 INSERT transaction

This causes an entry to be added to the uses database and the path database
(if one does not already exist--see EXPECT).

=head2 EXPECT transaction

Currently used only by on-demand processing, this is similar to the INSERT 
except that the data have not yet arrived.  The number of uses is recorded in 
expect.db instead of uses.db.  The purpose is to record multiple "expectations" 
that may arrivee before the data actually arrive.

When the INSERT work order is received, the expected uses are inserted into 
the users.db and the expect.db entry is deleted.

The EXPECT work order is particularly necessary when multiple requests are 
received for the same data.  The Request Data station will silently ignore the 
subsequent requests (no need to ask for the same data twice). Yet we must 
record the fact that more uses are expected than indicated by the single 
INSERT work order that is received.

=head2 UPDATE transaction

This decrements the uses.db according to the number of uses in the work order 
(usually -1).  If the result in uses.db is 0, the entry will be deleted and 
a SWEEP work order will be written.

=head1 ARGUMENTS

=over 4

=item B<-t> I<transaction>

Type of transaction, either I (Insert), U (Update), or E (EXPECT). This argument
is required, and the first letter must be either E, I or U (or lowercase 
version).

=item B<-l> I<logfile>

Log file in which to log transactions.  Default is not to log.

=item B<-p> I<path_db>

Pathname of the path database.  Default is '../path.db'.

=item B<-u> I<uses_db>

Pathname of the uses database.  Default is '../uses.db'.

=item B<-e> I<expect_db>

Pathname of the expect database.  This has no default:  if not specified, then
"EXPECT" processing is not performed.

=item B<-H>

Data handles in use.  This causes Track Data to keep a reverse lookup database,
handle.db, with the filenames as key and UR files as values.

=back

=head1 BUGS

It uses the first filename in the .UR file, so results with multi-file granules
may be unpredictable.

=head1 AUTHOR

Chris Lynnes

=cut

################################################################################
# s4pm_track_data.pl,v 1.4 2007/04/10 20:28:10 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use DB_File;
use File::Basename;
use Getopt::Std;
use S4PM::Handles;
use S4P;
use vars qw($opt_e $opt_l $opt_p $opt_u $opt_t $opt_H);

# Parse and check arguments
getopts('e:l:p:t:u:H');
my $transaction;
if (! $opt_t || $opt_t !~ /^[eiu]/i) {
    S4P::perish(3, "Transaction type (-t) must be E, I or U");
}
elsif ($opt_t =~ /^e/i && ! $opt_e) {
    S4P::perish(4, "Transaction type of E must be accompanied with -e <expect.db> option");
}
else {
    # Allows for i, u, I, U, Insert, Update, etc.
    $transaction = uc(substr($opt_t, 0, 1));
}
my $uses_dbfile = $opt_u || "../uses.db";
my $path_dbfile = $opt_p || "../path.db";
my $handle_dbfile = "../handle.db" if $opt_H;
my $expect_dbfile = $opt_e;  # Also flags whether to do EXPECT 
                             # processing, thus no default

# Read work order
my ($granule_id, %uses, %path, %expect, %data_handle);

my $input_file = $ARGV[0];

# Open databases
my $rh_uses_db = open_db($uses_dbfile);
my $rh_path_db = open_db($path_dbfile);
my $rh_expect_db = open_db($expect_dbfile) if ($expect_dbfile);
my $rh_handle_db = open_db($handle_dbfile) if ($handle_dbfile);

# First read and check input before we try modifying anything
while (<>) {
    my ($file_id, $uses) = split;

    # Strip "FileId="
    $file_id =~ s/^FileId=//;

    # Strip trailing slash if it's there
    $file_id =~ s#/*$##;

    # Strip "Uses="
    $uses =~ s/^Uses=//;
    
    # Granule_id is the last element of the path
    $granule_id = basename($file_id);
    my $path = dirname($file_id);

    if ( $granule_id =~ /\*/ ) {

        S4P::logger("DEBUG", "main: Granule ID contains a wildcard: [$granule_id]");
        $granule_id =~ s/\*/\.\*/g;
        S4P::logger("DEBUG", "main: Granule ID pattern is now: [$granule_id]");

      # Find all files in DB matching pattern

        S4P::logger("DEBUG", "main: Scanning DB for pattern: [$granule_id]");
        foreach my $key ( keys %{$rh_uses_db} ) {
            S4P::logger("INFO", "key: [$key]");

            if ( $key =~ /$granule_id/ ) {
                S4P::logger("DEBUG", "Key: [$key] matches pattern: [$granule_id]");
                $uses{$key} = $uses;
                $path{$key} = $path;
            } else {
                S4P::logger("DEBUG", "Key: [$key] does NOT match pattern: [$granule_id]");
            }
        }
    } 
    else {
        # In some cases, an UPDATE work order contains data file even though
        # data handles are in effect.  So we need the reverse lookup.
        if ($opt_H) {
            if ($granule_id !~ /\.ur$/) {
                $granule_id = handle_lookup($file_id, $rh_handle_db);
            }
            elsif ($transaction eq 'I') {
                my @files = S4PM::Handles::get_filenames_from_handle($file_id);
                $data_handle{$granule_id} = $files[0];
            }
        }

        # Add to hashes in memory
        if ($granule_id) {
            $uses{$granule_id} = $uses;
            $path{$granule_id} = $path;
        }
    }
}

# Save paths to be deleted in @delete for output work order
my @delete;

# Loop through files
foreach $granule_id(keys %uses) {
    my $uses = $uses{$granule_id};
    my $path = $path{$granule_id};
    my $data_handle = $data_handle{$granule_id} if $opt_H;
    my $exists = exists($rh_uses_db->{$granule_id});
        
    # EXPECT
    if ($transaction eq 'E') {
        log_transaction($opt_l, 'EXPECT', $granule_id, $path, $uses, $input_file);
        # Insert already received, but not yet updated out of existence
        if ($exists) {
            $rh_uses_db->{$granule_id} += $uses;
        }
        else {
            $rh_expect_db->{$granule_id} += $uses;
        }
    }
    # INSERT    
    elsif ($transaction eq 'I') {
        # If already expected, uses the expected uses
        if ($opt_e && $rh_expect_db->{$granule_id}) {
            $uses = $rh_expect_db->{$granule_id};
            delete $rh_expect_db->{$granule_id};
        }
        my $log_trans = ($exists) ? 'INSERT_DUP' : 'INSERT';
        log_transaction($opt_l, $log_trans, $granule_id, $path, $uses);
        # Will overwrite in case of INSERT_DUP; could be useful...
        $rh_uses_db->{$granule_id} = $uses;
        $rh_path_db->{$granule_id} = $path;
        $rh_handle_db->{$data_handle} = $granule_id if $opt_H;
    }
    # UPDATE
    elsif ($transaction eq 'U') {
        if (! $exists) {
            log_transaction($opt_l, "UPDATE_GHOST", $granule_id, $path, $uses, $input_file);
            next;  # Save an indent level
        }
        my $new_uses = $rh_uses_db->{$granule_id}  + $uses;

        log_transaction($opt_l, "UPDATE", $granule_id, $path, $uses, $input_file);

        if ($new_uses > 0) {
            $rh_uses_db->{$granule_id} = $new_uses;
        }
        else {
            # Save for output work order
            push(@delete, "$rh_path_db->{$granule_id}/$granule_id");

            # If now useless, proceed with deletion
            delete $rh_uses_db->{$granule_id};
            delete $rh_path_db->{$granule_id};
            delete $rh_handle_db->{$data_handle} if $opt_H;

            log_transaction($opt_l, "DELETE", $granule_id, $path);
        }
    }
}

# Release the databases
untie %{$rh_uses_db};
untie %{$rh_path_db};
untie %{$rh_handle_db} if $opt_H;

# Write output work order
if (@delete) {
    # Output file name: replace leading DO.UPDATE w/ SWEEP, add .wo 
    # on end
    my $output_file = $input_file;
    $output_file =~ s/^DO\.UPDATE/SWEEP/;
    $output_file =~ s/$/.wo/;
    S4P::write_file($output_file, join("\n", @delete, "\n"))
        ? S4P::logger('INFO', "Wrote output work order $output_file")
        : S4P::perish(20, "Failed to write output file $output_file");
}
exit(0);

sub handle_lookup {
    my ($file_id, $rh_handle) = @_;
    my $granule_id = $rh_handle->{$file_id};
    if ($granule_id) {
        return $granule_id;
    }
    else {
        S4P::logger('ERROR', "Cannot find handle for $granule_id");
        return;
    }
}

###############################################################################
# log_transaction:  append brief info about transaction to log file
sub log_transaction {
    my ($logfile, $transaction, $granule_id, $location, $uses, $input_file) = @_;
    my $job_id;

    if ($input_file) {
        $job_id = (split('\.', $input_file))[2];
    }
    # Open for concatenation
    open LOG, ">>$logfile" or
       S4P::perish(110, "Cannot open logfile $logfile for writing: $!");
    print LOG S4P::timestamp(), " $transaction $location/$granule_id";
    print LOG " $uses" if $uses;
    print LOG " $job_id" if $job_id;
    print LOG "\n";
    close LOG;
}

###############################################################################
# open_db:  open database
###############################################################################
sub open_db {
    my ($file) = @_;
    my %hash;
    tie(%hash, 'DB_File', $file) or 
       S4P::perish(110, "Cannot open DBM file $file: $!");
    return \%hash;
}
