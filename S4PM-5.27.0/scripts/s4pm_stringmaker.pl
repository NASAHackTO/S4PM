#!/usr/bin/perl

=head1 NAME

s4pm_stringmaker.pl - setup or change configuration for an S4PM string

=head1 SYNOPSIS

s4pm_stringmaker.pl
[B<-a>]
[B<-c>]
[B<-u>]
[B<-T>]
[B<-h>]
[B<-q> I<dir>]
[B<-t> I<tmpdir>]
[B<-d> I<directory>]
B<-s> I<string_cfg_file>

=head1 ARGUMENTS

=over 4

=item B<-u>

Update string configuration.

=item B<-T>

Create, submit, and close a Remedy Trouble Ticket (TT) after 
B<s4pm_stringmaker.pl> finishes. After B<s4pm_stringmaker.pl> completes, 
it will prompt for the userid (assumed to exist as a Remedy AND UNIX 
account on g0mss10). A short description, which is used in the problem portion 
of the ticket will also be asked for. The TT will then be automatically 
submitted and updated with a close.

=item B<-c>

Create new string. (Can also be used to update.)

=item B<-a>

Append to string configuration.  This is used for updating $cfg_max_children
settings in the station.cfg files:  the station.cfg files are appended with the
new settings; and no other files are modified.

=item B<-h>

Hide unused station directories. With this option, stations that are not used
are not generated. The default is to have all stations generated and use the
$cfg_disable parameter in the station.cfg file to tell Stationmaster whether
or not the station is active. With this option, disabled stations are not 
generated in the first place.

=item B<-q> I<directory>

Query string configuration.  Output is put in I<directory>.

=item B<-t> I<tmpdir>

Temporary directory used for constructing configuration files and installation
scripts. Default is './tmp'.

=item B<-d> I<directory>

Change to specified directory before beginning (i.e., location of various 
configuration files.) Default is '.'.

=item B<-s> I<string_cfg_file>

=back

Running without any create, update, query or append arguments will generate all 
configuration files in tmpdir (default = './tmp').  This is quite useful for 
checking the configuration generation before it is executed.

=head1 DESCRIPTION

Stringmaker is a program for creating and managing complete S4PM processing
strings.  This allows the configuration information to be kept in one place
(actually several files).  At the same time, the configuration files that
drive B<s4pm_stringmaker.pl> are fully normalized to eliminate the need for 
maintaining duplicate information.  That is, whenever a configuration value 
can be derived from some other set of values, it is derived instead of set.

The actual creation/update/query operations must be done on the machine
where the strings being updated (or created) reside. Remote machine
configuration using Stringmaker is not available.

=head2 UPDATE

Updating a string will overwrite each and every configuration file in the
string with files representing the currently set values in the 
B<s4pm_stringmaker.pl> configuration files.  Configuration files that do 
not yet exist will be created.  However, if directories do not exist, 
errors will occur (see CREATION below).  If there is any doubt about 
whether all necessary directories exist, using the CREATION argument 
(I<-c>) instead.

Before the update is executed, the script will query the current configuration,
saving the configuration files in ./tmp.[epochal_time], unless the B<-q>
option is specified, in which case it is saved in the argument to B<-q>.
If the directory does not yet exist, it will be created.

=head2 CREATION

The B<-c> argument causes two scripts to be produced, one to create
the whole directory structure, and the second to execute a normal
configuration update.

=head2 APPEND

The B<-a> argument is used for updating the $cfg_max_children of the
stations listed in the B<s4pm_stringmaker_jobs.cfg> file. This simply cats 
on to the end of the station.cfg the new $cfg_max_children value, along
with a comment noting the date and time of the modification.

=head2 QUERY

(Not yet tested).
This function obtains the contents of all of the configuration files,
writing them to the directory specified on the command line.
Directory separators ('/') are replaced with periods to flatten the
structure.

=head1 FILES

Owing to the normalization, the configuration is actually split up into
several files, all of them Perl code segments.  The files are evaluated
in a specific order:  s4pm_stringmaker_global.cfg, s4pm_stringmaker_host.cfg,
s4pm_stringmaker_datatypes.cfg, s4pm_stringmaker_static.cfg,
s4pm_stringmaker_string.cfg, s4pm_stringmaker_algorithm.cfg,
s4pm_stringmaker_jobs.cfg, s4pm_stringmaker_derived.cfg, and
s4pm_stringmaker_extensions.cfg.

=over 4

=item s4pm_stringmaker_global.cfg

Holds information about the global configuration, specifically:
  $user              (e.g., 's4pm' . lc($mode) )
  $global_root       (e.g. '/home/s4pmts2' )
  $stringmaker_root  (e.g. '/home/s4pmts2/stringmaker')

Note that even here, derivation is used whenever possible.

=item s4pm_stringmaker_host.cfg

Hold information applicable to all strings on a particular host machine.
Actual file name is [hostname].cfg, e.g. g0spg10.cfg

=item s4pm_stringmaker_datatypes.cfg

This includes information about I<all> data types (such as maximum size)
for all strings managed by Stringmaker.

=item [algorithm].cfg, [algorithm]_[profile].cfg

There must be one algorithm.cfg file for each algorithm. This file must be 
named: [algorithm_name].cfg. For example: MoPGE01_reproc.cfg. For algorithms
that need to support more than one profile (that is, "flavor"), the file must
be named [algorithm_name]_[profile_name].cfg: For example: MoPGE01_reproc.cfg.
These files are assumed to be packaged with the algorithm and reside in 
the algorithm installation directories.

=item s4pm_stringmaker_static.cfg

Holds relatively static information which is common to all strings, that is,
station and other configuration file parameters that are unchanged from string 
to string.

=item [string].cfg

This holds string-specific information, such as the string_id, the data_source, 
and instance. This file also lists the algorithms to be run along with 
specific versions and profiles of those algorithms. The file should be named 
as the string_id.cfg, though this is not essential. 

=item s4pm_stringmaker_jobs.cfg

This holds the $cfg_max_children settings for the stations in a string.
Not all stations need to be specified in this file. If they are not specified,
default $cfg_max_children settings are assigned. Typically, this file
only lists stations that are likely to need tweaked frequently such as
run_algorithm, allocate_disk, and find_data.

=item s4pm_stringmaker_derived.cfg

This derives many, many configuration variables based on the information
in the above configuration files.  

WARNING:  This is a very ugly file, with "map" statements used profusely
and in unconventional ways.  Looking too closely may cause temporary
Medusa-like petrification.  If this occurs, flush eyes with warm water and
seek immediate medical attention.

=item s4pm_stringmaker_extensions.cfg

This optional file can contain any other configurations. Since it is the last
file, it has at its disposal all variables defined by earlier configuration 
files. This is particularly useful for adding entire new stations to a 
string. In general, string-specific settings should be in the [string].cfg 
file.

=item stringmaker.log

Log file to which much is logged in addition to being sent to STDERR.

=back

=head1 SEE ALSO

L<s4pm_stringmaker_derived.cfg>, L<s4pm_stringmaker_host.cfg>, 
L<s4pm_stringmaker_string.cfg>, L<s4pm_stringmaker_static.cfg>, 
L<s4pm_stringmaker_datatypes.cfg>, L<s4pm_stringmaker_jobs.cfg>,
L<s4pm_stringmaker_algorithm.cfg>

=head1 LIMITATIONS

Assumes that a given algorithm is triggered by only one data type.

Does not store old configurations automatically on update.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2
Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_stringmaker.pl,v 1.13 2007/01/16 17:01:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use S4PM;
use Safe;
use Getopt::Std;
use Sys::Hostname;
use File::Basename;
use File::Copy;
use Cwd;
use vars qw($string_config_path
            $string_config_dir
            @all_datatypes
            @all_input_datatypes
            @all_output_datatypes
            %all_input_uses
            %all_uses
            %all_triggers
            %all_stats_datatypes
            %all_algorithm_versions
            %all_algorithm_execs
            %all_algorithm_stations
            %all_algorithm_profiles
            %all_preselect_data_args
            %all_trigger_block_args
            %all_run_easy_algorithms
            %all_datatype_versions
            %all_datatype_coverages
            %all_ph_algorithms
            %all_check_n_spec_algorithms
            %datatype_max_sizes
            %all_file_accumulation_parms
            %all_production_summary_parms
            %all_custom_find
            %algorithm_inputs
            %algorithm_outputs
            %punt_uses
            $pan_dir
            %output_archives
            %input_archives
            $multiuser_mode
            $die_msg
            $global_dir
);
require 5.6.0;
my $cmd = $0 . join(' ', @ARGV);
my $start_dir = getcwd() or die "Cannot get starting directory";
getopts('acd:l:q:ur:s:t:Thk:');

my $logfile = $opt_l || 'stringmaker.log';
open LOG, ">>$logfile" or die "Cannot open log file $logfile: $!";
my $now = localtime();
tee('=' x 72, "\n$now: $cmd\n");
my $curdir = cwd();

# Parse (most of) the arguments
if ($opt_d) {
    chdir $opt_d or die "Cannot chdir to $opt_d: $!";
}
my $create_cfg = $opt_c;
my $query_cfg = $opt_q;
my $update_cfg = $opt_u;
my $max_children_update = $opt_a;
my $hostname = hostname();

$die_msg = "\n################################################################################\n# Stringmaker FAILURE!!!                                                       #\n################################################################################\n#                                                                              #\n# Reason for failure will be in the several lines above this message.          #\n################################################################################\n\n";

my @stringmaker_files = ();   # For storing backout versions of all config files

# Import configuration files

# Using 'require' rather than 'Safe' makes it easier to support having 
# parameters placed in any file desired (depending upon how global or unique
# the setting is). This would be harder to do with Safe. But...

require 's4pm_stringmaker_global.cfg' or die "Error importing s4pm_stringmaker_global.cfg file";
require "$hostname.cfg" or die "Error importing $hostname.cfg file";
require 's4pm_stringmaker_datatypes.cfg' or die "Error importing s4pm_stringmaker_datatypes.cfg file";
require 's4pm_stringmaker_static.cfg' or die "Error importing s4pm_stringmaker_static.cfg file";
push(@stringmaker_files, "s4pm_stringmaker_global.cfg");
push(@stringmaker_files, "$hostname.cfg");
push(@stringmaker_files, "s4pm_stringmaker_datatypes.cfg");
push(@stringmaker_files, "s4pm_stringmaker_static.cfg");
require $opt_s or die "Cannot exec string-specific config file $opt_s: $!"; 
push(@stringmaker_files, "$opt_s");
unless ( $algorithm_root ) {
    $algorithm_root = "$s4pm_root/$data_source/pge";
}

%all_algorithm_versions = (%algorithm_versions);

foreach my $algorithm ( @run_sorted_algorithms ) {
    my $cfgfile = $algorithm_root . "/" . $algorithm . "/" . $algorithm_versions{$algorithm} . "/" . $algorithm . "_" . $algorithm_profiles{$algorithm} . ".cfg";
    read_algorithm_cfg($cfgfile);
    push(@stringmaker_files, "$cfgfile");
}

# Compute the uses to decrement data types if an algorithm is punted
# (shipped to the archive as a failed PGE tar file). The uses computed here
# is a big improvement over what was done before, namely, hardwiring the
# information in the s4pm_stringmaker_derived.cfg file.

%punt_uses = get_punt_uses(\%algorithm_inputs, \%algorithm_outputs);

# Do some consistency checks

consistency_check();

# Now, remove duplicates in the @all_datatypes array

my %alldata = ();
map { $alldata{$_} = 1 } @all_datatypes;
@all_datatypes = keys %alldata;

# Compute the array of external data types.

@all_external_datatypes = get_external_datatypes();

# Compute all uses. Note that output uses are accounted for in 
# s4pm_stringmaker_derived.cfg

foreach my $alg (keys %all_input_uses) {
    map { $all_uses{$alg}{$_} += $all_input_uses{$alg}{$_} } (keys %{$all_input_uses{$alg}});
}

# Read in remaining configuration files

if (-f 's4pm_stringmaker_jobs.cfg') {
    require 's4pm_stringmaker_jobs.cfg';
    push(@stringmaker_files, "s4pm_stringmaker_jobs.cfg");
}
require 's4pm_stringmaker_derived.cfg';
push(@stringmaker_files, "s4pm_stringmaker_derived.cfg");
if (-f 's4pm_stringmaker_extensions.cfg') {
    require 's4pm_stringmaker_extensions.cfg';
    push(@stringmaker_files, "s4pm_stringmaker_extensions.cfg");
}

# If -h option invoked, prune away disabled stations and unneeded config files

my %pruned_stations = ();
if ( $opt_h ) {
    foreach my $sta ( keys %stations ) {
        if ( $stations{$sta}{'$cfg_disable'} == 1 ) {
            delete $stations{$sta};
            $pruned_stations{$sta} = 1;
        }
    }
}
foreach my $c ( keys %config_files ) {
    foreach my $sta ( keys %pruned_stations ) {
        delete $config_files{$c} if ( $c =~ /$sta/ );
    }
}

# Built S4PM start-up script

my $now = `/bin/date`;
chomp($now);
my $warningmsg = "\n\n# This file was created by s4pm_stringmaker.pl on $now\n# DO NOT EDIT THIS FILE MANUALLY!\n\n";
my $ds_label = ($data_source_longname) ? $data_source_longname : $data_source;
my $stations_list = get_stations();
if ( $multiuser_mode ) {

### First, verify that this user can run Stringmaker

    my $gids = $);
    my @groups = split(/\s/, $gids);
    my $current_group = @groups[0];    # We assume that current group is 1st
    my (undef, undef, $s4pm_gid, undef) = getgrnam($multiuser_mode);
    unless ( $current_group == $s4pm_gid ) {
        print "\nMulti-user mode group is set to $multiuser_mode with group ID $s4pm_gid, yet you are\nrunning this with group ID $current_group. They do not match. Therefore, you cannot run\nStringmaker. ACTION: Switch to the group set by the variable \$multiuser_mode first and then\nrerun.\n\n";
        my $die_msg = "\n################################################################################\n# Stringmaker FAILURE!!!                                                       #\n################################################################################\n#                                                                              #\n# Reason for failure will be in the several lines above this message.          #\n################################################################################\n\n";

        die $die_msg;
    }
    my $Copt = ( $use_classic_stationmaster ) ? "-C" : "";
    $config_files{'s4pm_start.ksh'} = "#!/bin/ksh" . $warningmsg . "export PATH=$bindir:$cfgdir:$ENV{'PATH'}\n\numask 0002 && $bindir/tkstat.pl -F $Copt -c ./tkstat.cfg -t 'S4PM Monitor: " . $ds_label . " " . ucfirst($instance) . " on $host' ";
} else {
    $config_files{'s4pm_start.ksh'} = "#!/bin/ksh" . $warningmsg . "$bindir/tkstat.pl -F $Copt -c ./tkstat.cfg -t 'S4PM Monitor: " . $ds_label . " " . ucfirst($instance) . " on $host' ";
}

if ( @privileged_users or scalar(@privileged_users) > 0 ) {
    $config_files{'.acl'}{'%cfg_authorize'} = {
        'KILLALL' => [@privileged_users],
    };
}
$config_files{'s4pm_start.ksh'} .= "$stations_list\n\n";

# Setup temporary directory

my $tmpdir = $opt_f || $opt_t || (getcwd() . '/tmp');
if (-d $tmpdir) {
    clean_dir($tmpdir);
} else {
    make_dir($tmpdir, "0755");
}

my $backoutfile = mk_backout_tar($tmpdir, @stringmaker_files);

# Finish putting config files together

map {$config_files{"$_/station.cfg"} = $stations{$_}} keys %stations;

if ( exists $input_archives{'ecs'} and ! $input_symlink_root ) {
    $config_files{'request_data/ACQParmfile'} .= "FTPPASSWORD=" . get_ftp_passwd($host) . "\n";
}

### If we're using the INPUT disk pool only for symbolic links to the 
### ftp pull disk on Datapool, we override the the max sizes set for input
### files and make them all just 1 byte.

if ( $input_symlink_root ) {
    foreach my $dt ( keys %datatype_max_sizes ) {
        if ( $datatype_pool_map{$dt} eq "INPUT" ) {
            $datatype_max_sizes{$dt} = 1;
        }
    }
}

# For on-demand, we need to modify the disk pools

if ( $on_demand ) {
    my ($rh_new_map, $rh_new_pool, $rh_new_capacity) = 
        pool_ondemand_adjustment(\%datatype_max_sizes, \%datatype_pool_map, \%datatype_pool);
    %datatype_pool_map = %{$rh_new_map};
    %datatype_pool     = %{$rh_new_pool};
}

# Test whether which method to use for sizing the disk pools and then execute
# the appropriate subroutine. 

# Note that generate_pool_capacities() should be considered VERY EXPERIMENTAL
# for now!

if ( exists $autopool_parms{'max_size'} and
     exists $autopool_parms{'input_weight'} and
     exists $autopool_parms{'small_file_weight'} and
     exists $autopool_parms{'small_file_thresh'} ) {
    ($config_files{"allocate_disk/DO.UPDATE_POOLS.wo"}, $config_files{'allocate_disk/s4pm_allocate_disk.cfg'}) =
        generate_pool_capacities(\%datatype_max_sizes, \%all_datatype_coverages, \%datatype_uses, \%datatype_pool_map, \%proxy_esdt_map, \%datatype_pool, \%autopool_parms);
} else {
    ($config_files{"allocate_disk/DO.UPDATE_POOLS.wo"}, $config_files{'allocate_disk/s4pm_allocate_disk.cfg'}) =
        alloc_disk_setup(\%datatype_max_sizes, \%datatype_pool_map, \%datatype_pool, \%pool_capacity, \%proxy_esdt_map);
}

# Generate S4PM version file

version();

# Seed files for repeat_* stations

my @seed_files;
foreach my $sta(keys %stations) {
    if (exists $stations{$sta}{'seed_files'}) {
        my $path = $stations{'pathname'} || "$station_root/$sta";
        push @seed_files, map {sprintf "$path/DO.$_.*.wo"} @{$stations{$sta}{'seed_files'}};
    }
}

# Subdirectories for creation

my @subdirs;
foreach my $file(keys %config_files) {
    # Identify directories with trailing slash
    next if ($file !~ m#/$#);
    if ($file =~ m#^/#) {
        push @subdirs, $file;
    }
    else {
        my @path = split('/', $file);
        # If it is under a station, we better find the real pathname first
        # Otherwise, the directory will get made before the directory symlink,
        # causing a duplicate directory tree for that station
        if (exists $stations{$path[0]}) {
            $path[0] = $stations{$path[0]}{'pathname'} || "$station_root/$path[0]";
            push @subdirs, join('/', @path);
        }
        else {
            push @subdirs, "$station_root/$file";
        }
    }
}

my $script_file;

if ($create_cfg) {
    my $create_script = make_creation_script(\%stations, \%datatype_pool, 
        \@pge_stations, \@seed_files, \@subdirs, \@sort_pges, \%pge_version);
    $script_file = "$tmpdir/create_$string_id.ksh";
    tee("Writing creation script to $script_file...\n");
    open TMP, ">$script_file" or die "Cannot open creation script file $script_file: $!";
    print TMP $create_script;
    close TMP;
    $config_files{'station.list'} = join("\n", grep(!/(configurator|sub_notify|insert_datapool)/, keys %stations), '');
}

# Prepare to save current configuration

if ($update_cfg) {
    $query_cfg ||= "tmp." . time();
}
if ($query_cfg) {
    make_dir($query_cfg, "0750");
}

# Get current configuration

if ($query_cfg) {
    chdir $tmpdir or die "Cannot chdir to $tmpdir: $!";
    my $xfer_script = xfer_config('get', %config_files);
    my $cmd;
    $cmd = "/bin/ksh $xfer_script && cd .. && mv $tmpdir/* $query_cfg";
    run_command($cmd);
}

# Create new config files in tmpdir if we are updating or creating, or running
# with no action arguments whatsoever

if ($update_cfg || $create_cfg || !$query_cfg) {
    my $dir = $opt_d || '.';
    chdir $tmpdir or die "Cannot chdir to $tmpdir: $!"; 

    # Don't need to write out all the files if we're just updating max_children

    unless ($max_children_update) {
        foreach $cfg (sort keys %config_files) {
            my $file = print_config($cfg, $config_files{$cfg}, $tmpdir);
            if ( $file and $cfg =~ /ACQParmfile/ and ! $multiuser_mode ) {
                change_mode("0400", $file);
            }
        }
        change_mode("0755", "s4pm_start.ksh");
        change_mode("0755", ".acl");
    }
    if ($create_cfg || $update_cfg || $max_children_update) {
        my $xfer_type = ($max_children_update) ? 'cat' : 'xfer';
        my $xfer_script = xfer_config($xfer_type, %config_files);
        my $cmd;
        my @scripts;
        push @scripts, basename($script_file) if ($create_cfg);
        push @scripts, "$xfer_script";
        my $install_script = '/bin/ksh ' . join(" && /bin/ksh ", @scripts);
        $cmd = $install_script;
        run_command($cmd);
    }
}

# Special permissions handling for DME

if ( $dme ) {
    change_mode("0777", "$global_dir/logs");
    change_mode("0777", "$global_dir/request_data");
    change_mode("0777", "$global_dir/auto_request");
}

# Special permissions for PAN directory if S4PA

change_mode("0773", "$pan_dir") if ( exists $output_archives{'s4pa'} );

# Special permissions in multi-user mode for this config file since it can be 
# updated after via GUI creation

if ( $multiuser_mode ) {
    change_mode("0775", "$station_root/ARCHIVE");
    change_mode("0775", "$station_root/ARCHIVE/logs");
    change_mode("0775", "$station_root/DATA") unless ( $data_root_set );
    change_mode("0644", "$station_root/allocate_disk/DO.UPDATE_POOLS.wo");
    change_mode("0775", "$global_dir");
    my @archive_dirs = ('logs/export', 'logs/receive_pan', 'logs/sweep_data',
        'logs/track_requests', 'ORDERS', 'PDR', 'REQUESTS');
    foreach my $dir ( @archive_dirs ) {
        change_mode("0775", "$station_root/ARCHIVE/$dir");
    }
}

# Update the Configurator configuration file

update_configurator();

close LOG;
chdir $curdir;
change_mode("0664", $logfile) if ( $multiuser_mode );
make_trouble_ticket($string_id) if ( $opt_T );

print "################################################################################\n# Stringmaker completed SUCCESSFULLY!!!                                        #\n#                                                                              #\n# A backup of Stringmaker configuration files has been saved in:               #\n# $backoutfile                           #\n################################################################################\n\n";

exit(0);

sub alloc_disk_setup {
    my ($rh_maxsize, $rh_pool_map, $rh_pool, $rh_pool_capacity, $rh_proxy_esdt) = @_;

    my %pool_size;
    
    my $allocdisk_cfg = format_hash('%datatype_maxsize', $rh_maxsize) .
        format_hash('%proxy_esdt_map', $rh_proxy_esdt) .
        format_hash('%datatype_pool_map', $rh_pool_map) . 
        format_hash('%datatype_pool', $rh_pool);
    foreach my $dt ( keys %$rh_pool_map ) {
        my $pool = $datatype_pool{$rh_pool_map->{$dt}};
        if (! $pool) {
            tee("No datatype pool for $dt!\n");
            exit(11);
        }
        unless ( exists $rh_maxsize->{$dt} and $rh_maxsize->{$dt} > 0 ) {
            S4P::perish(30, "alloc_disk_setup(): No maximum size set for data type $dt in \%all_datatype_max_sizes in s4pm_stringmaker_datatypes.cfg file. ACTION: Add this data type to \%all_datatype_max_sizes in s4pm_stringmaker_datatypes.cfg.");
        }
        unless ( exists $rh_pool_capacity->{$dt} ) {
            S4P::perish(30, "alloc_disk_setup(): No pool capacity set for data type $dt in \%pool_capacity in $opt_s file. ACTION: Add this data type to \%pool_capacity in $opt_s.");
        }

####### For on-demand, there are only two disk pools, INPUT and OUTPUT

        if ( $on_demand ) {
            my $rootdir = dirname( $datatype_pool{$rh_pool_map->{$dt}} );
            $pool = ($rh_pool_map->{$dt} eq "INPUT") ?
                "$rootdir/INPUT" : "$rootdir/OUTPUT";
        }

        $pool_size{$pool} += $rh_maxsize->{$dt} * $rh_pool_capacity->{$dt};
    }

    my $total_size;
    map {$total_size += $pool_size{$_}} keys %pool_size;
    my $allocdisk_pool = format_hash('%pool_size', \%pool_size);
    $allocdisk_pool .= "# Total size:  " . $total_size / (1024 * 1024 * 1024) . " GB\n";
    return ($allocdisk_pool, $allocdisk_cfg);
}

sub clean_dir {
    my $dir = shift;
    opendir DIR, $dir or die "Cannot opendir $dir: $!";
    my @files = grep !/^\./, readdir DIR;
    map {unlink "$dir/$_" or die "Cannot unlink $dir/$_: $!"} @files;
    closedir DIR;
}
sub make_trouble_ticket {
    my $string_id = shift;

    print <<EOF;

Stringmaker has completed successfully. 

I will now help you submit a Trouble Ticket to the Master Change Log. You will
need your Remedy account userid and password for logging onto g0mss10.

If this part fails, remember that you do NOT have to run Stringmaker again.
You will, however, have to manually submit and close a Trouble Ticket.

EOF

    print "\nEnter Remedy user name (it must already exist): ";
    my $user = <STDIN>;
    chomp $user;
    print "\nEnter the problem you are addressing (be brief): ";
    my $prob = <STDIN>;
    chomp $prob;
    my $short_descrip = "MCL: Stringmaker run on $string_id in $mode";

    my $tt_open_parms =<<EOF;
$user
L
A
$user
L
$short_descrip
$prob
.
Problem requires that Stringmaker be run on this S4PM string.
.
$user
bvollmer

EOF

    my $tt_close_parms =<<EOF;
C
Implemented
configuration
Stringmaker was run.
.
S4PM

EOF

    print STDERR "\nSubmitting and closing Trouble Ticket to the Master Change Log on your behalf.\nYou will be asked for your password twice, so don't panic.\n";
    
    my $status = S4PM::submit_trouble_ticket($mode, $user, "g0mss10", $tt_open_parms, $tt_close_parms);

    if ( $status ) {
        infobox("ERROR!", "Submitting and closing a Trouble Ticket on your behalf has failed. I'm afraid that you will have to do so manually.");
    }

}
sub xfer_config {
    my ($getput, %cfg) = @_;
    my $xfer_script = "xfer_script.$$";
    $xfer_script .= '.ksh' if ($getput eq 'xfer');
    open XFER, ">$xfer_script" or die "Cannot open xfer script $xfer_script: $!";
    print XFER "echo executing update script...\n";
    foreach (keys %cfg) {
        next if ($_ =~ m#/$#);
        my ($filename, $local) = ($_, $_);
        if (m#/#) {
            $filename = basename($_);
            $local = basename(dirname($_)) . '.' . $filename;
        }
        my $dir = $cfg{dirname($_) . "/station.cfg"}{'pathname'};
        $dir ||= "$station_root/" . dirname($_);
        my $remote = "$dir/$filename";
        if ($getput eq 'xfer') {
            print XFER "mv -f $local $remote || exit 2\n";
        } elsif ($getput eq 'get') {
            print XFER "cp $remote $local\n";
        } elsif ($getput eq 'cat' && $filename eq 'station.cfg') {
            $max_children = $cfg{$_}{'max_children_override'};

########### We test if defined since we want it to accept zero as a setting.
########### Setting $max_children to zero causes stationmaster.pl to run the
########### job without forking a child process.

            if ( defined $max_children ) {
                if ( $multiuser_mode ) {
                    print XFER "chmod 2660 $remote && ";
                } else {
                    print XFER "chmod 0640 $remote && ";
                }
                print XFER "echo '#' `date` >> $remote && ";
                printf XFER "echo '%s = %d;' >> %s && ",
                    qw($cfg_max_children), $max_children, $remote;
                if ( $multiuser_mode ) {
                    print XFER "chmod 2440 $remote || exit 2\n";
                } else {
                    print XFER "chmod 0440 $remote || exit 2\n";
                }
            }
        }
    }
    close XFER;
    return $xfer_script;
}
sub get_cfg_sequence {
    my $dir = shift;
    my @cfgs = sort glob("$dir/cfg_????");
    return 1 if (! @cfgs);
    my $top = basename(pop @cfgs);
    $top =~ s/^cfg_0*//;
    return ($top + 1);
}
    
# get_ftp_passwd($host) - retrieve FTP password from .netrc file

sub get_ftp_passwd {
    my ($host) = @_;
    my ($login, $password) = S4P::get_ftp_login($host);
    return $password;
}

# format_config($cfg, \%config) - return formatted string
#   for hash variable $cfg

sub format_config {
    my ($cfg, $rh_config) = @_;
    # Could be just a verbatim string; if so return it
    return $rh_config if (! ref $rh_config);
    my %config = %$rh_config;
    my $fmt_s;
    foreach my $var(sort grep /^[\$@%]/, keys %config) {
        my $anonymous = (($var =~ /^\$/) && (ref($config{$var}) =~ /(HASH|ARRAY)/));
        $s = format_var($config{$var}, $anonymous);
        $fmt_s .= "$var = $s;\n\n";
    }
    $fmt_s .= "$config{'verbatim'}\n" if $config{'verbatim'};
    return ($fmt_s);
}

sub format_hash {
    my ($var, $rh) = @_;
    my $s = "$var =" . format_var($rh) . ";\n";
}
    
sub format_var {
    my ($value, $anonymous) = @_;
    my $s;
    if (ref $value eq "HASH") {
        my @entries;
        foreach my $par(sort keys %$value) {
            my ($p) = quote($par);
            $v =  (ref $value->{$par}) ? format_var($value->{$par}, 1) : quote($value->{$par});
            push @entries, "$p => $v";
        }
        my @paren = ($anonymous) ? ("{\n\t",'}') : ("(\n\t","\n)");
        $s .= $paren[0] . join(",\n\t", @entries) . $paren[1];
    }
    elsif (ref $value eq "ARRAY") {
        my @paren = ($anonymous) ? ("[\n\t","\n]") : ("(\n\t","\n)");
        $s .= $paren[0] . join(",\n\t", quote(@$value)) . $paren[1];
    }
    else {
        $s .= quote($value);
    }
    return $s;
}

sub make_creation_script {
    my ($rh_stations, $rh_data_dirs, $ra_pge_stations, $ra_seed_files, $ra_dirs, $ra_sort_pges, $rh_pge_version) = @_;
    my @script = ('#!/bin/ksh');
    push @script, "echo executing string creation script...";

    # Station directories
    my @dirs = map {$rh_stations->{$_}{'pathname'} || "$station_root/$_"} 
        keys %$rh_stations;

    # Data pool directories
    push @dirs, values %$rh_data_dirs;

    # Miscellaneous directories
    push @dirs, @$ra_dirs;

    # Begin assembling script;
    my $mkmode = ( $multiuser_mode ) ? "2775" : "0775";
    push @script, "if [ ! -d $station_root ] ; then mkdir -m $mkmode -p $station_root || exit 1 ; fi";
    push @script, map {"if [ ! -d $_ ] ; then mkdir -m $mkmode -p $_ || exit 1 ; fi"} @dirs;

    my @symlinks;
    foreach my $sta(keys %$rh_stations) {
        my %sta = %{$rh_stations->{$sta}};
        my $dir = $sta{'pathname'} || "$station_root/$sta";
        $symlinks{"$station_root/$sta"} = $sta{'pathname'} if ($sta{'symlink'});
        
        foreach my $link(@{$sta{'exec_symlinks'}}) {
            $symlinks{"$dir/$link"} = "$bindir/$link";
        }
        foreach my $link(keys %{$rh_stations->{$sta}{'misc_symlinks'}}) {
            $symlinks{"$dir/$link"} = $sta{'misc_symlinks'}{$link};
        }
    }

    # Miscellaneous symlinks
    # Only recreate links if the target has changed.
    foreach my $key ( keys %symlinks ) {
        if ( -l $key ) {
            unless ( readlink($key) eq $symlinks{$key} ) {
                push @script, "if [ -L $key ] ; then /bin/rm $key ; fi ; ln -s $symlinks{$key} $key || exit 2";
           }
        } else {
            push @script, "if [ -L $key ] ; then /bin/rm $key ; fi ; ln -s $symlinks{$key} $key || exit 2";
        }
    }

    # Seed files for repeat stations
    foreach my $file(@$ra_seed_files) {
        my $target = $file;
        $target =~ tr/*/0/;   # Modification en passant
        push @script, "if (! ls -C1 $file > /dev/null 2>&1) ; then touch $target || exit 3 ; fi";
    }

    push @script, "exit 0";
    my $script = join("\n", @script) . "\n";
    tee('-' x 60, "\nCreation script:\n$script");
    return $script;
}

sub print_config { 
    my ($cfg, $rh_config, $tmpdir) = @_;
    if (! $rh_config) {
        tee("$cfg has no content.\n");
        return;
    }
    print LOG "Contents of $cfg...\n";
    print STDERR "Contents of $cfg...\n";
    my $file = $cfg;
    # Convert directory to period to flatten directory structure
    $file =~ s#/#.#;
    # Test to see if this is a Perl config file
    my $perl_cfg = ($file =~ /\.cfg$/ && $file !~ /s4pm_select_data\w+.cfg/);

    # Open file
    open OUT, ">$file" or die "Cannot open output file $file: $!";
    # Print header if Perl configuration file
    if ($perl_cfg) {
        my $now = `/bin/date`;
        chomp($now);
        print OUT "\n# This file was created by s4pm_stringmaker.pl on $now\n";
        print OUT "# DO NOT EDIT THIS FILE MANUALLY!\n\n";
    }
    print OUT format_config($cfg, $rh_config);

### Set dummy variable at bottom of every Perl config file so that it always
### evals to true

    if ( $file =~ /station.cfg$/ ) {
        print OUT "\n\$cfg_foobar = 1;\n";
    } elsif ( $file =~ /\.cfg$/ and $file !~ /tkstat.cfg$/ ) {
        print OUT "\n1;\n";
    }
    
    close OUT;
    change_mode("0644", $file);
    run_command("perl -c $file") if ($perl_cfg);
    return $file;
}

sub quote {

    if (wantarray) {
        return map { ($_ =~ /[^\d.+-]/) ? sprintf("'%s'",$_) : $_} @_;
    }
    else {
        my $qtype = ($_[0] =~ /'/) ? '"' : "'";
        return ($_[0] =~ /[^\d.+-]/) ? sprintf("%s%s%s",$qtype, $_[0], $qtype) : $_[0];
    }
}
sub run_command {
    my $cmd = shift;
    tee("Executing command $cmd...\n");
    my $output = `$cmd`;
    my $rc = $?;
    tee($output);
    tee("Exit = $rc\n");
    if ( $rc ) {
        die $die_msg;
    }
}

sub tee {
    my (@strings) = @_;
    print LOG @strings;
    print STDERR @strings;
}

sub usage {
    print STDERR << "EOF";
Usage: s4pm_stringmaker.pl -s string_config [-c] [-d] [-f] [-p] [-t tmpdir]
-s string_config    config file for that string
-d dir              change to specified directory before beginning
-c                  create a new string
-u                  update current configuration
-a                  append cfg_max_children values to current station.cfg files
-q dir              obtain configuration, writing files to dir
-t tmpdir           local temporary directory
EOF
}

sub version {
    my $verstr = `/bin/grep "@@@" $station_root/track_data/s4pm_track_data.pl`;
    if ( $verstr =~ /Version\s+(.*)$/ ) {
        $verstr = "S4PM v$1";
    } else {
        $verstr = "S4PM vUNKNOWN";
    }
    unlink("$station_root/VERSION") if ( -e "$station_root/VERSION" );
    if ( -e $station_root ) {
        my $cmd = "echo $verstr > $station_root/VERSION";
        my ($errstr, $rc) = S4P::exec_system("$cmd");
        if ($rc) {
            print "$errstr\n";
            die $die_msg;
        }
        change_mode("0644", "$station_root/VERSION");
    }
}

sub read_algorithm_cfg {

    my $cfg = shift;

    my $algorithm = basename($cfg);
    $algorithm =~ s/\.cfg$//;

### The following double reversal makes it easier to deal with regex greediness
### since some algorithm names have underscore characters embedded in them

    $algorithm = reverse $algorithm;
    $algorithm =~ s/^.*?_//;
    $algorithm = reverse $algorithm;


    my $compartment = new Safe 'CFG';

    $compartment->share('$algorithm_name',          '$algorithm_exec', 
                        '$make_ph',                 '%inputs', 
                        '%outputs',                 '%input_uses',
                        '@stats_datatypes',         '$run_easy',
                        '$stats_index_datatype',    '$algorithm_station',   
                        '$preselect_data_args',     '$trigger_block_args',   
                        '$check_n_spec_algorithm',  '%file_accumulation_parms',
                        '%production_summary_parms','%custom_find',
    );

    $CFG::algorithm_name = undef;
    $CFG::algorithm_exec = undef;
    $CFG::algorithm_station = undef;
    $CFG::make_ph = undef;
    $CFG::run_easy = undef;
    $CFG::stats_index_datatype = undef;
    $CFG::preselect_data_args = undef;
    $CFG::trigger_block_args = undef;
    $CFG::check_n_spec_algorithm = undef;
    @CFG::stats_datatypes = undef;
    %CFG::inputs = undef;
    %CFG::outputs = undef;
    %CFG::input_uses = undef;
    %CFG::file_accumulation_parms = undef;
    %CFG::production_summary_parms = undef;
    %CFG::custom_find = undef;

    $compartment->rdo($cfg) or die "Cannot import config file $cfg: $!\n";

    foreach my $input ( keys %CFG::inputs ) {
        unless ( $CFG::inputs{$input}{'data_version'} eq $all_datatype_versions{ $CFG::inputs{$input}{'data_type'} } ) {
            S4P::perish(30, "read_algorithm_cfg(): Conflict between $cfg and s4pm_stringmaker_datatypes.cfg for data type $CFG::inputs{$input}{'data_type'}. Algorithm configuration file has version " . $CFG::inputs{$input}{'data_version'} . " while the datatypes configuration file has " . $all_datatype_versions{ $CFG::inputs{$input}{'data_type'} } . ". You need to resolve this.");
        }
        $all_datatypes{ $CFG::inputs{$input}{'data_type'} } = 1;
        push(@all_input_datatypes, $CFG::inputs{$input}{'data_type'});
        push( @{$in{$algorithm}}, $CFG::inputs{$input}{'data_type'});
        $all_datatype_coverages{ $CFG::inputs{$input}{'data_type'} } = $CFG::inputs{$input}{'coverage'};
    }
    foreach my $output ( keys %CFG::outputs ) {
        unless ( $CFG::outputs{$output}{'data_version'} eq $all_datatype_versions{ $CFG::outputs{$output}{'data_type'} } ) {
            S4P::perish(30, "read_algorithm_cfg(): Conflict between $cfg and s4pm_stringmaker_datatypes.cfg. Version " . $CFG::outputs{$output}{'data_version'} . " of data type " . $CFG::outputs{$output}{'data_type'} . " is not included in the Stringmaker s4pm_stringmaker_datatypes.cfg configuration file");
        }
        $all_datatypes{ $CFG::outputs{$output}{'data_type'} } = 1;
        push(@all_output_datatypes, $CFG::outputs{$output}{'data_type'});
        push( @{$out{$algorithm}}, $CFG::outputs{$output}{'data_type'});
        $all_datatype_coverages{ $CFG::outputs{$output}{'data_type'} } = $CFG::outputs{$output}{'coverage'};
    }

### Below we build hashes for algorithm inputs and outputs where the hash
### keys are the algorithm names and the hash values are lists of data types.
### There's a bit of extra work to remove duplications from these data type
### lists.

    foreach my $i ( keys %in ) {
        my %itmp = ();
        my @ar = @{$in{$i}};
        map { $itmp{$_} = 1 } @ar;
        my @iar = keys %itmp;
        $algorithm_inputs{$i} = [@iar];
    }
    foreach my $o ( keys %out ) {
        my %otmp = ();
        my @ar = @{$out{$o}};
        map { $otmp{$_} = 1 } @ar;
        my @oar = keys %otmp;
        $algorithm_outputs{$o} = [@oar];
    }

    push @all_datatypes, keys %all_datatypes;
    push @all_datatypes, 'FAILPGE';	# Always needed
    $all_datatype_coverages{'FAILPGE'} = 1; # Avoids divide by zero error later

    foreach my $dt ( keys %CFG::input_uses ) {
        unless ( $CFG::input_uses{$dt} ) {
            S4P::perish(30, "read_algorithm_cfg(): Invalid input uses set for data type $dt in $cfg");
        }
        $all_input_uses{$algorithm}{$dt} = $CFG::input_uses{$dt};
    }

    $all_stats_datatypes{$algorithm} = @CFG::stats_datatypes;
    $all_stats_indices{$algorithm} = $CFG::stats_index_datatype or $CFG::stats_datatypes[0];

    $all_algorithm_execs{$algorithm} = $CFG::algorithm_exec;

    if ( $CFG::algorithm_station ) {
        $all_algorithm_stations{$algorithm} = $CFG::algorithm_station;
    } else {
        $all_algorithm_stations{$algorithm} = "run_algorithm";
    }

    if ( $CFG::run_easy ) {
        $all_run_easy_algorithms{$algorithm} = 1;
    }

    if ( $CFG::check_n_spec_algorithm ) {
        $all_check_n_spec_algorithms{$algorithm} = 1;
    } else {
        $all_check_n_spec_algorithms{$algorithm} = 0;
    }

    if ( $CFG::make_ph ) {
        $all_ph_algorithms{$algorithm} = 1;
    }

    if ( $CFG::preselect_data_args ) {
        $all_preselect_data_args{$algorithm} = $CFG::preselect_data_args;
    }

    if ( $CFG::trigger_block_args ) {
        $all_trigger_block_args{$algorithm} = $CFG::trigger_block_args;
    }

    my $trigger = get_trigger(%inputs);
    unless ( $trigger ) {
        S4P::perish(30, "read_algorithm_cfg(): No input data type has a need of 'TRIG' in $cfg");
    }
    push( @{$all_triggers{$trigger}}, $algorithm );

### Handling of optional %file_accumulation_parms parameter:

    if ( exists $CFG::file_accumulation_parms{'window_width'} or 
         exists $CFG::file_accumulation_parms{'window_boundary'} or
         exists $CFG::file_accumulation_parms{'polling_interval'} or
         exists $CFG::file_accumulation_parms{'file_threshold'} or
         exists $CFG::file_accumulation_parms{'timer'} ) {
        unless ( exists $CFG::file_accumulation_parms{'window_width'} and
                 exists $CFG::file_accumulation_parms{'window_boundary'} and
                 exists $CFG::file_accumulation_parms{'polling_interval'} and
                 exists $CFG::file_accumulation_parms{'file_threshold'} and
                 exists $CFG::file_accumulation_parms{'timer'} ) {
            S4P::perish(30, "read_algorithm_cfg(): Missing parameters in the \%file_accumulation_parms hash you specified in $cfg. If you include this hash, you need to set all values: 'window_width', 'window_boundary', 'polling_interval', 'file_threshold', and 'timer'. One or more values are missing.");
        }
        $all_file_accumulation_parms{$algorithm}{'window_width'} = $CFG::file_accumulation_parms{'window_width'};
        $all_file_accumulation_parms{$algorithm}{'window_boundary'} = $CFG::file_accumulation_parms{'window_boundary'};
        $all_file_accumulation_parms{$algorithm}{'polling_interval'} = $CFG::file_accumulation_parms{'polling_interval'};
        $all_file_accumulation_parms{$algorithm}{'file_threshold'} = $CFG::file_accumulation_parms{'file_threshold'};
        $all_file_accumulation_parms{$algorithm}{'timer'} = $CFG::file_accumulation_parms{'timer'};
    }

### Handling of optional %custom_find parameter:

if ( scalar(keys %CFG::custom_find) > 0 ) {
    foreach my $lun ( keys %CFG::custom_find ) {
        $all_custom_find{$algorithm}{$lun} = $CFG::custom_find{$lun};
    }
} 
    

### Handling of optional %production_summary_parms parameter:

    if ( exists $CFG::production_summary_parms{'runlog_file'} or 
         exists $CFG::production_summary_parms{'logstatus_file'} or
         exists $CFG::production_summary_parms{'pcf_file'} ) {
        unless ( exists $CFG::production_summary_parms{'runlog_file'} and
                 exists $CFG::production_summary_parms{'logstatus_file'} and
                 exists $CFG::production_summary_parms{'pcf_file'} ) {
            S4P::logger("WARNING", "read_algorithm_cfg(): Missing parameters in the \%production_summary_parms hash you specified in $cfg. I will assume that you did this on purpose.");
        }
        $all_production_summary_parms{$algorithm}{'runlog_file'} = $CFG::production_summary_parms{'runlog_file'} if ( exists $CFG::production_summary_parms{'runlog_file'} );
        $all_production_summary_parms{$algorithm}{'logstatus_file'} = $CFG::production_summary_parms{'logstatus_file'} if ( exists $CFG::production_summary_parms{'logstatus_file'} );
        $all_production_summary_parms{$algorithm}{'pcf_file'} = $CFG::production_summary_parms{'pcf_file'} if ( exists $CFG::production_summary_parms{'pcf_file'} );
        if ( exists $CFG::production_summary_parms{'lun'} ) {
            $all_production_summary_parms{$algorithm}{'lun'} = $CFG::production_summary_parms{'lun'};
        }
        if ( exists $CFG::production_summary_parms{'data_version'} ) {
            $all_production_summary_parms{$algorithm}{'data_version'} = $CFG::production_summary_parms{'data_version'};
            $all_datatype_versions{$ps_datatype} = $CFG::production_summary_parms{'data_version'};
        } else {
            $all_production_summary_parms{$algorithm}{'data_version'} = "001";
            $all_datatype_versions{$ps_datatype} = "001";
        }

####### Data type name for the production summary 

        my $ps_datatype;
        if ( exists $CFG::production_summary_parms{'data_type'} ) {
            $ps_datatype = $CFG::production_summary_parms{'data_type'};
        } else {
            $ps_datatype = $algorithm . "_PS";
        }

####### Now, we want to treat this production summary data type like a real
####### output product:

        $all_production_summary_parms{$algorithm}{'data_type'} = $ps_datatype;
        $datatype_max_sizes{$ps_datatype} = 1000;
        $all_datatype_max_sizes{$ps_datatype} = 1000;
        $pool_capacity{$ps_datatype} = 100000;
        $datatype_pool_map{$ps_datatype} = $ps_datatype;
        $all_datatypes{$ps_datatype} = 1;
        $all_datatype_versions{$ps_datatype} = $CFG::production_summary_parms{'data_version'};
        push(@all_output_datatypes, $ps_datatype);
        push( @{$out{$algorithm}}, $ps_datatype);
        $all_datatype_coverages{$ps_datatype} = 1;
    }

}

sub get_trigger {

    foreach my $tag ( keys %CFG::inputs ) {
        if ( $CFG::inputs{$tag}{'need'} eq "TRIG" ) {
            return $CFG::inputs{$tag}{'data_type'};
        }
    }

    return undef;
}

sub consistency_check {

### If @datapool_insert_datatypes is non empty, then $datapool_staging_dir
### better be specified too.

    if ( defined @datapool_insert_datatypes and
        scalar(@datapool_insert_datatypes) > 0 ) {
        unless ( $datapool_staging_dir ) {
            S4P::perish(30, "main: You specified data types to be inserted into datapool via \@datapool_insert_datatypes, but did not set the staging machine and directory with \$datapool_staging_dir. ACTION: Set \$datapool_staging_dir.");
        }
    }

### If $dme is set, then $sub_request_email and $pickup_dir need to be
### set too.

    if ( $dme ) {
        unless ( $sub_request_email ) {
            S4P::perish(30, "main: You specified this string as a Data Mining string with \$dme, but did not set the the email address for subscription requests with \$sub_request_email. ACTION: Set \$sub_request_email.");
        }
        unless ( $pickup_dir ) {
            S4P::perish(30, "main: You specified this string as a Data Mining string with \$dme, but did not set the pickup machine and directory with \$pickup_dir. ACTION: Set \$pickup_dir.");
        }
    }

### If $data_source_polling is set, then $data_source_polling_dir needs to
### be set as well.

    if ( $data_source_polling ) {
        unless ( $data_source_polling_dir ) {
            S4P::perish(30, "main: You configured this string for polling data from a disk data source (e.g. datapool), but did not specify the polling directory root with \$data_source_polling_dir. ACTION: Set \$data_source_polling_dir.");
        }
    }

}

sub get_external_datatypes {

# Use @all_input_datatypes and @all_output_datatypes to determine which
# data types must be external.

    my %in  = ();
    my %out = ();
    my @all_external_datatypes = ();

    map { $in{$_}  = 1 } @all_input_datatypes;
    map { $out{$_} = 1 } @all_output_datatypes;

    foreach my $key ( keys %in ) {
        unless ( exists $out{$key} ) {
            push(@all_external_datatypes, $key);
        }
    }

    return @all_external_datatypes;
}

sub update_configurator {

    my $now = localtime();

### First see if this file already exists or not. If not, create it. If it
### does, read it in.

    my $cfg    = "$stringmaker_root/s4pm_configurator.cfg";
    my $cfgold = $cfg . ".backup";
    my $tmp    = $cfg . ".tmp";
    
    my %cfg_string_info = ();
    my $cfg_checkin;
    my $cfg_checkout;

    if ( -e $cfg ) {

        my $compartment = new Safe 'CFG';
        $compartment->share('%cfg_string_info', '$cfg_checkin', '$cfg_checkout');
        $compartment->rdo($cfg) or S4P::perish(30, "update_configurator(): Cannot import config file $cfg: $!");
        %cfg_string_info = (%CFG::cfg_string_info);
        $cfg_checkin = $CFG::cfg_checkin;
        $cfg_checkout = $CFG::cfg_checkout;

        copy($cfg, $cfgold) or S4P::perish(30, "update_configurator(): Failed to copy $cfg to $cfgold: $!");
        unlink($cfg) or S4P::perish(30, "update_configurator(): Failed to unlink file $cfg: $!");

    }

    $cfg_string_info{$string_id}{'root_dir'} = $station_root;
    $cfg_string_info{$string_id}{'shortname'} = $data_source;
    $cfg_string_info{$string_id}{'longname'} = $data_source_longname;
    $cfg_string_info{$string_id}{'machine'} = $host;

    open(FLE, ">$tmp") or S4P::perish(100, "update_configurator(): Cannot open $tmp for write: $!");
    print FLE "\n# This file was created by s4pm_stringmaker.pl on $now\n";
    print FLE "# DO NOT EDIT THIS FILE MANUALLY!\n\n";
    print FLE "\%cfg_string_info = (\n";
    foreach my $str ( keys %cfg_string_info ) {
        print FLE "    '$str' => {\n";
        print FLE "\t'root_dir' => '" . $cfg_string_info{$str}{'root_dir'} . "',\n";
        print FLE "\t'shortname' => '" . $cfg_string_info{$str}{'shortname'} . "',\n";
        print FLE "\t'longname' => '" . $cfg_string_info{$str}{'longname'} . "',\n";
        print FLE "\t'machine' => '" . $cfg_string_info{$str}{'machine'} . "',\n";
        print FLE "    },\n";
    }
    print FLE ");\n\n";
    print FLE "\$cfg_checkin = \"" . $cfg_checkin . "\";\n";
    print FLE "\$cfg_checkout = \"" . $cfg_checkout . "\";\n";
    print FLE "\n1;\n";

    close(FLE);
    rename($tmp, $cfg) or S4P::perish(30, "update_configurator(): Failed to rename $tmp to $cfg: $!");
    change_mode("0644", $cfg);
    change_mode("0644", $cfgold);
}

sub generate_pool_capacities {

    my ($rh_maxsize, $rh_datatype_coverages, $rh_uses, $rh_pool_map,
        $rh_proxy_esdt, $rh_pool, $rh_autopool_parms) = @_;

    my $allocdisk_cfg = format_hash('%datatype_maxsize', $rh_maxsize) .
        format_hash('%proxy_esdt_map', $rh_proxy_esdt) .
        format_hash('%datatype_pool_map', $rh_pool_map) .
        format_hash('%datatype_pool', $rh_pool);

    my $sum = 0;
    my $i   = 0;
    my %pool_size = ();

    do {
        $i++;
        foreach my $esdt ( @all_datatypes ) {
            my $pool_name = $rh_pool_map->{$esdt};
            my $pool_name = $datatype_pool{$rh_pool_map->{$esdt}};
            my $w1 = ( $rh_maxsize->{$esdt} <= $rh_autopool_parms->{'small_file_thresh'} ) ? $rh_autopool_parms->{'small_file
_weight'} : 1;
            my $w2 = ( $pool_name =~ /INPUT/ ) ? $rh_autopool_parms->{'input_weight'} : 1;
            my $n = $i * $rh_uses->{$esdt} * $w1 * $w2 / ($rh_datatype_coverages->{$esdt});
            $sum += $n * $rh_maxsize->{$esdt};
            if ( $sum > $rh_autopool_parms->{'max_size'} ) {
                last;
            } else {
                $pool_size{$pool_name} += $n * $rh_maxsize->{$esdt};
            }
        }
    } while ( $sum < $rh_autopool_parms->{'max_size'} );

    my $allocdisk_pool = format_hash('%pool_size', \%pool_size);
    $allocdisk_pool .= "# Total size:  " . $sum / (1024 * 1024 * 1024) . " GB\n";
    return ($allocdisk_pool, $allocdisk_cfg);
}

sub get_punt_uses {

    my $r_algorithm_inputs  = shift;
    my $r_algorithm_outputs = shift;

    my %algorithm_inputs  = %{$r_algorithm_inputs};
    my %algorithm_outputs = %{$r_algorithm_outputs};

    my @algorithms = keys %algorithm_outputs;

    my %dependencies = get_dependencies(\%algorithm_inputs, \%algorithm_outputs);
    my $uses = ();
    foreach my $alg ( @algorithms ) {

        my %remaining_algorithms =
            remove_inactive_algorithms($alg, %dependencies);

        my @pges_can_run = keys %remaining_algorithms;
        my @pges_cannot_run = diff($alg, \%dependencies, \%remaining_algorithms);

        foreach my $x ( @pges_cannot_run ) {
            foreach $inp ( @{$algorithm_inputs{$x}} ) {
                my @ar = ();
                my @br = @{$algorithm_outputs{$alg}};
                push(@ar, $inp);
                unless ( union(\@ar, \@br) ) {
                    $uses{$alg}{$inp} = ( exists $uses{$alg}{$inp} ) ? $uses{$alg}{$inp} + 1 : 1;
                }
                my @cr = @{$algorithm_inputs{$alg}};
            }
        }
        foreach $inp ( @{$algorithm_inputs{$alg}} ) {
            $uses{$alg}{$inp} = ( exists $uses{$alg}{$inp} ) ? $uses{$alg}{$inp} + 1 : 1;
        }
    }

    return %uses;
}

sub diff {
    my $pge = shift;
    my $ra = shift;
    my $rb = shift;

    my @akeys = keys %{$ra};
    my @bkeys = keys %{$rb};
    my @aonly = ();

    my %seen;
    map { $seen{$_} = 1 } @bkeys;

    foreach my $item ( @akeys ) {
        push(@aonly, $item) unless ( exists $seen{$item} or $item eq $pge);
    }

    return @aonly;
}

sub union {

    my $ra = shift;
    my $rb = shift;

    foreach my $a ( @{$ra} ) {
        foreach my $b ( @{$rb} ) {
            if ( $b eq $a ) {
                return 1;
            }
        }
    }
    return 0;
}

sub remove_inactive_algorithms {

    my ($punted_pge, %dependencies) = @_;

    my %remaining_algorithms = (%dependencies);

    delete $remaining_algorithms{$punted_pge};

    foreach my $pge ( keys %remaining_algorithms ) {
        my @deps = @{$remaining_algorithms{$pge}};
        foreach $dep ( @deps ) {
            if ( $dep eq $punted_pge ) {
                delete $remaining_algorithms{$pge};
                %remaining_algorithms = remove_inactive_algorithms($pge, %remaining_algorithms);
            }
        }
    }
    return %remaining_algorithms;
}

sub get_dependencies {

    my $r_algorithm_inputs = shift;
    my $r_algorithm_outputs = shift;

    my %algorithm_inputs = %{$r_algorithm_inputs};
    my %algorithm_outputs = %{$r_algorithm_outputs};

    my @pges = keys %algorithm_inputs;

    foreach my $x ( @pges ) {
        foreach my $y ( @pges ) {
            next if ( $x eq $y );
            my @inputs  = @{$algorithm_inputs{$x}};
            my @outputs = @{$algorithm_outputs{$y}};
            if ( union(\@inputs, \@outputs) ) {
                push(@{$dependencies{$x}}, $y);
            } else {
                push(@{$dependencies{$x}}, "");
            }

        }
    }

    return %dependencies;
}

sub pool_ondemand_adjustment {

    my $rh_datatype_max_sizes = shift;
    my $rh_datatype_pool_map = shift;
    my $rh_datatype_pool = shift;

    my %tmp_map = ();
    my %tmp_pool = ();

    foreach my $dt ( keys %{$rh_datatype_max_sizes} ) {
        unless ( $rh_datatype_pool_map->{$dt} eq "INPUT" ) {
            $tmp_map{$dt} = 'OUTPUT';
        } else {
            $tmp_map{$dt} = 'INPUT';
        }
    }
    my $output_pool = dirname( $rh_datatype_pool->{'INPUT'} ) . "/OUTPUT";
    $tmp_pool{'INPUT'} = $rh_datatype_pool->{'INPUT'};
    $tmp_pool{'OUTPUT'} = $output_pool;

    my $rh_tmp_map = \%tmp_map;
    my $rh_tmp_pool = \%tmp_pool;

    return ($rh_tmp_map, $rh_tmp_pool);
}

sub change_mode {
 
    my $mode = shift;
    my $item = shift;
    return unless ( -e $item );

    my @bits = split(//, $mode);

    if ( $multiuser_mode ) {
        $bits[2] = $bits[1];
    }

    my $newmode = join("",@bits);

### First, check to see if we even need to change the mode

    my @info = stat($item);   # mode will be in element with index 2
    my $currentMode = sprintf("%04o", $info[2] & 07777);
    return if ( oct($currentMode) == oct($newmode) );    # No need to chmod

    my $cmd = "umask 0002 && chmod $newmode $item";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
        die $die_msg;
    }
}

sub make_dir {
 
    my $item = shift;
    my $mode = shift;

    my @bits = split(//, $mode);

    if ( $multiuser_mode ) {
        $bits[0] = 2;
        $bits[2] = $bits[1];
    }

    my $newmode = join("",@bits);

    my $cmd = "umask 0002 && mkdir -m $newmode $item";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
        die $die_msg;
    }

}

sub get_stations {

    my @list = ();
    foreach my $sta ( keys %stations ) {
        push(@list, $stations{$sta}{'display_order'} . ":" . $sta);
    }
    if ( -e "$global_dir/sub_notify" ) {
        push(@list, "20:sub_notify");
    }

    my @ordered_list = sort { $a <=> $b } @list;

    my $station_string = "";
    foreach my $item ( @ordered_list ) {
        $item =~ s/^[0-9]*://;
        $station_string .= $item . " ";
    }
    return $station_string . "&";

}

sub mk_backout_tar {

    my ($tmpdir,@files) = @_;
    my @fn = ();

    my $now = localtime(time());
    $now =~ s/ /_/g;
    $now =~ s/://g;

    foreach $f ( @files ) {
        copy($f, "$tmpdir");
        push(@fn, basename($f));
    }

    my $list = join(' ', @fn);

    chdir $tmpdir;
    my $cmd = "tar cvf StringmakerConfigBackup.$now.tar $list";
    print "cmd: [$cmd]\n";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
        die $die_msg;
    }
    move("StringmakerConfigBackup.$now.tar", "..");
    return "StringmakerConfigBackup.$now.tar";
}
