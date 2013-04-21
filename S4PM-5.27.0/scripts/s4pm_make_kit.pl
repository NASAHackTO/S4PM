#!/usr/bin/perl

=head1 NAME

s4pm_make_kit.pl - create an S4PM kit template tar file

=head1 SYNOPSIS

s4pm_make_kit.pl
B<-c> I<cfg_file>
B<-s> I<string>
[B<-i>]

=head1 DESCRIPTION

This script builds an S4PM kit template file from an existing S4PM string
(with some limitations on the string). The assumption is that the string
from which a kit template is being made is a tested and working string with
all configurations appropriately adjusted.

=head1 ARGUMENTS

=over 4

=item B<-c> I<cfg_file>

This is the kit configuration file. See CONFIGURATION FILE.

=item B<-s> I<string>

This is the full path of the root of the S4PM string from which a kit template
will be generated. This root is the location below which the station directories
are to be found.

=item B<-i>

If specified, this option enables interactive mode allowing each step in the
kit building to be seen. By default, the script does not run interactively.

=back

=head1 CONFIGURATION FILE

The kit configuration file contains several parameters:

=over 4

=item $kit_id

String identifier for this kit template.

=item @s4pm_packages

This array contains the full paths of the S4P/S4PM gzipped tar files 
comprising a full build of both S4P and S4PM. The minimum packages needed
are S4P, S4PM, and S4PM_CFG. Other packages, such as S4PM_DISC, are optional.

=item $std_path

This string needs to contain elements needed to be set in the PATH environment
variable on the machine running the S4PM kit. As such, it should contain
a list of pathnames separated by colons. It should not contain the path
to the newly built S4P/S4PM binaries as this will be done automatically. Thus,
only standard UNIX stuff should be in the list.

=back

=head1 SEE ALSO

L<s4pm_stringmaker.pl>

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_make_kit.pl,v 1.13 2007/01/16 17:01:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use S4P::TimeTools;
use S4PM;
use File::Basename;
use File::Find;
use Cwd;
use Config;
use File::Copy;
use Safe;
use Getopt::Std;

getopts('c:s:i');

unless ( $opt_c and $opt_s ) {
    usage();
}
unless ( -e $opt_c ) {
    die "\nmain: Configuration file [$opt_c] doesn't seem to exit!\n\n";
}
unless ( -e $opt_s ) {
    die "\nmain: String station root directory [$opt_s] doesn't seem to exist!\n\n";
}

if ( $opt_i ) {
    intro();
}

# Read in the kit configuration file

my $compartment = new Safe 'CFG';
$compartment->share('$kit_id', '@s4pm_packages', '$std_path', '$kit_working_dir');
$CFG::kit_working_dir = undef;
$compartment->rdo($opt_c) or die "main: Cannot import kit config file $opt_c: $!\n";
$CFG::kit_id =~ s/\s+/_/g;

# Create the working directory

my $cwd = cwd();
unless ( $CFG::kit_working_dir ) {
    $CFG::kit_working_dir = $cwd . "/kit_" . $$;
}
is_continue("About to create a working directory: $CFG::kit_working_dir.");
mkdir("$CFG::kit_working_dir", 0755);

# Copy the existing string into the working directory

is_continue("About to copy the string at its root to the working directory. You may see\nsome error messages. Most should be nothing to worry about, but keep an eye\nout just in case.");
copy_string($opt_s, $CFG::kit_working_dir);

# Do some touch-ups on the copy

is_continue("About to do some touch-ups in the copied string.");
touch_up($CFG::kit_working_dir, $CFG::kit_id);

# Now, build S4P/S4PM placing the binaries in the copied string

is_continue("About to build S4P/S4PM. This may take a few minutes and you'll see lots of\nstuff scroll by. Don't panic.");
build($CFG::kit_working_dir);

# Get rid of logs and similar junk from the copy

is_continue("About to clean out some junk from the copied string including various log\nfiles which are not needed.");
clean($CFG::kit_working_dir);

# With the new build in place, locate the directories where the S4P/S4PM
# binaries are located

is_continue("About to find the location of the newly built S4P/S4PM binaries.");
my $bindir = get_bindir($CFG::kit_working_dir);

# Now, locate the S4P/S4PM libraries

is_continue("About to locate the directory locations of the newly built S4P/S4PM libraries.");
my @libs = get_libdir($CFG::kit_working_dir, "$CFG::kit_working_dir/src");

# Generate the kit start-up script

is_continue("About to generate the kit's start-up script.");
make_start_script($CFG::kit_working_dir, $bindir, @libs);

# Relink binaries in the copied string to point to the newly built binaries

is_continue("With the new build completed, we now need to re-establish all the links so\nthey point to the newly built files.");
relink_binaries($CFG::kit_working_dir, $bindir);

is_continue("Finally, about to make a tarball of your brand new S4PM kit template.");
make_kit_tarball($CFG::kit_working_dir, $CFG::kit_id);

################################################################################
# Subroutines                                                                  #
################################################################################

sub build {
 
    my $working_dir = shift;

### Create the src and tarball directories

    my $tardir = "$working_dir/src/tarballs";
    mkdir("$working_dir", 0755);
    mkdir("$working_dir/src", 0755);
    mkdir($tardir, 0755);

### Move over the S4PM packages, unpack them, and do the builds

    foreach my $tarfile ( @CFG::s4pm_packages ) {
        copy($tarfile, $tardir);
    }

    my $cwd = cwd();
    chdir $tardir;
    foreach my $tarfile ( @CFG::s4pm_packages ) {
        my $tf = basename($tarfile);
        my $cmd = "tar xvzf $tf";
        my ($errstr, $rc) = S4P::exec_system("$cmd");
        if ($rc) {
            print "$errstr\n";
            die $die_msg;
        }
    }
        
    my @make_cmds = (
        "perl Makefile.PL PREFIX=" . "$working_dir/src",
        "make",
        "make install",
        "make clean",
    );
    opendir DIR, $tardir or die "Cannot opendir $tardir: $!";
    while ( defined(my $f = readdir(DIR)) ) {
        if ( -d $f ) {
            next if ( $f eq ".." or $f eq ".");
            chdir "$tardir/$f";
            foreach my $cmd ( @make_cmds ) {
                my ($errstr, $rc) = S4P::exec_system("$cmd");
                if ($rc) {
                    print "$errstr\n";
                    die $die_msg;
                }
            }
            chdir $tardir;
        }
    }

    chdir $cwd;

    return;
}

sub clean {

    my $working_dir = shift;
    my $cwd = cwd();

    chdir($working_dir) or die "\nFailed to chdir to [$working_dir]: $!\n\n";

### Remove all station.log and related files

    my $cmd = "/bin/rm -fr */station.log */station.lock */station.pid */station_counter.log";
    print "cmd: [$cmd]\n";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
    }

### Clean out the ARCHIVE directory

    find(\&empty, "$working_dir/ARCHIVE");

### Clean out the DATA directory

    find(\&empty, "$working_dir/DATA");

### Remove any big tar files

    find(\&remove_tars, "$working_dir");

    chdir $cwd;
}

sub copy_string {

    my $root = shift;
    my $working_dir = shift;
    my @copy_items = (
        'ARCHIVE', 'DATA', 'acquire_data', 'allocate_disk', 'auto_acquire', 
        'export', 'find_data', 'prepare_run', 'receive_dn', 
        'receive_pan', 'register_*', 'select_data', 'sweep_data', 'track_data',
    );
    my $list = join(" ", @copy_items);

    my $cwd = cwd();
    chdir($root) or die "\nFailed to chdir to station root [$root]: $!\n\n";
    my $cmd = "cp -R -L $list $working_dir";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
#       usage();
    }

### Do the Run Algorithm directory separately since recursive copy has
### problems in figuring out the dereferencing of the algorithm subdirectories
### Not sure why exactly, but this treatment just works.

    my $cmd = "cp -R -L run_algorithm $working_dir";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
#       usage();
    }
    chdir($cwd);
}

sub get_bindir {

    my $working_dir = shift;

### Determine the directory where the binaries and config files went
### On Linux systems, the directory is bin, on Mac, it is usr/bin.

    my $bindir = "$working_dir/src/bin";
    unless ( -e "$bindir/s4pm_find_data.pl" ) {
        $bindir = "$working_dir/src/usr/bin";
        unless ( -e "$bindir/s4pm_find_data.pl" ) {
            die "build(): Failed to locate binaries after running make\n\n";
        }
    }

    return $bindir;

}

sub intro {

    print "\n\n################################################################################\n";
    print "This script will generate an S4PM kit tar file from the string whose root is:\n";
    print "$opt_s\n";
    print "\n";
    print "using the configuration file:\n";
    print "$opt_c\n";
    print "\n";
    print "The entire process will only take a few minutes. Good luck!\n";
    print "################################################################################\n\n\n";
}

sub is_continue {

    my $prompt = shift;

    if ( $opt_i ) {
        print "################################################################################\n";
        print "$prompt\n";
        print "################################################################################\n";
        print "Ok? (y/n) [y] ";
        $ans = <STDIN>;
        chomp($ans);
        if ( $ans =~ /^N/ or $ans =~ /^n/ ) {
            print "\nOk. Good-bye.\n\n";
            exit 0;
        }
    } else {
        print "################################################################################\n";
        print "$prompt\n";
        print "################################################################################\n";
    }

}

sub get_libdir {

    my $working_dir = shift;
    my $dir = shift;

    my @libs = ();
    my $str;

    open(FIND, "find $dir -name S4P.pm -print |");
    while ( my $f = <FIND> ) {
        chomp $f;
        next if ( $f =~ /tarballs/ );
        $str = dirname($f);
    }
    close(FIND);
    my $str1 = $str;
    $str1 =~ s/$working_dir//;
    open(FIND, "find $dir -name S4PM.pm -print |");
    while ( my $f = <FIND> ) {
        chomp $f;
        next if ( $f =~ /tarballs/ );
        $str = dirname($f);
    }
    close(FIND);
    my $str2 = $str;
    $str2 =~ s/$working_dir//;

    $libs[0] = $str1;
    $libs[1] = $str2;

    return @libs;
}

sub make_kit_tarball {

    my $working_dir = shift;
    my $kit_id = shift;

    my $cwd = cwd();
    chdir($working_dir) or die "\nmake_kit_tarball(): Failed to chdir to [$working_dir]: $!\n\n";

    my $now = S4P::TimeTools::CCSDSa_Now;
    my $tarfile = "S4PMKit:" . $kit_id . "-" . $now . ".tar.gz";
    $tarfile =~ s/\://g;
    my $cmd = "tar cvzf $tarfile *";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
        usage();
    }
    chdir($cwd);
}

sub make_start_script {

    my($working_dir, $bdir, @libs) = @_;

    my $to_link = $bindir;
    $to_link =~ s#$working_dir#..#;
    my $cwd = cwd();

    chdir($working_dir) or die "\nmake_start_script(): Failed to chdir to [$working_dir]: $!\n\n";

    my $stringer_file = "$working_dir/s4pm_kit_start.pl";
    open (STRINGER, ">$stringer_file") or 
        die "\nmake_start_script(): Failed to open start kit file [$stringer_file] for write: $!\n\n";

    my $now = `/bin/date`;
    chomp($now);

    print STRINGER <<EOF;
#!/usr/bin/perl

# This file was created by s4pm_make_kit.pl with the kit option on $now
# DO NOT EDIT THIS FILE MANUALLY!

use Cwd;

my \$cwd = cwd();

# Locate S4P/S4PM binaries

my \$bindir = \"\$cwd/src/bin\";
unless ( -e \"\$bindir/s4pm_find_data.pl\" ) {
    \$bindir = \"\$cwd/src/usr/bin\";
    unless ( -e \"\$bindir/s4pm_find_data.pl\" ) {
        die \"s4pm_kit_start.ksh: Failed to locate binaries after running make\n\n\";
    }
}

# Set required environment variables

\$ENV{'PERLLIB'} = \"\$cwd/" . \"$libs[0]\" . \":\" . \"\$cwd/" . \"$libs[1]\";
\$ENV{'PATH'} = \"$std_path\" . ":" . "\$bindir\";

# Start up the stations

system(\"\$bindir/s4pstart.ksh\");

EOF

    close(STRINGER);
    chmod(0775, $stringer_file);

    chdir $cwd;

}

sub relink_binaries {

    my $working_dir = shift;
    my $bindir = shift;

    my $to_link = $bindir;
    $to_link =~ s#$working_dir#..#;

    my @skips = ('ARCHIVE', 'PDR', 'PDR_LIMBO', 'DATA', 'VERSION', 'src');
    my $skip_str = join(" ", @skips);

    opendir(DIR, $working_dir) or 
        die "\nrelink_binaries(): Failed to opendir [$working_dir]: $!\n\n";
    while ( defined(my $dir = readdir(DIR)) ) {
        next unless ( -d "$working_dir/$dir" );
        next if ( $dir eq "." or $dir eq ".." or $skip_str =~ /$dir/ );
        opendir(SUBDIR, "$working_dir/$dir") or 
            die "\nrelink_binaries(): Failed to opendir [$working_dir/$dir]: $!\n\n";
        while ( defined(my $file = readdir(SUBDIR)) ) {
            if ( $file =~ /\.pl$/ ) {
                if ( -e "$bindir/$file" ) {
                    unlink( "$working_dir/$dir/$file");
                    symlink("$to_link/$file", "$working_dir/$dir/$file");
                }
            }
        }
        closedir(SUBDIR);
    }
    closedir(DIR);
}

sub empty {
    unlink unless ( -d $_ and $_ !~ /\.hdf$/ );
}

sub remove_tars {
    unlink if ( $_ =~ /\.tar$/ );
    unlink if ( $_ =~ /\.tar\.gz$/ );
}

sub touch_up {

    my $working_dir = shift;
    my $kit_id = shift;

    my $cwd = cwd();
    chdir($working_dir) or die "\ntouch_up(): Failed to chdir to [$working_dir]: $!\n\n";

### Take care of Allocate Disk database and its links

    $allocdb = "$working_dir/allocate_disk/s4pm_allocate_disk.db";
    unless ( -e $allocdb ) {
        my $cmd = "touch $allocdb";
        my ($errstr, $rc) = S4P::exec_system("$cmd");
        if ($rc) {
            print "$errstr\n";
        }
    }
    unlink( "$working_dir/acquire_data/s4pm_allocate_disk.db");
    unlink( "$working_dir/register_data/s4pm_allocate_disk.db");
    unlink( "$working_dir/register_local_data/s4pm_allocate_disk.db");
    unlink( "$working_dir/sweep_data/s4pm_allocate_disk.db");
    symlink("../allocate_disk/s4pm_allocate_disk.db", "$working_dir/acquire_data/s4pm_allocate_disk.db");
    symlink("../allocate_disk/s4pm_allocate_disk.db", "$working_dir/register_data/s4pm_allocate_disk.db");
    symlink("../allocate_disk/s4pm_allocate_disk.db", "$working_dir/register_local_data/s4pm_allocate_disk.db");
    symlink("../allocate_disk/s4pm_allocate_disk.db", "$working_dir/sweep_data/s4pm_allocate_disk.db");
        
### Remake the PDR and PDR_LIMBO directories

mkdir("$working_dir/PDR", 0755);
mkdir("$working_dir/PDR_LIMBO", 0755);

### Build a brand new station.list file

    my @stations = (
        'auto_request', 'acquire_data', 'receive_dn', 'register_data',
        'select_data', 'find_data', 'prepare_run', 'allocate_disk',
        'run_algorithm', 'register_local_data', 'export', 'track_data',
        'receive_pan', 'sweep_data',
    );

    open(LST, ">$working_dir/station.list") or 
        die "\n\ntouch_up(): Failed to open $working_dir/station.list for write: $!\n\n";
    my $list = join("\n", @stations);
    print LST "$list\n";
    close(LST);
    
### Create a kit ID file 

    my $kfile = "KitID:" . $kit_id;
    my $cmd = "touch $kfile";
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        print "$errstr\n";
        die "\n\nFailed to touch kit ID file: [$kfile]: $!\n\n";
    }

    chdir($cwd);
}

sub usage {
    die "\nUsage: " . basename($0) . " -c <config_file> -s <string>\n\n";
}

