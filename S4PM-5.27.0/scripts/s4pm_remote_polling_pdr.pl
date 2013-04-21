#!/usr/bin/perl

=head1 NAME

s4pm_remote_polling_pdr.pl - script to poll a remote directory for PDRs

=head1 SYNOPSIS

s4p_remote_polling_pdr.pl
B<-h> I<remote_hostanme>
B<-p> I<remote_directory>
[B<-d> I<local_directory>]
[B<-o> I<history_file>]
[B<-t> I<protocol: FTP or SFTP>]
[B<-e> I<PDR name pattern>]
[B<-j> I<jobtype>]
[B<-P> I<remote PAN URL>]
[B<-H>]
I<workorder>

=head1 ARGUMENTS

=over

=item B<-h> I<remote_hostanme>

Hostname to ftp to in order to poll. A suitable entry in the .netrc file must
exist for this host.

=item B<-p> I<remote_directory>

Directory on the remote host (as seen in the ftp session) which will be
examined for new PDRs.

=item B<-d> I<local_directory>

Local directory to which new PDRs will be directed.

=item B<-o> I<history_file>

Local filename containing list of previously encountered PDRs.  Name defaults
to "../oldlist.txt".

=item B<-t> I<protocol>

Specifies the protocol to be used for polling. Valids are FILE, FTP and SFTP.

=item B<-e> I<PDR name pattern>

Optional argument which specifies filename pattern (default is \.PDR)

=item B<-P> I<remote PAN URL>

Optional argument which specifies where to send back the PAN.
This is stashed in the ORIGINATING_SYSTEM for later use by acquire_data.

=item B<-H>

Replace node names in PDR with the host from which the PDR was obtained.
(This is a workaround for MODAPS PDRs, which reference a node_name that is not
accessible.)

=item B<-j> I<jobtype>

Output jobtype for work order.  If this is specified, an output work order of the type JOBTYPE.<PDRNAME>.wo is output instead of the native filename.

=back

=head1 DESCRIPTION

This script polls the I<remote_directory> directory on host I<remote_hostname>
using COPY/FTP/SFTP. It compares PDRs (.PDR) in the polling directory with 
entries in the I<history_file> file. If a new PDR entry is found it is 
transferred to the I<local_directory> directory.

=head1 AUTHOR

C. Lynnes, NASA/GSFC, Code 610.2, Greenbelt, MD 20771.
Mike Theobald, NASA/GSFC, Code 902, Greenbelt, MD  20771.
T. Dorman, SSAI, NASA/GSFC, Greenbelt, MD 20771
M. Hegde, SSAI, NASA/GSFC, Greenbelt, MD 20771

=cut

################################################################################
# s4pm_remote_polling_pdr.pl,v 1.7 2008/02/12 17:09:53 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use Net::FTP;
use Net::Netrc;
use Getopt::Std;
use Safe;
use File::Basename;
use File::Copy;
use S4P;
use S4P::PDR;

# Use a BEGIN block to include Net::SSH2 so that the program won't croak if
# Net::SSH2 is not available.
BEGIN {
    eval 'require Net::SSH2;';
}

use vars qw($opt_h $opt_p $opt_d $opt_o $opt_t $opt_j $oldlist $opt_e $opt_H $opt_P);

#Read and parse command line options
getopts( 'h:p:P:d:o:t:F:e:Hj:' );

S4P::perish( 1,
             "Usage: $0 -h hostname -p remote_dir -d local_dir"
             . " -o oldlist_file -t protocol(FILE/FTP/SFTP)" )
    unless ( defined($opt_h) and defined($opt_p) );

# A file to track history of transferred PDRs.
my $oldlistfile = $opt_o || "../oldlist.txt";
my $hostname = $opt_h;
my $polldir = $opt_p;
my $destdir = $opt_d || ".";
my $protocol = $opt_t || "FTP";
my $pattern = $opt_e || '\.PDR$';
my $sshpid;

# Make sure the protocol is supported
S4P::perish( 1,
    "Protocol (-t), $protocol, not supported; valids are FILE, FTP and SFTP" )
    unless ( $protocol eq 'FTP' || $protocol eq 'SFTP' || $protocol eq 'FILE' );

# A hash to keep track of downloaded PDRs
my %oldlist;

$destdir .="/" unless ( $destdir =~ /\/$/ );

# Lock a dummy file to lock out access old list file.
open( LOCKFH, ">$oldlistfile.lock" ) || S4P::perish( 1, "Failed to open lock file" );
unless( flock( LOCKFH, 2 ) ) {
    close( LOCKFH );
    S4P::perish( 1, "Failed to get a lock" );
}

# Read oldlist (%oldlist hash in external file of remote PDRs already processed)
open ( OLDLIST,"$oldlistfile" )
    || S4P::logger( "WARN",
                    "Failed to open oldlist file $oldlistfile; created new.");

while ( <OLDLIST> ) {
    chomp() ;
    $oldlist{$_} = "old";
}
close(OLDLIST);

# Connection object and list to hold polling result.
my ( $ftp, @remfiles );
if ( $protocol eq 'FTP' ) {
    # specify $FTP_FIREWALL shell env. variable to enale ftp through firewall
    my $firewall = $ENV{FTP_FIREWALL} ? $ENV{FTP_FIREWALL} : undef;
    my $firewallType = $ENV{FTP_FIREWALL_TYPE} ? $ENV{FTP_FIREWALL_TYPE} : 1;

    # Open FTP connection, login, cd to polldir and ls contents.
    if ($firewall) {
        $ftp = Net::FTP->new( $hostname, Firewall => $firewall, FirewallType => $firewallType );
    }
    else {
        $ftp = Net::FTP->new( $hostname );
    };
    S4P::perish( 1, "Failed to create an FTP object for $hostname" )
        unless defined $ftp;
    S4P::perish( 1, "Failed to login to $hostname (" . $ftp->message . ")" )
        unless $ftp->login();
    S4P::logger( "INFO", "Beginning scan of $hostname:$polldir" );
    if ($ftp->cwd($polldir)) {
        my @remdirlist = $ftp->dir();
        # Dir gives us a full directory listing
        # Lop off the beginning "total N files", if there
        # Then take the last word in the line as the filename
        @remfiles = map {my @w = split(/\s+/,$_); pop @w} grep (!/^total \d/, @remdirlist);
    }
    else {
        S4P::perish( 1, "Failed to cwd to $polldir (" . $ftp->message . ")" )
    }
} elsif ( $protocol eq 'SFTP' ) {
    # Lookup hostname in .netrc to find login name.
    my $machine = Net::Netrc->lookup( $hostname )
        || S4P::perish( 1, "Failed to lookup $hostname in .netrc" );
    my $login = $machine->login()
        || S4P::perish( 1,
            "Failed to find login name for $hostname in .netrc" );
    my $passwd = ( defined $login ) ? ( $machine->password() ) : undef;

    my $firewall = $ENV{FTP_FIREWALL} ? $ENV{FTP_FIREWALL} : undef;
    my $proxyPort = $ENV{FTP_FIREWALL_PORT} ? $ENV{FTP_FIREWALL_PORT} : '1080';
    my $sshPort = $ENV{FTP_SSH_PORT} ? $ENV{FTP_SSH_PORT} : '22';
    my $localHost = $ENV{FTP_LOCALHOST} ? $ENV{FTP_LOCALHOST} : 'localhost';
    my $localPort = $ENV{FTP_LOCAL_PORT} ? $ENV{FTP_LOCAL_PORT} : '30001';

    # establish ssh tunneling through firewall with OpenSSH
    #   -f : Requests ssh to go to background just before command execution.
    #   -N : Do not execute a remote command.
    #   -g : Allows remote hosts to connect to local forwarded ports.
    if ( defined $firewall ) {
        my $tunnel = "$localPort:$hostname:$sshPort";
        my $cmd = "ssh -f -N -g -l $login" .
            " -o \"ProxyCommand /usr/local/bin/ssh-connect -S $firewall:$proxyPort %h %p\" " .
            " -L $tunnel $hostname";

        # check whether the SSH tunneling session is still active
        $sshpid = `ps -ef | grep $tunnel | grep -v grep | awk '{print \$2}'`;

        # setup a new tunnel session if the session is dead
        if ( $sshpid eq "" ) {
            my $sshStatus = system("$cmd 2>&1 /dev/null");
            $sshpid = `ps -ef | grep $tunnel | grep -v grep | awk '{print \$2}'`;
        }
    }

    # Obtain a SSH connection.
    $ftp = Net::SSH2->new();
    S4P::perish( 1, "Failed to create Net::SSH2" ) unless $ftp;

    # establish connection via ssh tunneling while behind firewall
    if ( defined $firewall ) {
        $ftp->connect( $localHost, $localPort )
            || S4P::perish( 1, "Failed to connect to $hostname" );
        unless ( $ftp->auth_password( $login, $passwd ) ) {
            $ftp->disconnect();
            S4P::perish( 1, "Failed to authenticate with $hostname" );
        }
    }

    # establish ssh connection directly to remote host
    else {
        $ftp->connect( $hostname )
            || S4P::perish( 1, "Failed to connect to $hostname" );
        # For now, use default public/private keys and authenticate.
        my $pubKeyFile = "$ENV{HOME}/.ssh/id_dsa.pub";
        my $priKeyFile = "$ENV{HOME}/.ssh/id_dsa";
        unless ( $ftp->auth_publickey( $login, $pubKeyFile, $priKeyFile ) ) {
            $ftp->disconnect();
            S4P::perish( 1, "Failed to authenticate with $hostname" );
        }
    }
    # Obtain Net::SSH2::SFTP object and read the polling directory.
    my $sftp = $ftp->sftp();
    my $dir = $sftp->opendir( $polldir );
    while ( my $stat = $dir->read() ) {
        # Skip hidden files.
        next if ( $stat->{name} =~ /^\./ );
        my $path = $polldir . "/$stat->{name}";
        # Test whether directory content is a sub-directory; if so, skip it.
        my $subDir = $sftp->opendir( $path );
        if ( $subDir ) {
            undef $subDir;
        } else {
            # Compile regular files in a list to be used later.
            push( @remfiles, $stat->{name} );
        }
    }
    undef $dir;
    undef $sftp;
} elsif ( $protocol eq 'FILE' ) {
    $polldir =~ s#$#/# unless ( $polldir =~ m#/$# );
    @remfiles = glob( "$polldir*" );
}

S4P::logger( "INFO", @remfiles . " files found in $polldir" );
my $xferstatus = 0;

# Check contents against oldlist, transfer any new, and update oldlist
foreach my $remfile ( @remfiles ) {
    next unless ( $remfile =~ m/$pattern/ );
    S4P::logger( "INFO", "$remfile is old: skipping" )
        && next if ( $oldlist{$remfile} eq 'old' );

    if ( ref( $ftp ) eq 'Net::FTP' ) {
        if ( $ftp->get( $remfile ) ) {
            S4P::logger( "INFO", "Success in transfer of $remfile" );
        } else {
            S4P::logger( "ERROR",
                "Failure in transfer of $remfile (" . $ftp->message . ")"  );
            $xferstatus = 1 ;
        }
    } elsif ( ref( $ftp ) eq 'Net::SSH2' ) {
        if ( $ftp->scp_get( "$polldir/$remfile" ) ) {
            S4P::logger( "INFO", "Success in transfer of $remfile" );
        } else {
            my ( $code, $err_name, $err_str ) = $ftp->error();
            S4P::logger( "ERROR",
                "Failure in transfer of $remfile ($code/$err_name/$err_str)" );
            $xferstatus = 1;
        }
    } elsif ( $protocol eq 'FILE' ) {
        ( my $localdir = $destdir ) =~ s#/$##;
        my $localfile = "$localdir/" . basename($remfile);
        if ( copy( $remfile, $localfile ) ) {
            S4P::logger( "INFO", "Success in copy of $remfile" );
            chmod ( 0644, "$localfile" );
            $oldlist{$remfile} = "new";
        } else {
            $xferstatus = 1;
            S4P::logger ( "ERROR", "Failure to copy " . basename ($remfile)
                         . " to $destdir" );
        }
    }

    # If successful in transfering file, move the file to destination
    # directory and make the file writable. Save the file name in history.
    unless ( $xferstatus || $protocol eq 'FILE' ) {
        if ( move( basename( $remfile ), $destdir ) ) {
            chmod( 0644, "$destdir/$remfile" );
            $oldlist{$remfile} = "new";
        } else {
            $xferstatus = 1;
            S4P::logger( "ERROR", "Failure to move " . basename($remfile)
                         . " to $destdir" );
        }
    }

    # Post-transfer cleanup:  fix host and rename to work order style name
    my $path = "$destdir/" . basename($remfile);
    fix_pdr($path, $hostname, 1, $opt_P) if ($opt_P || ($opt_H && $hostname));
    rename_work_order($path, $opt_j) if $opt_j;
}

# Gracefully, close sessions.
$ftp->disconnect() if ( ref( $ftp ) eq 'Net::SSH2' );
$ftp->quit() if ( ref( $ftp ) eq 'Net::FTP' );
system("kill $sshpid") unless ( $sshpid eq "" );

# Create new oldlist
open ( OLDLIST, ">>$oldlistfile" )
    || S4P::perish( 1, "Failed opening oldlist file $oldlistfile for write." );
foreach my $remfile ( sort keys( %oldlist ) ) {
   next if ( $oldlist{$remfile} eq "old" );
   print OLDLIST "$remfile\n" ;
}
close( OLDLIST );

# Remove lock
close( LOCKFH );
flock( LOCKFH, 8 );

# Exit with a status code:0 for success, 1 for failure
exit( $xferstatus );

sub fix_pdr {
    my ($pdr_file, $host, $version_format, $remote_pan_url) = @_;

    # Read in PDR and parse it
    my $pdr = S4P::PDR::read_pdr($pdr_file) or 
        die "Cannot read PDR from $pdr_file: $S4P::PDR::errstr\n";

    if ($host) {
        # Change the node_names in all the file_groups
        map {$_->node_name($host)} @{$pdr->file_groups};
    }
    if ($version_format) {
        # Force the data_version "set" method to reformat the version
        foreach my $gr ( @{$pdr->file_groups} ) {
            $gr->data_version($gr->data_version);
        }
    }

    # If a remote PAN dir is specified, stick it in the ORIGINATING_SYSTEM
    # attribute for later access by S4PM
    if ($remote_pan_url) {
        my $pan = "$remote_pan_url/" . basename($pdr_file);
        $pan =~ s/\.PDR$/.PAN/;
        $pan =~ s/\.pdr$/.pan/;
        $pdr->originating_system($pan);
    }

    # Write to a temporary output file
    my $outfile = "$pdr_file.tmp";
    my $rc = $pdr->write_pdr($outfile);
    S4P::perish(101, "Cannot write PDR to $outfile: $S4P::PDR::errstr") 
        if ($rc != 0);

    # Replace current file with changed output file
    move($outfile, $pdr_file) or 
        S4P::perish(102, "Failed to move $outfile to $pdr_file: $!");
    return 1;
}

sub rename_work_order {
    my ($pathname, $job_type) = @_;

    # Parse the name up and tweak it
    my $base = basename($pathname);
    $base =~ s/\.PDR$//i;
    my $dir = dirname($pathname) || '.';

    # Use a name with an output work order form
    my $newpath = "$dir/$job_type.$base.wo";

    # Now move the file
    if (move($pathname, $newpath)) {
        S4P::logger('INFO', "MOved $pathname to $newpath");
    }
    else {
        S4P::perish(102, "Failed to move $pathname to $newpath: $!");
    }
    return $newpath;
}
