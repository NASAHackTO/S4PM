=head1 NAME

S4PMOD.pm - package containing methods used by S4PM on-demand

=head1 SYNOPSIS

use S4PM::S4PMOD;

=head1 DESCRIPTION

=head1 EXIT

=head1 AUTHORS

M. Hegde, SSAI

=head1 CREATED

November 01, 2002

=cut

################################################################################
# S4PMOD.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::S4PMOD;

use File::Copy;
use File::Basename;
use Cwd;
use S4P;
use strict;

################################################################################
# ^NAME: 
#   GenerateId
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#
#
# ^RETURN VALUE:
#   A number if successful; undef otherwise.
#
# ^ALGORITHM:
#   The function uses a file to generate and store a sequence number. For every
#   invokation, the number stored in the file is incremented and saved in the
#   file. The pre-increment value is returned for use as a sequence number.
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub GenerateId
{
    my ( $file ) = @_;
    local ( *FH );

    # Try to open the file in rw+ mode
    open( FH, "+<$file" ) || open( FH, "+>$file" ) || return undef;

    # Try to lock the file; on failure to lock, return an empty string
    if ( ! flock( FH, 2 ) ) {
        close( FH );
        return undef;
    }

    # If the file exists, read from the file
    my ( $curCount, $nextCount );
    if ( $curCount = <FH> ) {
        $curCount =~ s/^\s*|\s*$//g;
    } else {
        $curCount = 1;
    }

    # Increment the count and write it back, remove the lock and close the file
    $nextCount = $curCount + 1;
    seek( FH, 0, 0 );
    local ( $| ) = 1;
    print FH $nextCount, "\n";
    flock( FH, 8 );
    close( FH );

    # Return the count
    return $curCount;
}
################################################################################
# ^NAME: 
#   ParseEcsDataNotification
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#
#
# ^RETURN VALUE:
#   A hash ref containing data notice fields.
#
# ^ALGORITHM:
#   Parse the e-mail and store fields such as order ID, media info, granule
#   info, etc. ORDER_ID, FTP_HOST, FTP_DIR, MEDIA_TYPE are stored in the hash
#   with corresponding keys. Granule info is stored as a hash ref in the 
#   'GRANULES' field. Granules hash ref has granule UR as the primary key. A 
#   secondary key 'STATUS' stores the status of the granule (1/0=>ok/not ok), 
#   'FILES' stores a hash ref with file name as the key and file size as the 
#   value.
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub ParseEcsDataNotification
{
    my ( $file ) = @_;
    
    # Try to open the file containing data notification.
    local ( *UNIT );

    S4P::logger( 'WARN', "failed to open data notification $file for reading" )
    and return undef
        unless ( open( UNIT, $file ) );
    
    # Read the e-mail header and discard it.
    {
        local $/ = "";
        my $header = <UNIT>;
    }
    
    # Read all the lines.
    my @lineList = <UNIT>;    

    # A container for holding information contained in the data notifiacation.
    my $dnRef = {};

    # A container for holding granule information
    my $granuleRef = {};
    
    # Variable for current granule ID and current file name
    my ( $granuleID, $fileName ) = ( undef, undef );

    # Loop through each line and parse.
    foreach my $line ( @lineList ) {
    
        if ( not defined $dnRef->{ORDER_ID}
             and $line =~ /^\s*ORDERID\s*:\s*([^\s]+)/i ) {
            $dnRef->{ORDER_ID} = $1;    # Get the order ID.
        } elsif ( not defined $dnRef->{FTP_HOST}
                  and $line =~ /^\s*FTPHOST\s*:\s*([^\s]+)/i ) {
            $dnRef->{FTP_HOST} = $1;    # Get the FTP host name
        } elsif ( not defined $dnRef->{FTP_DIR}
                  and $line =~ /^\s*FTPDIR\s*:\s*([^\s]+)/i ) {
            $dnRef->{FTP_DIR} = $1;     # Get the FTP directory name
        } elsif ( not defined $dnRef->{FTPEXPR}
                  and $line =~ /^\s*FTPEXPR\s*:\s*([^\s]+)/i ) {
            $dnRef->{FTP_EXPR} = $1;    # Get the FTP expiry date
        } elsif ( not defined $dnRef->{MEDIATYPE} 
                  and $line =~ /^\s*MEDIATYPE\s*:\s*([^\s]+)/i ) {
            $dnRef->{MEDIA_TYPE} = $1;  # Get the media type
        } elsif ( $line =~ /^\s*GRANULE\s*:.+:([^:]+:[^:]+:[^:\s]+)$/ ) {

	    # Accumulate granule IDs
	    $granuleID = $1;

	    # Set the status for granule to true (1)
	    $granuleRef->{$granuleID}{STATUS} = 1;

	    # Create a holder for storing files belonging to the granule
	    $granuleRef->{$granuleID}{FILES} = {};

	} elsif ( $line =~ /^\s*STATUS\s*:\s*(.+)\s*$/i ) {

	    # If there exists a status line for a granule, by default it is
	    # considered failure and hence the status is set to false (0).
	    $granuleRef->{$granuleID}{STATUS} = 0 if defined $granuleID;
	    S4P::logger( 'WARN', "Status found for unknown granule ID in data "
				 . "notification" ) unless defined $granuleID;

	} elsif ( $line =~ /^\s*FILENAME\s*:\s*(.+)\s*$/i ) {

	    # Store the file name
	    $fileName = $1;

	} elsif ( $line =~ /^\s*FILESIZE\s*:\s*(\d+)/i ) {

	    # Store file size and save it as a value of the hash with file name
	    # as the key
	    my $fileSize = $1;
	    $granuleRef->{$granuleID}{FILES}{$fileName} = $fileSize 
		if ( defined $granuleID and defined $fileName );

	    S4P::logger( 'WARN', "File size ($fileSize) found for unknown "
				 . "file name" ) unless defined $fileName;
	    S4P::logger( 'WARN', "File size ($fileSize) found for unknown "
				 . "granule" ) unless defined $granuleID;

	}
    }
    
    close( UNIT );
    $dnRef->{GRANULES} = $granuleRef if defined $granuleRef;
    return $dnRef;     
}

################################################################################
# ^NAME: 
#   ParseWorkOrderName
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   ParseWorkOrderName( <work order name> );
#
# ^RETURN VALUE:
#   A list containing job ID and job Type.
#
# ^ALGORITHM:
#   Extract the job ID and job type from work order name in the 
#   format=*.<job type>.<job ID>
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub ParseWorkOrderName
{
    my ( $workOrder ) = @_;
    
    # Get the order ID and job type from work order name. Work order names must
    # be of the format *<job type>.<order ID>.
    my ( $jobID, $jobType );
    if ( $workOrder =~ /\.wo$/ ) {
	( $jobID, $jobType ) = ( $workOrder =~ /([^.]+)\.([^.]+)\.wo/ )
                                ? ( $2, $1 )
                                : ( undef, undef );
    } else {
	( $jobID, $jobType ) = ( $workOrder =~ /([^.]+)\.([^.]+)$/ )
                                ? ( $2, $1 )
                                : ( undef, undef );
    }
    return ( $jobID, $jobType );
}

################################################################################
# ^NAME: 
#   CreateWorkOrder
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   CreateWorkOrder( <file name>, <configuration hash ref> );
#
# ^RETURN VALUE:
#   0/1 => failure/succes
#
# ^ALGORITHM:
#   This is a wrapper around write_config() which was from the original 
#   GenConfig.pm. write_config() has been subsumed into this module.
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub CreateWorkOrder
{
    my ( $file, $config ) = @_;
    
    return write_config( FILE => $file, CONFIG => $config );
}

################################################################################
# ^NAME: 
#   CmdInterpolate
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   CmdInterpolate( <string to be intepolated>, <work order hash ref> );
#
# ^RETURN VALUE:
#   Interpolated string or undef.
#
# ^ALGORITHM:
#   1) Replace any string enclosed in '<<' and '>>' with the hash value whose
#   keys is the enclosed string.
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub CmdInterpolate
{
    my ( $command, $workOrder ) = @_;
    
    my $origCmd = $command;
    
    # Search and replace words/phrases enclosed in '<<' and '>>'. If the hash
    # does not contain the word/phrase as the key, log a message and return 
    # undef.
    while ( $command =~ /<<(.+?)>>/ ) {
        my $match = $1;
        unless ( defined $workOrder->{$match} ) {
            S4P::logger( 'WARN', "Failed to interpolate <<$match>> in the "
                                 . "command=$origCmd" );
            return undef;
        }
        
        $command =~ s/<<$match>>/$workOrder->{$match}/;
    }
    
    return $command;
}

################################################################################
# ^NAME: 
#   RequestEcsData
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   RequestEcsData( );
#
# ^RETURN VALUE:
#   ECS order Id if successful.
#
# ^ALGORITHM:
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub RequestEcsData
{
    my ( %arg ) = @_;
    
    local( *UNIT ) = @_;
    
    # Variables for order ID and order status.
    my ( $id, $status ) = ( undef, 1 );
    
    unless ( defined $arg{SERVER}{ADDRESS} ) {
        S4P::logger( 'WARN', "Server address not defined" );
        $status = 0;
    }    
    unless ( defined $arg{SERVER}{PORT} ) {
        S4P::logger( 'WARN', "Server port not defined" );
        $status = 0;
    }    
    unless ( defined $arg{CLIENT}{COMMAND} ) {
        S4P::logger( 'WARN', "Client command not defined" );
        $status = 0;
    }    
    unless ( defined $arg{ODL_TREE} ) {
        S4P::logger( 'WARN', 'ODL tree not found' );
        return;
    }    

    # Try to open the order file for writing; on failure, stop.
    if ( open(UNIT, ">$arg{ORDER_FILE}") ) {
        print UNIT $arg{ODL_TREE}->toString(), "\nEND\n";
        close( UNIT );
        $status = 1;
    } else {
        $status = 0;
        S4P::logger( 'WARN', "Failed to open order file, $arg{ORDER_FILE}, for "
                             . "writing" );
    }
    
    # Try to open V0-client command file for writing; on failure, stop.
    if ( $status and open(UNIT, ">$arg{COMMAND_FILE}") ) {
        print UNIT "VERBOSE ON;\n",
                    "SERVER_PORT $arg{SERVER}{PORT};\n",
                    "SERVER_ADDRESS $arg{SERVER}{ADDRESS};\n",
                    "CST {\n",
                    "\tSEND $arg{ORDER_FILE};\n",
                    "\tRECV $arg{CLIENT}{TIMEOUT};\n",
                    "}\n";                    
        close( UNIT );
    } else {
        $status = 0;
        S4P::logger( 'WARN', "Failed to open v0-client command file, "
                             . "$arg{COMMAND_FILE}, for writing" );
    }    
    
    # If creation of ECS product request and V0-client files are successful, try
    # sending the request.
    if ( $status ) {
        S4P::logger( 'INFO', 
		     "Invoking $arg{CLIENT}{COMMAND} to send product request" );
        my $response = `$arg{CLIENT}{COMMAND} $arg{COMMAND_FILE}`;
        S4P::logger( 'INFO', $response );
        # Remove the temporary file produced by V0-client.
        unlink ( './tmp.odl' ) if ( -f './tmp.odl' );
        
        # Get the status code and order ID.
        $status = ( $response =~ /STATUS_CODE\s*=\s*(\w+)/is ) ? $1 : 0;
        $id = ( $status == 1 and 
                $response =~ /DAAC_ORDER_ID\s*=\s*([\"\'])?(\w+)(\1)/is 
              ) ? $2 : undef;
    }
    
    return ( defined $id ) ? $id : undef;
           
}

################################################################################
# ^NAME: 
#   DeleteJob
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   DeleteJob( <hash reference containing arguments> )
#
# ^RETURN VALUE:
#   1=successful, 0=failure
#
# ^ALGORITHM:
#   1) Delete the job from queue.  
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub DeleteJob
{
    my ( $arg ) = @_;
    
        
    # Try to get the work order name.
    my $workOrder = GetWorkOrderName();

    return 0 unless defined $workOrder;

    # Get the job ID and type from the work order name.
    my ( $jobID, $jobType ) = S4PM::S4PMOD::ParseWorkOrderName( $workOrder );

    # Define a cleanup workorder if configuration items exist.
    my $cleanupWorkOrder;
    if ( defined $arg->{CLEANUP} and $arg->{CLEANUP} ) {
	S4P::read_config_file( $arg->{s} ) if ( defined $arg->{s} );
	S4P::read_config_file( $arg->{f} ) if ( defined $arg->{f} );

	$cleanupWorkOrder = $S4PM::S4PMOD::cfg_root 
			    . '/' . $S4PM::S4PMOD::cfg_cleanup{$jobType}
	    if ( defined $S4PM::S4PMOD::cfg_root 
		 and defined $S4PM::S4PMOD::cfg_cleanup{$jobType} );

	# If work order name for cleanup is defined, make sure it has a '.wo'
	# suffix.
	if ( defined $cleanupWorkOrder ) {
	    $cleanupWorkOrder .= "/DO.SWEEP.$jobID.wo";
	} else {
	    # If work order name can not be constructed, return false.
	    S4P::logger( 'WARN', 
			 "Failed to construct work order for cleanup station" );
	    return 0;
	}
    }

    my $status;
    if ( defined $cleanupWorkOrder ) {
	$status = copy( $workOrder, $cleanupWorkOrder );
	S4P::logger( 'WARN', "Failed to copy $workOrder to $cleanupWorkOrder" )
	    unless $status;
	S4P::remove_job() if $status;
    } else {
	S4P::remove_job();
	$status = 1;
    }
    return $status;
}

################################################################################
# ^NAME: 
#   TryAgain
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   TryAgain( <hash reference containing arguments> )
#
# ^RETURN VALUE:
#   1=successful, 0=failure
#
# ^ALGORITHM:
#   1) Resubmit the job and remove the old job from the queue.  
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub TryAgain
{
    my ( $arg ) = @_;
    
    # Resubmit the job.
    S4P::remove_job() if S4P::restart_job();
}
################################################################################
# ^NAME: 
#   GetWorkOrderName
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   GetWorkOrderName( <hash reference containing arguments> )
#
# ^RETURN VALUE:
#   1=successful, 0=failure
#
# ^ALGORITHM:
#   1) Gets the work order name by listing files in the current directory.
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub GetWorkOrderName
{
    my ( $dirName ) = @_;

    # Set the default directory to be current directory.
    $dirName = '.' unless defined $dirName;

    my $workOrderName;
    # Open the directory and look for a file beginning with 'DO.'.
    local ( *DH );
    if ( opendir( DH, $dirName ) ) {
	my @files = grep /^DO\./, readdir( DH );
        my $status = 0;
        if ( @files > 1 ) {
	    S4P::logger( 'WARN', 
			 "Directory contains more than one file beginning "
                         . "with 'DO'" );
        } elsif ( @files == 1 ) {
	    $workOrderName = $files[0];
        } else {
	    S4P::logger( 'WARN', 
                         "Directory does not contain any work order" );
        }
        closedir( DH );
    } else {
	S4P::logger( 'WARN', "Failed to open directory $dirName" );
    }

    return ( defined $workOrderName ) ? $workOrderName : undef;
}
################################################################################
# ^NAME: 
#   RestartAll
#
# ^PROJECT:
#   GSFC V0 DAAC
#
# ^SYNOPSIS:
#   RestartAll( <hash reference containing arguments> )
#
# ^RETURN VALUE:
#   1=successful, 0=failure
#
# ^ALGORITHM:
#   1) Restart all failed jobs by executing S4P::restart_job() for each failed
# job. 
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  
################################################################################
sub RestartAll
{
    my ( $arg ) = @_;
    
    my $curDir = cwd();

    # On failure to get the current directory, return false.
    unless ( defined $curDir ) {
	warn "Failed to get the current directory";
	return 0;
    }

    local ( *UNIT );
    # Try to open the directory for reading; on failure, return false.
    unless ( opendir( UNIT, $curDir ) ) {
	warn "Failed to open $curDir for reading";
	return 0;
    }

    # Get a listing of all failed directories.
    my @dirList = grep /FAILED/, readdir( UNIT );
    my $jobCount = @dirList;
    my $count = 0;
    foreach my $dir ( @dirList ) {
	# For each failed job, chdir to its directory and restart the job.
	if ( chdir $dir ) {
	    # Remove the job (directory) if restart attempt is successful.	     
            if ( S4P::restart_job() ) {
                S4P::remove_job();
                $count++;
            }
	    # Change back to the original directory.
	    chdir $curDir;
	} else {
	    warn "Failed to change directory to $curDir";
	}
    }
    closedir( UNIT );
    return 1;
}

###############################################################################
# ^NAME: write_config
#
# ^PROJECT:
#        GSFC V0 DAAC
#
# ^SYNOPSIS:
#        $status = write_config( <file name>, <config hash> );
#
# ^RETURN VALUE:
#        0/1/
#
# ^ALGORITHM:
#        This sub was originally part of GenConfig.pm.   
#
# ^FILES:
#
# ^INTERNALS CALLED:
#
# ^EXTERNALS CALLED:
#        none
#
# ^DEPENDENCIES:
#
# ^AUTHOR: M. Hegde
#
# ^CREATED:  1/29/2002
###############################################################################

sub write_config
{
    my ( %arg ) = @_;

    # Convert all keys to upper case
    foreach my $key ( keys(%arg) ) {
        $arg{uc($key)} = $arg{$key};
        delete $arg{$key} if ( uc($key) ne $key );
    }
    
    # Open the file for writing.   
    local ( *UNIT );  
    unless ( open( UNIT, ">$arg{FILE}" ) ) {
        warn "Failed to open $arg{FILE} for writing";
        return 0;
    }

    my $config = $arg{CONFIG};    
    if ( defined $arg{KEY} ) {
        # Case of configuration where primary key is defined.
        foreach my $primeKey ( keys %$config ) {
            print UNIT "{\n";
            print UNIT " $arg{KEY} = $primeKey\n";
            foreach my $secKey ( keys %{$config->{$primeKey}} ) {
                print UNIT " $secKey = $config->{$primeKey}{$secKey}\n";
            }            
            print UNIT "}\n";
        }
    } else {
        if ( defined $config->{0} ) {
            # Case of configuration without a primary key defined, but with
            # multiple records (blocks enclosed in a pair of braces).
            for ( my $i = 0 ; $i < keys(%$config) ; $i++ ) {
                last unless defined $config->{0};
                print UNIT "{\n";
                foreach my $key ( keys %{$config->{$i}} ) {
                    print UNIT "$key = $config->{$i}{$key}\n";
                }
                print UNIT "}\n";
            }
        } else {
            # Case of configuration without a primary key and without multiple
            # records (blocks enclosed within a pair of braces), but contains
            # set of items in "key=value" format.
            foreach my $key ( keys %$config ) {
                print UNIT "$key = $config->{$key}\n";
            }
        }
    }
    
    close( UNIT );
    return 1;
}
1
