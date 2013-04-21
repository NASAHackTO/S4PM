=head1 NAME

Handles.pm - general routines to support S4PM data file handles

=head1 SYNOPSIS

use S4PM::Handles;

@files = S4PM::Handles::get_filenames_from_handle($handle);

$met = S4PM::Handles::get_metadata_from_handle($handle);

$xml = S4PM::Handles::get_xml_from_handle($handle);

$browse = S4PM::Handles::get_browse_from_handle($handle);

$ur = S4PM::Handles::get_ur_from_handle($handle);

@handles = S4PM::Handles::get_matching_handles($dir, $pattern);

$count = S4PM::Handles::matching_handle_count($dir, $pattern);

$res = S4PM::Handles::is_multi_file_granule($handle);

$size = S4PM::Handles::get_granule_size($handle);

S4PM::Handles::touch_granule($handle);

=head1 DESCRIPTION

=over 4

=item get_filenames_from_handle

This function takes a data file or a file handle as input and returns the 
corresponding full pathnames of the data files. If the handle is the old-style 
UR file, the function simply returns the name passed to it with the .ur 
truncated off. If the handle is a true handle file, the function returns a 
list comprised of lines 2 through the next to the last line (the last line is 
the metadata file). If it's passed a data file, it simply returns back the
data file or, for multi-file granules, the list of data files within the
subdirectory.

=item get_metfile_from_handle

This function returns the full pathname of the ODL metadata file corresponding 
to the data file or handle file passed in. It works for both old- and new-style 
UR files as well as data files.

=item get_xml_from_handle

This function returns the full pathname of the XML metadata file corresponding 
to the data file or handle file passed in or undef if there is no XML file. It 
works for both old- and new-style UR files as well as data files.

=item get_browse_from_handle

This function returns the full pathname of the browse file corresponding to
the data file or handle file passed in or undef if there is no browse. It 
works for both old- and new-style UR files as well as data files.

=item get_ur_from_handle

This function returns the UR from the data file handle, which is assumed to be
the first line in the data handle file.

=item get_matching_handles

Given a directory and file pattern, this function returns an array of matching
data file handle, new or old-style.

=item matching_handle_count

Given a directory and file pattern, this function returns the number of 
data file handles matching the pattern.

=item is_multi_file_granule

Returns 1 if the granule associated with the data file or data file handle 
passed in contains more than 1 data file; 0 otherwise.

=item get_granule_size

When given a data file or data file handle, this function returns the size of 
the granule in bytes when passed a file handle or a data file (single or multi).

=item touch_granule

When given a data file or data file handle, this function simply runs 'touch' 
on all components of the granule including the file handle itself. It works 
with both old- and new-style data file handles.

=back

=head1 AUTHOR

Stephen Berrick, SSAI, NASA/GSFC, Code 610.2

=cut
################################################################################
# SccsId: 2006/08/07 12:22:19, 1.107
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::Handles;
use strict;
use Safe;
use S4P;
use File::Basename;

1;

sub get_filenames_from_handle {

    my $handle = shift;
    S4P::logger("INFO", "get_filenames_from_handle(): handle: [$handle]");
    my @suffixlist = ('ur');
    my @filenames = ();
    my @tmp = ();

    my (undef, undef, $ext) = fileparse($handle, @suffixlist);
    if ( $ext eq "ur" ) {         # We are dealing with some sort of UR file

        unless ( open(HANDLE, $handle) ) {
            S4P::logger("FATAL", "get_filenames_from_handle(): Failed to open handle file: [$handle]: $!");
            return undef;
        }
        while ( <HANDLE> ) {
            chomp;
            push(@tmp, $_);
        }
        my @files = splice(@tmp, 1);    # Remove first line which is the UR
        close(HANDLE) or S4P::perish(10, "get_filenames_from_handle(): Failed to close handle file: [$handle]: $!");

        if  ( scalar(@files) == 0 ) {    # Old-style UR
            $filenames[0] = $handle;
            $filenames[0] =~ s/\.ur$//;
            if ( -d $filenames[0] ) {
                @filenames = glob("$filenames[0]/*");
            }
        } else {
            foreach my $f ( @files ) {
                if ( $f =~ /DATA\s*=\s*(.*)$/ ) {
                    push(@filenames, $1);
                }
            }
        }

    } else {       # We were given a data file instead

        if ( -d $handle ) {          # Data file is actually a directory
            @filenames = glob("$handle/*");
        } else {                     # Data file is truly a file
            $filenames[0] = $handle;
        }

    }

        return @filenames;
}

sub get_browse_from_handle {

    my $handle = shift;
    S4P::logger("INFO", "get_browse_from_handle(): handle: [$handle]");
    my $browse = undef;
    my @suffixlist = ('ur');
    my (undef, undef, $ext) = fileparse($handle, @suffixlist);

    if ( $ext eq "ur" ) {         # We are dealing with some sort of UR file
        unless ( open(HANDLE, $handle) ) {
            S4P::perish(30, "get_browse_from_handle(): Failed to open handle file: [$handle]: $!");
            return undef;
        }
        while ( <HANDLE> ) {
            chomp;
            if ( /BROWSE\s*=\s*(.*)$/ ) {
                $browse = $1;
                last;
            }
        }
        close(HANDLE) or S4P::perish(10, "get_browse_from_handle(): Failed to close handle file: [$handle]: $!");

    }

    return $browse;
}

sub get_xml_from_handle {

    my $handle = shift;
    S4P::logger("INFO", "get_xml_from_handle(): handle: [$handle]");
    my @suffixlist = ('ur');
    my @filenames = ();
    my $xmlfile = undef;

    my (undef, undef, $ext) = fileparse($handle, @suffixlist);
    if ( $ext eq "ur" ) {         # We are dealing with some sort of UR file

        unless ( open(HANDLE, $handle) ) {
            S4P::logger("FATAL", "get_xml_from_handle(): Failed to open handle file: [$handle]: $!");
            return undef;
        }
        my @tmp = ();
        while ( <HANDLE> ) {
            chomp;
            push(@tmp, $_);
        }
        my $ur_string = $tmp[0];
        @filenames = splice(@tmp, 1);    # Remove first line which is the UR

        close(HANDLE) or S4P::perish(10, "get_xml_from_handle(): Failed to close handle file: [$handle]: $!");

        if ( scalar(@filenames) == 0 ) {
            $xmlfile = $handle;
            $xmlfile =~ s/\.ur$/\.xml/;
            unless (-e $xmlfile) {   # something else to try
                $xmlfile = basename($ur_string);
                $xmlfile .= ".xml";
            }
        } else {
            foreach my $f ( @filenames ) {
                if ( $f =~ /XML\s*=\s*(.*)$/ ) {
                    $xmlfile = $1;
                    last;
                }
            }
            unless ( $xmlfile ) {
                S4P::logger("INFO", "get_xml_from_handle(): Failed to locate XML metadata file in handle file: [$handle]");
            }
        }

    } else {       # We were given a multi-file data directory instead

        S4P::logger("INFO", "get_xml_from_handle(): We have a multi-file handle.");
        if ( -e $handle . ".xml" ) {
            $xmlfile = $handle . ".xml";
        } else {
            my $ur_string = S4PM::Handles::get_ur_from_handle($handle);
            my $dirname = dirname($handle);
            $xmlfile = $dirname . "/" . basename($ur_string) . ".xml";
        }

    }

    return $xmlfile;
}

sub get_metadata_from_handle {

    my $handle = shift;
    S4P::logger("INFO", "get_metadata_from_handle(): handle: [$handle]");
    my @suffixlist = ('ur');
    my @filenames = ();
    my $metfile = undef;

    my (undef, undef, $ext) = fileparse($handle, @suffixlist);
    if ( $ext eq "ur" ) {         # We are dealing with some sort of UR file

        unless ( open(HANDLE, $handle) ) {
            S4P::logger("FATAL", "get_metadata_from_handle(): Failed to open handle file: [$handle]: $!");
            return undef;
        }
        my @tmp = ();
        while ( <HANDLE> ) {
            chomp;
            push(@tmp, $_);
        }
        @filenames = splice(@tmp, 1);    # Remove first line which is the UR

        close(HANDLE) or S4P::perish(10, "get_metadata_from_handle(): Failed to close handle file: [$handle]: $!");

        if ( scalar(@filenames) == 0 ) {
            $metfile = $handle;
            $metfile =~ s/\.ur$/\.met/;
        } else {
            foreach my $f ( @filenames ) {
                if ( $f =~ /MET\s*=\s*(.*)$/ ) {
                    $metfile = $1;
                    last;
                }
            }
            unless ( $metfile ) {
                S4P::perish(10, "get_metadata_from_handle(): Failed to locate metadata file in handle file: [$handle]");
            }
        }

    } else {       # We were given a multi-file data directory instead

        $metfile = $handle . ".met";

    }

    return $metfile;
}

sub get_ur_from_handle {

    my $handle = shift;
    S4P::logger("INFO", "get_ur_from_handle(): handle: [$handle]");

    my @suffixlist = ('ur');

    my (undef, undef, $ext) = fileparse($handle, @suffixlist);

    unless ( $ext eq 'ur' ) {     # Deal with data files (or directories)
        $handle .= ".ur";
    }

    unless ( open(HANDLE, $handle) ) {
        S4P::logger("FATAL", "get_ur_from_handle(): Failed to open handle file: [$handle]: $!");
        return undef;
    }
    
    my $urline = <HANDLE>;
    chomp($urline);
    close(HANDLE) or S4P::perish(10, "get_metadata_from_handle(): Failed to close handle file: [$handle]: $!");

    return $urline;
}

sub get_matching_handles {

    my $dir     = shift;
    my $pattern = shift;
    S4P::logger("INFO", "get_matching_handles(): dir: [$dir], pattern: [$pattern]");

    my @handles = glob("$dir/$pattern");
    my %handles = ();
    map { $handles{$_} = 1 } @handles;

    foreach my $key ( keys %handles ) {
        my @files = S4PM::Handles::get_filenames_from_handle($key);
        foreach my $file ( @files ) {
            unless ( -e $file ) {
                S4P::logger("ERROR", "get_matching_handles(): File [$file] doesn't exist for file handle [$key]. Dropping matched item from list.");
                delete $handles{$key};
                last;
            }
        }
        my $met   = S4PM::Handles::get_metadata_from_handle($key);
        unless ( -e $met ) {
            S4P::logger("ERROR", "get_matching_handles(): Metadata file [$met] doesn't exist for file handle [$key]. Dropping matched item from list.");
            delete $handles{$key};
        }
    }

    return keys %handles;
        
}

sub matching_handle_count {

    my $dir     = shift;
    my $pattern = shift;
    S4P::logger("INFO", "matching_handle_count(): dir: [$dir], pattern: [$pattern]");
    my @files = S4PM::Handles::get_matching_handles($dir, $pattern);
    return scalar(@files);
}

sub is_multi_file_granule {

    my $handle = shift;
    S4P::logger("INFO", "is_multi_file_granule(): handle: [$handle]");

    my @files = S4PM::Handles::get_filenames_from_handle($handle);

    my $count = scalar(@files);
    return ( $count > 1 ) ? 1 : 0;
}

sub get_granule_size {

    my $handle = shift;

    my $size = 0;

    my @files = S4PM::Handles::get_filenames_from_handle($handle);
    foreach my $file ( @files ) {
        $size += (-s $file);
    }

    return $size;
}

sub touch_granule {

    my $handle = shift;

    S4P::logger("INFO", "touch_granule(): handle: [$handle]");

    my @suffixlist = ('ur');
    my (undef, undef, $ext) = fileparse($handle, @suffixlist);

### First, touch the handle itself

    my $cmd = "touch $handle";
    S4P::logger("touch_granule(): Running this command: [$cmd]");
    S4P::exec_system($cmd);

### Now, touch associated data and metadata files

    my @files = S4PM::Handles::get_filenames_from_handle($handle);
    foreach my $f ( @files ) {
        $cmd = "touch $f";
        S4P::logger("touch_granule(): Running this command: [$cmd]");
        S4P::exec_system($cmd);
    }
    my $met = S4PM::Handles::get_metadata_from_handle($handle);
    $cmd = "touch $met";
    S4P::logger("touch_granule(): Running this command: [$cmd]");
    S4P::exec_system($cmd);

    my $xml = $met;
    $xml =~ s/met$/xml/;
    if ( -e $xml ) {
        $cmd = "touch $xml";
        S4P::logger("touch_granule(): Running this command: [$cmd]");
        S4P::exec_system($cmd);
    }

}
