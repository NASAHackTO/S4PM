#!/usr/bin/perl

=head1 NAME

s4pm_receive_pan.pl -  process PAN and PDRD 

=head1 SYNOPSIS

s4pm_receive_pan.pl 
[B<-c> I<config_file>] 
[B<-h>] 
[B<-e> I<expiration>] 
[B<-p> I<on|off>] 
B<-d> I<pdr_dir>
[B<-o> I<suffix>]
PAN_file
   

=head1 ARGUMENTS

=over

=item B<-c> I<config_file>

Configuration file; default is ../s4pm_receive_pan.cfg.

=item B<-h>

Print usage.

=item B<-e> I<expiration_time>

Expiration time for fixed PDR work orders (generated from Long PANs).  
This should be specified as Nunits, e.g. 2days.  
Valid units are seconds, minutes, hours,
days, weeks, months and years.

The default is 3days.

=item B<-p> I<on|off>

Switch to determine whether or not to propagate PH or not.  The default
is "on" meaning PH will be propagated.  Supplying a value of "off"
turns off PH export.

=item B<-o> I<suffix>

Copy PDR to output work order with specified suffix.
This is needed to send output PDRs to the track_requests station for
S4PM-OnDemand. 

=head1 DESCRIPTION

This program processes Product Acceptance Notices (PANs) and
and Product Delivery Record Discrepancies (PDRD).  
The handling is dependent on the status obtained from the PAN or PDRD:

=head2 Successful Short PAN

If all of the files ingest successfully, a very terse PAN is received.
In this case, the code looks for PDR in the set by -d on the command line.
If it is a browse or production history PDR, it deletes the files referenced 
in the PDR.

If it is a data-type PDR (i.e., not browse or production history), it
makes an UPDATE work order for the data granules in the PDR.
It also looks in the PDR limbo directory for matching browse or production
history PDRs (which actually have the form of work orders) and copies them 
into the current directory, removing the DO. extension.
These files and their associated logs will be moved downstream to the Export
station when the program exits successfully.

=head2 Short PDRD, Long PDRD

These are caused by problems in the Product Delivery Records themselves.
This script will move the PDR from the PDR directory into the limbo
directory, renaming it to prepare for resubmission once the
underlying problem has been corrected.  The PDR for the Short PDRD case
will have the form: FIXPDR.jobid.D.wo, and the PDR for the Long PDRD case
will be FIXPDR.jobid.X.wo.

A companion failure handler is available to move these FIXPDR work orders
downstream to the export work station at the operator's discretion.

=head2 Short PAN Failure

This happens when all of the files fail ingest for the same reason.  It
is handled the same way as the PDRDs (see above), except that the pattern
is FIXPDR.jobid.S.wo.

=head2 Long PAN Failure

In a long PAN, some of the files are successful and others are failures.
For this case, the program notes all of the file_groups that were completely
successful and puts them in an UPDATE work order.  The file_groups that were
not completely successful (i.e., had at least one constituent file fail)
are bundled into a FIXPDR.jobid.L.wo file.

The companion failure handler moves both the UPDATE and the FIXPDR work orders
downstream to the respective stations at the operator's discretion.

=head1 FILES

s4pm_receive_pan.pl uses a configuration file (../s4pm_receive_pan.cfg by 
default), with the following information:

=over 4

=item $cfg_save_location

Location to place matched PAN/PDRD ($cfg_save_location)

=item $main::cfg_pdr_limbo

Limbo" location where PH (production history) and BR (Browse) PDRs
live until a successful PAN comes in.  When this happens, they are moved to
the directory specified with the -d argument.

=back

=head1 SEE ALSO

Further details ragarding PDR/PAN/PDRD, see ICD between
ECS-SIPS (423-41-57).

=head1 ALGORITHM

Read in configuration file 
Search for matching PDR
IF Matched PDR FOUND
   Parse PAN/PDRD
   CASE workorder is
      SHORT_SUCCESS_PAN:
         Create UPDATE workorder for all files or delete (if prodhist or browse)
      LONG PAN:
         Extract successful files
         Create UPDATE workorder for successful files
         Log in information related to successful files
         Extract failed files
         Log in information related to failed files
         Create PDR for the failed files
      SHORT FAILED PAN:
         Log in the error
         Create PDR work order for the failed files
      PDRD:
         Log in the error
         Create PDR for the failed PDRD
   ENDCASE
   Rename PDR to work order for Sink Station 
ELSE
   Log in the error
   copy workorder to unmatch_workorder
ENDIF

=head1 DIAGNOSTICS

Exits 0 for successful SHORTPANs.  

Exits non-zero for all other dispositions.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 610.2 and Michael Ta, NASA/GSFC-Raytheon ITSS, Code 902

=head1 MODIFICATION

08/24/2000     First version

=cut

################################################################################
# s4pm_receive_pan.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

# Declare packages
use strict;
use Getopt::Std;
use Cwd;
use File::Copy;
require 5.6.0;
use File::Basename;
use S4P;
use S4P::PAN;
use S4P::PDR;
use Safe;
use vars qw($opt_c $opt_e $opt_h $opt_p $opt_d);
use vars qw($cfg_save_location $cfg_no_matching_pdr_err $cfg_expiration 
$cfg_pdr_limbo);

# Parse command arguments
getopts('c:e:hp:d:');
my $pan_file = shift @ARGV;

unless ( $opt_d ) {
    S4P::perish(10, "No PDR directory specified with the -d argument.");
}

if ($opt_h || (!$pan_file)) {
   print STDERR "\nUsage: s4pm_receive_pan.pl ";
   print STDERR "[-c config_file] [-h] PAN_file\n\n\tNOTE\n";
   print STDERR "\tIf config file is not given, ../s4pm_receive_pan.cfg will be used\n\n";
   exit 100;
}

# Apply configuration file stuff
my $config_file = $opt_c || "../s4pm_receive_pan.cfg";
configure($config_file);
my $expiration = $opt_e || $main::cfg_expiration || "3days";
my $PHprop = (defined($opt_p)) ? $opt_p : "on" ;

my $workorder = $pan_file;
$workorder =~ s/\.(PAN|PDRD)//;
$workorder =~ s/^DO\.//;
S4P::logger('INFO',"main: Work order: $workorder\n");

# Find matching PDR
my $pdr_path = "$opt_d/$workorder.PDR";

if (! -e $pdr_path ) {
   S4P::perish(1, "main: No matching PDR found for $workorder.PAN in $opt_d");
}
S4P::logger('INFO',"main: Found matching PDR ($pdr_path) for $pan_file\n");

# Process PAN/PDRD
my ($disposition, $file, $file_list, $no_file_grps);
my (@update_file_list, @pdr_file_list, %unlink_hash);
my $pan_text = S4P::read_file($pan_file);
S4P::perish(101, "No text in PAN/PDRD") unless ($pan_text);
my $pan = new S4P::PAN($pan_text);
my $type = $pan->msg_type();

# retrieve files from PDR
S4P::logger('INFO',"main: Received $type = $pan_file\n");
my $pdr = new S4P::PDR('text' => S4P::read_file($pdr_path)) or
   S4P::perish(200, "Couldn't create PDR object from $pdr_path, exiting\n");

if ($type eq "SHORTPAN") {
    exit(2) if (! short_pan($pan, $pdr, $pdr_path, $pan_file, $workorder, $PHprop));
}  # End of If SHORTPAN

elsif ($type eq "LONGPAN") {

   S4P::job_message("LONGPAN");
   # Hash file groups on filenames to make matching easy
   my (%granules, $file_group, $file_spec, $file);
   foreach $file_group(@{$pdr->file_groups}) {
       foreach $file_spec(@{$file_group->file_specs}) {
           $granules{$file_spec->pathname} = $file_group;
       }
   }

   # Get the unique failed granules:  granule for each failed file
   my (%failed_granules);
   foreach $file(@{$pan->fail_file_list}) {
       $file =~ s#//*#/#g;
       S4P::perish(20, "Cannot find PAN file $file in PDR $pdr_path") 
           unless $granules{$file};
       $failed_granules{$granules{$file}} = $granules{$file};
   }

   # Put the failed granules into a list for the output PDR
   my @failed_granules = values %failed_granules;
   S4P::logger('ERROR', "Found " . scalar(@failed_granules) . " failed granules");
   
   # Make an Output PDR with the failed granules
   my $output_pdr = new S4P::PDR;
   $output_pdr->originating_system($pdr->originating_system);
   $expiration =~ m/(\d+)\s*(\w+)/;
   my $exp_number = $1;
   my $exp_units = $2;
   $output_pdr->expiration_time(S4P::PDR::get_exp_time($exp_number, $exp_units));
   my $output_file = output_pdr_name(basename($pdr_path), 'L');
   $output_pdr->file_groups(\@failed_granules);
   S4P::logger('ERROR', "Writing failed granules to PDR $output_file");
   $output_pdr->write_pdr($output_file);
   symlink($output_file, basename($output_file)) or
       S4P::perish(2, "Failed to symlink to $output_file: $!");
   
   # SUCCESSFUL GRANULES
   my (%successful_granules);
   foreach $file(@{$pan->success_file_list}) {
       $file =~ s#//*#/#g;
       S4P::perish(20, "Cannot find PAN file $file in PDR $pdr_path") 
           unless $granules{$file};
       $successful_granules{$granules{$file}} = $granules{$file} 
           unless exists $failed_granules{$granules{$file}};
   }
   my (@update_files, @unlink_files);
   foreach $file_group(values %successful_granules) {
       my (@unlink, @update);
       foreach $file_spec(@{$file_group->file_specs}) {
           my $ftype = $file_spec->file_type || $file_spec->guess_file_type;
           my $pathname = $file_spec->pathname;

           # Metadata file doesn't tell us anything; can have them with either
           if ($ftype eq 'METADATA') {
               push @unlink, $pathname; # Adding on spec...
               # Also add the real path if it is a symlink
               push (@unlink, readlink($pathname)) if (-l $pathname);
               next;
           }

           # We generate UPDATE only for SCIENCE FILE_GROUPs
           # Anything other than METADATA or SCIENCE means it's a non-SCIENCE
           # PRODHIST is kept around; other non-science is unlinked
           if ($ftype eq 'SCIENCE') {
               if (-l $pathname) {
                   push @unlink, $pathname;
                   $pathname = readlink($pathname);
               }
               push(@update, $pathname);
           }
           elsif ($ftype ne 'PRODHIST') {
               push(@unlink, $pathname);
           }
       }
       if (@update) {
           push(@update_files, @update);
       }
       else {
           push(@unlink_files, @unlink);
       }
   }

   # Generate UPDATE and PDR, if needed
   gen_update(\@update_files, $workorder) if @update_files;
   foreach (@unlink_files) {
       unlink($_) ? S4P::logger('INFO', "Unlinking $_") 
                  : S4P::perish(110, "Cannot unlink $_: $!");
   }
   move($pdr_path, '.') or S4P::perish(2, "Failed to move $pdr_path to .: $!");
   exit(10);

}  # End of If LONGPAN

elsif ($type eq "SHORTPDRD") {
    S4P::job_message("SHORTPDRD");
    $disposition = $pan->disposition();
    $disposition =~ s/\"//g;
    $disposition =~ s/ /_/g;
    my $new_path = output_pdr_name(basename($pdr_path), 'D');
    move($pdr_path, $new_path) or 
        S4P::perish(2, "Failed to move $pdr_path to $new_path: $!");
    symlink($new_path, basename($new_path)) or
        S4P::perish(2, "Failed to symlink to $new_path: $!");
    exit(11);
}  # End of If SHORTPDRD

elsif ($type eq "LONGPDRD") {
    S4P::job_message("LONGPDRD");
    my $new_path = output_pdr_name(basename($pdr_path), 'X');
    move($pdr_path, $new_path) or 
        S4P::perish(2, "Failed to move $pdr_path to $new_path: $!");
    symlink($new_path, basename($new_path)) or
        S4P::perish(2, "Failed to symlink to $new_path: $!");
    exit(12);
}  # End of If LONGPDRD

exit(0);

sub short_pan {
    my ($pan, $pdr, $pdr_path, $pan_file, $workorder, $PHprop) = @_;
    my $pdr_file = basename($pdr_path);
    my $disposition = $pan->disposition();
    $disposition =~ s/\"//g;
    my (@update_file_list, %unlink_hash);

    # If PAN was successful...
    if ( $disposition eq "SUCCESSFUL" ) {
       # If data, then release the PH and Browse that were in limbo
       if ($main::cfg_pdr_limbo && 
           $pan_file !~ /(PH|_BR)_\d{13}/ && 
           $pan_file !~ /FAILPGE/) 
       {
           my $file = $pan_file;
           # Strip off DO. prefix
           $file =~ s/^DO\.//;

           # Look for matching PH or BROWSE PDRs: *_YYYYDDDHHMMSS
           $file =~ m/.*?\.(.*?_\d{13})/;
           my $pdr_id = $1 or
               S4P::perish(11, "Limbo release failed, can't extract ID from $file");

           # It's OK to find no limbo matches for Browse...
           release_limbo_pdr($main::cfg_pdr_limbo, $pdr_id, '_BR', "on") or 
               S4P::logger('INFO', "Limbo release: no matching Browse PDR found for $pdr_id");

           # ...or no limbo matches for PH
           release_limbo_pdr($main::cfg_pdr_limbo, $pdr_id, 'PH', $PHprop) or 
               S4P::logger('INFO', "Limbo release failed: no matching PH PDR found for $pdr_id");
       }

       # Cleanup files that were successful
       foreach my $file_group(@{$pdr->file_groups}) {
           my $r_file_specs = $file_group->file_specs;
           foreach my $file_spec(@{$r_file_specs}) {
              my $file_type = $file_spec->file_type || $file_spec->guess_file_type;
              my $pathname = $file_spec->pathname;
              # Science files go in the UPDATE workorder for grancentral
              if ($file_type eq 'SCIENCE') {
                  if (-l $pathname) {
                      S4P::logger('INFO', "$pathname is a link, adding to unlink hash");
                      $unlink_hash{$pathname} = 1;
                      $pathname = readlink($pathname);
                  }
                  push(@update_file_list, $pathname);
              }
              # Production History files are kept around
              # Other non-science files are unlinked directly
              # Metadata is always handled as an adjunct file, never on its own
              elsif ($file_type ne 'PRODHIST' && $file_type ne 'METADATA') {
                  $unlink_hash{$pathname} = 1;
                  my $metfile = $pathname . ".met";
                  $unlink_hash{$metfile} = 1 if (-e $metfile);
              }
              elsif ($file_type eq 'METADATA' && (-l $pathname)) {
                  # Unlink the link; the actual met file will be unlinked by sweep_data
                  S4P::logger('INFO', "$pathname is a link, adding to unlink hash");
                  $unlink_hash{$pathname} = 1;
              }
           }
       }

       gen_update(\@update_file_list, $workorder) if @update_file_list;

       # Unlink non-SCIENCE files
       foreach (keys %unlink_hash) {
           unlink($_) ? S4P::logger('INFO', "Unlinking $_") 
                      : S4P::perish(110, "Cannot unlink $_: $!");
       }
           
       my $ret = copy($pdr_path,"$pdr_file.wo");
       move($pdr_path, $main::cfg_save_location) or 
           S4P::perish(12, "Failed to move $pdr_path to $main::cfg_save_location: $!");
       $ret = copy($pan_file,"$main::cfg_save_location/$workorder.PAN");
       return 1;
    }  
    else {
       S4P::job_message("SHORTPAN FAILURE: " . $disposition);
       my $new_path = output_pdr_name(basename($pdr_path), 'S');
       move($pdr_path, $new_path) or S4P::perish(2, "Failed to move $pdr_path to $new_path: $!");
       symlink($new_path, basename($new_path)) or 
           S4P::perish(2, "Failed to symlink to $new_path: $!");
       return 0;
    }  
}
###########################################################################
# Generate output work order name for fixer-upper PDRs
# Use .wo suffix to be consistent with other output work orders from this
# station.
# Format is FIXPDR.<job_id>.<pid>.wo, or FIXPDR_PH.<job_id>.<pid>.wo
# - Chris Lynnes
sub output_pdr_name {
    my ($original, $extra) = @_;

    # Strip suffix off
    $original =~ s/\.PDR//;

    # Strip prefix off
    my ($prefix, $job_id) = split('\.', $original, 2);
    my $job_type = 'FIXPDR';
    $job_id .= ".$extra" if $extra;
    return sprintf("%s/%s.%s.wo", $main::cfg_pdr_limbo, $job_type, $job_id);
}

################################################################################
# This routine generates UPDATE work order
################################################################################
sub gen_update {
   my ($r_file_list, $workorder) = @_;
   my $id = $workorder;
   $id =~ s/S4PM\.//;
   my $update_name = "UPDATE.RXPAN_$id.wo";
   open(DEST, "> $update_name" ) or 
       S4P::perish(203, "unable to open $update_name file for writing"); 
   foreach my $file (@{$r_file_list}) {
       S4P::logger('INFO', "Adding $file to UPDATE work order");
       printf DEST "FileId=%s  Uses=-1\n", $file;
   }
   close(DEST);
}	# End of gen_update

################################################################################
# This routine reads in configuration file
# The logic/code was based from run_pge.pl
################################################################################
sub configure {
   my $file = shift;
   if ($file) {
      # Read in configuration file
      my $compartment = new Safe "CFG";
      $compartment->share('$cfg_expiration');
      $compartment->share('$cfg_pdr_limbo');
      $compartment->share('$cfg_no_matching_pdr_err');
      $compartment->share('$cfg_save_location');
      $compartment->rdo($file) or
          S4P::perish(205, "Cannot read config file $file in safe mode: $!\n");
      $main::cfg_pdr_limbo = $CFG::cfg_pdr_limbo if $CFG::cfg_pdr_limbo;
      $main::cfg_no_matching_pdr_err=$CFG::cfg_no_matching_pdr_err if $CFG::cfg_no_matching_pdr_err;
      $main::cfg_save_location = $CFG::cfg_save_location if $CFG::cfg_save_location;
    }
}	# End of configure
sub release_limbo_pdr {
    my ($limbo, $pdr_id, $type_id, $release_flag) = @_;

    # Check limbo location
    if (! $limbo || ! -d $limbo) {
        S4P::perish(21, "Bad limbo location: $limbo");
    }

    # Split apart the ID
    $pdr_id =~ m/(.*?)(_\d{13})/;

    # Formulate glob pattern
    my $pattern = $limbo . '/*' . $1 . $type_id . $2 . '*';
    my @files = glob($pattern);
    my @pdr = grep !/\.log$/, @files;
    
    my $n_pdr = scalar(@pdr);

    # Zero matching PDRs is sometimes OK (e.g., Browse)
    # Multiple matching PDRs is not OK
    if ($n_pdr > 1) {
        S4P::perish(22, "More than one limbo match for $pattern: $n_pdr matches");
    } 
    elsif ($n_pdr == 0) {
        return 0;
    }

    my @logs = grep /\.log$/, @files;

    # Move PDR and associated log from limbo to the current directory
    # It will then act as an output work order
    my $filename = basename($pdr[0]);
    # Need to strip starting DO. so it looks like an output work order
    $filename =~ s/^DO\.//;
    
    my $dest = "./$filename";
    if ($release_flag eq "off") {
        $dest = "/dev/null";
    }
    
    if (!move($pdr[0], $dest)) {
        S4P::perish(23, "Failed to move $pdr[0] to $dest: $!");
    }
    else {
        S4P::logger('INFO', "Moved $pdr[0] from $limbo to $dest");
        if ($logs[0]) {
            if (unlink($logs[0])) {
               S4P::logger('INFO', "Unlinked matching log file $logs[0]");
            }
            else {
               S4P::logger('ERROR', "Failed to unlink log file $logs[0]: $!")
            }
        }
    }
    return 1;
}
