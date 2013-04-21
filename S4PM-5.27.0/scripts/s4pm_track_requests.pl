#!/usr/bin/perl

=head1 NAME

s4pm_track_requests.pl - track incoming requests and release to distribution

=head1 SYNOPSIS

s4pm_track_requests.pl
B<-f> I<config_file>
B<-n> 
B<-t>
B<-q>
B<-g>
B<-r>
B<-x>
B<-c>
B<-e>
B<-i>
I<workorder>

=head1 DESCRIPTION

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

Allows specification of a config file to override certain default parameters.

=item B<-n>

Specifies that the work order argument is a TRACK_REQUESTS work order. The 
routine will then do some initialization (setting up directories, parsing the 
work order), and then enter a sleep/wake cycle looking for evidence that the 
request has completed (or failed).

=item B<-t>

Specifies that the work order argument is a PREPARE work order from Find 
Data.

=item B<-q>

Specifies that the work order argument is a REQUEST_DATA work order from 
Request Data.

=item B<-g>

Specifies that the work order argument is a ALLOCATE work order from 
Prepare Run.

=item B<-r>

Specifies that the work order argument is a RUN work order from Allocate Disk.

=item B<-x>

Specifies that the work order argument is a EXPORT work order from Run 
Algorithm.

=item B<-c>

Specifies that the work order argument is a CLOSED work order from Ship Data
or Export.

=item B<-i>

Specifies that the work order argument is a IGNORE_FAILURE work order from the 
error handler. Tidies up in order to produce a partial request.

=item B<-e>

Specifies that the work order argument is a ORDER_FAILURE work order from one 
of the intermediate stations.

=item I<work order>

The input work order filename.

=back

=head1 EXIT STATUS VALUES

=head2 Status = 0

Successfully completed processing specified work order.

=head2 Status = 1

Unsuccessful in processing input wokrorder.

=head2 Status other than 0 or 1.

An internal script error was encountered:

10,"Error creating directory $dir."
11,"Unlink failed for $file"
12,"Rmdir failed for $dir"
13,"Failure renaming file $file to $newfile"
14,"Opendir failed on directory $dir"
15,"Failure opening $file"
16,"Failure opening $targetfile for write"
17,"E-Mail recipient list not defined"
20,"Config file $opt_f not read");
21,"Too many or too few options on command line: one and only one of -ntqgrxcei");
22,"Unable to read work order $workorder"
23,"Workorder $workorder has unrecognizeable request ID"
24,"Workorder $workorder has unrecognizeable partnumber"
25,"Failure copying $workorder to $newfile"
26,"Error in copying input work order $workorder to ORDER_FAILURE log $tmpfile."
30,"Error detected in intervening processing.  Operator must invoke error handler."
70,"Recycling work order $workorder"

=head1 AUTHOR

Mike Theobald

=cut

################################################################################
# s4pm_track_requests.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

# Track Requests station

# Main section branches to work order handler subroutines
# depending on type of work order -- type determined by
# work order name.
# Version 0.1 12/11/2003 Mike Theobald - Initial design
# Version 0.2 12/12/2003 Mike Theobald - Revised to account for stationmaster handling

# exit with status of work order handler
# exit with FAILURE if work order type unknown

use strict ;
use vars qw($opt_f $opt_n $opt_t $opt_q $opt_g $opt_r $opt_x $opt_c $opt_i $opt_e) ;
use Getopt::Std ;
use S4P;
use S4P::TimeTools ;
use S4P::PDR ;
use S4P::FileGroup ;
use S4P::FileSpec ;
use File::Copy;
use File::Basename;
require 5.6.0 ;
use Safe ;

my $configfile ;
my $status = 0;
my $trackdirtop = "../ACTIVE_REQUESTS" ;
my $archivedir = "../../ARCHIVE/logs/track_requests" ;
my $ordertype = undef ;
my %state ;

# get command line options
#  -f config file (containing sleep period)
#  -n TRACK_REQUEST work order
#  -q REQUEST_DATA work order
#  -t PREPARE work order
#  -g ALLOCATE work order
#  -r RUN work order
#  -x EXPORT work order
#  -c CLOSE work order
#  -e ORDER_FAILURE work order
#  -i IGNORE_FAILURE work order

getopts('f:nqtgrxcei') ;
my $workorder = shift ;

if ($opt_f) {
    my $compartment = new Safe 'CFG';
    $compartment->share('$notifylist');
    $compartment->rdo($opt_f) or
        S4P::perish(20, "main: Failed to read configuration file $opt_f in safe mode: $!");
}

%state = (
    'new_request'              => "A.waiting_for_REQUEST_DATA" ,
    'waiting_for_data'         => "B.waiting_for_PREPARE" ,
    'data_complete'            => "C.waiting_for_ALLOCATE" ,
    'waiting_for_disk'         => "D.waiting_for_RUN" ,
    'waiting_for_service'      => "E.waiting_for_EXPORT" ,
    'service_complete'         => "F.service_complete" ,
    'waiting_for_distribution' => "G.waiting_for_CLOSE" ,
    'distribution_complete'    => "H.distribution_complete" ,
    'order_failure'            => "Z.order_failure" ,
) ;

#  exit with error if not exactly one of (-n -t -q -g -r -x -c -e -i) specified
if ( ($opt_n + $opt_t + $opt_q + $opt_g + $opt_r + $opt_x + $opt_c + $opt_e + $opt_i) != 1 ) {
    S4P::perish(21, "Too many or too few options on command line: one and only one of -ntqgrxcei");
}

#  check to make sure work order exists and is readable
unless (-r $workorder) {
    S4P::perish(22, "Unable to read work order $workorder") ;
}

S4P::logger("INFO", "Beginning to process work order $workorder") ;

my ($trackID,$partno) = get_trackID($workorder) ;
my $trackdir = "$trackdirtop/$trackID" ;

foreach (keys(%state)) { $state{$_} = $trackdir."/".$state{$_} ; }

if ($opt_n) {
    $status = new_request($workorder) ;
}

#   error if running request_ID directory doesn't exist

unless (-d "$trackdir") {
    S4P::logger("INFO","Active request dir $trackdir doesn't exist.  Recycling this work order $workorder") ;
    exit(recycle($workorder)) ;
}

if ($opt_q) {
  $status = waiting_for_data($workorder) ;
}

if ($opt_t) {
  $status = data_complete($workorder) ;
}

if ($opt_g) {
  $status = waiting_for_disk($workorder) ;
}

if ($opt_r) {
  $status = waiting_for_service($workorder) ;
}

if ($opt_x) {
  $status = service_complete($workorder) ;
}

if ($opt_c) {
  $status = distribution_complete($workorder) ;
}

if ($opt_i) {
  $status = ignore_failure($workorder) ;
}

if ($opt_e) {
  $status = order_failure($workorder) ;
}

exit($status) ;

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub new_request {
  
    my $workorder = shift ;
    my %partinfo ;
    my @requestODL = () ;
    my $status = 0 ;

    my ($trackID,$partno) = get_trackID($workorder) ;

    mkdir($trackdirtop,0755) unless (-d $trackdirtop) ;
    my $trackdir = "$trackdirtop/$trackID" ;
    mkdir($trackdir,0755) or S4P::perish(10,"Error creating directory $trackdir.") ;

#   create state subdirectories in current directory
#   error on failure to create dirs
    foreach my $dir (values(%state)) {
        mkdir($dir,0755) or S4P::perish(10,"Error creating directory $dir.") ;
    }

#   read work order, and split specification of part members from request ODL
#   error on failure to read work order
    open(WORKORDER,"$workorder") or S4P::perish(15,"Error opening work order $workorder") ;

    while (<WORKORDER>) {
        if (/^(\d+):\s+(.+)/) {
            push @{$partinfo{$1}},$2 ;
        } else {
            push @requestODL,$_ ;
        }
    }

    close(WORKORDER) ;

#   error if no part numbers found
#   expect to recieve a ORDER_FAILURE from split_services in this case
    unless (keys(%partinfo)) {
        S4P::logger("WARNING", "No order parts in work order $workorder") ;
    }

#   error if no request ODL found
    unless (@requestODL) {
        S4P::logger("ERROR", "No order ODL found in work order $workorder") ;
        return(1) ;
    }

#   write request ODL to running request_ID directory
#   error if write fails

    my $odlfile = "$trackdir/$trackID.ODL.txt" ;
    open(ODL,">$odlfile") or S4P::perish(16,"Error creating odl file $odlfile") ;

    foreach (@requestODL) { print ODL ; }

    close(ODL) ;

#   create part number file named STATUS."request_ID"_"part_number".txt in new_request directory
#   create part number file named PENDING."request_ID"_"part_number".txt in service_complete directory
#   error if file creation failed

    my $now = S4P::TimeTools::CCSDSa_Now() ;

    foreach my $partno (keys(%partinfo)) {
        my $partnofile = "$state{new_request}/STATUS.${trackID}_${partno}.txt" ;
        my $timestamp = $now." ".$trackID."_".$partno." ".(split /\//,$state{new_request})[-1] ;
        open(PART,">$partnofile") or S4P::perish(16,"Error creating partnumber file $partnofile") ;
        print PART "$timestamp\n" ;
        close(PART)  ;

        my $partnofile = "$state{service_complete}/PENDING.${trackID}_${partno}.txt" ;
        open(PART,">$partnofile") or S4P::perish(16,"Error creating partnumber file $partnofile") ;
        foreach (@{$partinfo{$partno}}) { print PART ; print PART "\n" ; }
        close(PART)  ;
    }

    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub waiting_for_data {

    my $workorder = shift ;

#   move partnofile from "new_request" to "waiting_for_data"

    my $status = change_state($workorder,
                              $state{new_request},
                              "STATUS",
                              $state{waiting_for_data},
                              "STATUS") ;

    # If granule was already on the system from a previous order,
    # find_data may have already released.  In fact, the order could be
    # closed by the time the request_data gets in.
    # Thus, we do not recycle this work order.
    if ($status == 2) { 
        S4P::logger('INFO', "Request item has likely already passed waiting_for_data");
    }
    elsif ($status != 0) {
        S4P::logger('INFO', "Request has likely already finished");
    }
        
    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub data_complete {

    my $workorder = shift ;

#   move partnofile from "waiting_for_data" to "data_complete"

    my $status = change_state($workorder,
                              $state{waiting_for_data},
                              "STATUS",
                              $state{data_complete},
                              "STATUS") ;
    # If status change failed, see if we are waiting for request_data to do its
    # thing...
    unless ($status == 0) { 
        unless (change_state($workorder, $state{new_request}, "STATUS", $state{data_complete}, "STATUS") == 0) {
            return(recycle($workorder)) ; 
        }
    }
    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub waiting_for_disk {

    my $workorder = shift ;

#   move partnofile from "data_complete" to "waiting_for_disk"

    my $status = change_state($workorder,
                              $state{data_complete},
                              "STATUS",
                              $state{waiting_for_disk},
                              "STATUS") ;
    unless ($status == 0) { return(recycle($workorder)) ; }
    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub waiting_for_service {

    my $workorder = shift ;

#   move partnofile from "waiting_for_disk" to "waiting_for_service"

    my $status = change_state($workorder,
                              $state{waiting_for_disk},
                              "STATUS",
                              $state{waiting_for_service},
                              "STATUS") ;
    unless ($status == 0) { return(recycle($workorder)) ; }
    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub service_complete {

    my $workorder = shift ;

    my ($trackID,$partno) = get_trackID($workorder) ;
    my $jobID = $trackID."_".$partno ;

    if ($partno eq "") {
        S4P::perish(24,"Workorder $workorder has unrecognizeable partnumber") ;
    }

    my $file = "$state{service_complete}/READY.$jobID.txt" ;
    return(0) if (-e $file) ;

#   unlink part_number file in waiting_for_service subdirectory
#   error if part_number file doesn't exist
    unless (change_state($workorder,$state{waiting_for_service},"STATUS",$state{waiting_for_distribution},"STATUS") == 0 )  {
        return(recycle($workorder)) ;
    }

#   copy input work order to READY.$jobID.txt for eventual processing by create_output_workorder
    open(WORKORDER,"$workorder") or S4P::perish(15,"Error opening work order $workorder") ;

    my $jobfile = "$state{service_complete}/READY.$jobID.txt" ;
    open(JOBFILE,">$jobfile") or S4P::perish(16,"Error creating jobfile $jobfile") ;

    while(<WORKORDER>) { print JOBFILE ; }
    close(WORKORDER) ;
    close(JOBFILE) ;

#   unlink PENDING.$jobID.txt to indicate readiniess with this portion of the order
    unless (change_state($workorder,$state{service_complete},"PENDING","","") == 0 ) {
        return(recycle($workorder)) ;
    }

    my $status = 0 ;
#   create output work order if there's at least one READY and no more PENDING partno files
    if (glob("$state{service_complete}/READY*")) {
        S4P::logger("INFO","Detected at least one READY file") ;
        unless (glob("$state{service_complete}/PENDING*")) {
            S4P::logger("INFO","Detected no more PENDING files") ;
            $status = create_output_workorder($workorder) ;
        }
    }

    return($status) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub distribution_complete {

    my $workorder = shift ;

    my ($trackID,$partno) = get_trackID($workorder) ;

#   copy input work order to distribution_complete subdirectory
#   error if move failed

    my $newfile = "$state{distribution_complete}/".(split /\//,$workorder)[-1] ;
    readpipe("/bin/cp -f $workorder $newfile") ;
    unless ($? == 0) {
        S4P::perish(25,"Failure copying $workorder to $newfile") ;
    }

#   move any files from waiting_for_distribution subdirectory to distribution_complete
    foreach my $file (glob("$state{waiting_for_distribution}/*")) {
        my $newfile = "$state{distribution_complete}/".(split /\//,$file)[-1] ;
        rename $file,$newfile or S4P::perish(13,"Failure renaming file $file to $newfile") ;
    }

#   concatenate STATUS files from each part and place final timestamp on the result
    
    my $targetfile = "$state{distribution_complete}/FINAL.$trackID.txt" ;
    open (TARGET,">$targetfile") or S4P::perish(16,"Failure opening $targetfile") ;
    foreach my $file (glob("$state{distribution_complete}/STATUS*")) {
        open (SOURCE,"$file") or S4P::perish(15,"Failure opening $file") ;
        while (<SOURCE>) { print TARGET ; }
        close (SOURCE) ;
        unlink $file or S4P::perish(11,"Unlink failed for $file") ;
    }
    my $now = S4P::TimeTools::CCSDSa_Now() ;
    my $timestamp = $now." ".$trackID." ".(split /\//,$state{distribution_complete})[-1] ;
    print TARGET "$timestamp\n" ;
    close TARGET ;

#   move ODL file from toplevel request dir subdirectory to distribution_complete
    my $file = "$trackdir/$trackID.ODL.txt" ;
    my $newfile = "$state{distribution_complete}/$trackID.ODL.txt" ;
    rename $file,$newfile or recycle($workorder) ;

    wrap_up($workorder) ;

    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub ignore_failure {

    my $workorder = shift ;

    my ($trackID,$partno) = get_trackID($workorder) ;

    if ($partno eq "") {
        S4P::perish(24,"Workorder $workorder has unrecognizeable partnumber") ;
    }

    my $jobID = $trackID."_".$partno ;

    my $partnofile = "STATUS.${trackID}_${partno}.txt" ;

#   find  part_number file in state subdirectories
    my $jobfile = "" ;
    foreach my $dir (values(%state)) {
       if (-e "$dir/$partnofile") { $jobfile = "$dir/$partnofile" ; }
    }

    unless (-e $jobfile) {
        S4P::perish(22,"Unable to locate part number file in any of the active directories") ;
    }
    $jobfile =~ s/\/$partnofile// ;
    
#   create part_number error file in order_failure subdirectory

    change_state($workorder,$jobfile,"STATUS",$state{order_failure},"FAILED") or return(1) ;

#   copy input work order to order_failure subdirectory
    my $tmpfile = "$state{order_failure}/ORDER_FAILURE.$jobID.log" ;
    readpipe("/bin/cp -f $workorder $tmpfile") ;
    unless ($? == 0) {
        S4P::perish(26,"Error in copying input work order $workorder to ORDER_FAILURE log $tmpfile.") ;
    }

#   unlink PENDING.$jobID.txt to indicate readiniess with this portion of the order
    my $file = "$state{service_complete}/PENDING.$jobID.txt" ;
    unlink "$file" or S4P::perish(11,"Unlink failed for $file") ;

#   return error if no other PENDING or READY files exist in service_complete subdirectory
    unless (glob("$state{service_complete}/*")) {
        S4P::logger("ERROR", "ORDER_FAILURE handler found no more parts for this order.") ;
        return(1) ;
    }

    $status = 0 ;
#   create output work order if no more PENDING partno files in service_complete directory
    unless (glob("$state{service_complete}/PENDING*")) {
        $status = create_output_workorder($workorder) ;
    }

    return($status) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub error_notification {

    my $workorder = shift ;

    my ($trackID,$partno) = get_trackID($workorder) ;
    my $jobID = $trackID."_".$partno ;
    S4P::logger("INFO","Checking to see if any part failed requiring error notification for $trackID") ;

    my $mailfile = undef ;
    if ($CFG::notifylist eq "nobody") {
        $mailfile = ">-" ; 
    } else {
        $mailfile = "|mailx -s \"S4PM-ODS Order Failure - request ID $trackID\" $CFG::notifylist" ;
    }

    my $prefix = "\n";
    my $message = undef ;
    my $ODL = undef ;

    $prefix .= "This message was automaticaly generated.  Do not reply to the sender.\n\n" ;
    $prefix .= "Error processing S4PM-ODS requestID $trackID.\n" ;
    $prefix .= "One or more requested input granules were not available.\n\n" ;

    foreach my $errorfile (glob("$state{order_failure}/ORDER_FAILURE.*")) {
        open(FILE,"$errorfile") or S4P::perish(15,"Error opening errorfile $errorfile") ;
        while (<FILE>) {
            next unless (/\S/) ;
            if (/^(\d+):\s+(.+)/) {
                chomp() ;
                my ($no,$dbid,$lgid,$error) = split /\s/ ;
                $no =~ s/:// ;
                $message .= "Granule $no:\n" ;
                $message .="\tSDSRV dbID: $dbid\n" ;
                $message .="\tLocalGranuleID $lgid\n" ;
                if ($error eq "NO_REPLACE_DFA") {
                    $message .="\tGranule was marked DeleteFromArchive and no replacement is available.\n" ;
                }
                else {
                    $message .= "Specified granule does not exist inventory.\n" ;
                }
            } else {
               $ODL .= $_ ;
            }
        }
        close FILE ;
    }
    return(0) unless ($message) ;

    $message = $prefix."\n".$message ;
    $message .= "\nThe original ODL for this request follows:\n\n" ;

    open(MAIL,$mailfile) or S4P::perish(15,"Failure opening $mailfile") ;
    print MAIL $message ;
    print MAIL $ODL ;
    #open(WO,$workorder) or S4P::perish(15,"Failure opening $workorder") ;
    #while (<WO>) { print MAIL ; }
    #close WO ;
    close MAIL ;

    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub order_failure {

    my $workorder = shift ;
    my @badfiles = () ;

    my ($trackID,$partno) = get_trackID($workorder) ;
    my $jobID = $trackID ;
    if ($partno ne "") { $jobID .= "_$partno" ; }

#   if no PENDING or READY files exist in service_complete subdirectory
#   and no ORDER_FAILURE files exist in the order_failure subdirectory
#   there were no parts in original order ODL.  Thus, we have an un-fillable order 
#   and need to send notification to appropriate parties.

    unless ((glob("$state{service_complete}/*")) or (glob("$state{order_failure}/*")) ) {
        S4P::logger("WARNING", "ORDER_FAILURE handler found no viable parts for this order.") ;
        #   copy input work order to order_failure subdirectory
        my $tmpfile = "$state{order_failure}/ORDER_FAILURE.$jobID.log" ;
        readpipe("/bin/cp -f $workorder $tmpfile") ;
        unless ($? == 0) {
            S4P::perish(26,"Error in copying input work order $workorder to ORDER_FAILURE log $tmpfile.") ;
        }

#       move ODL file from toplevel request dir subdirectory to distribution_complete

        my $file = "$trackdir/$trackID.ODL.txt" ;
        my $newfile = "$state{distribution_complete}/$trackID.ODL.txt" ;
# Recycle to guard against apparent race
        unless (rename $file,$newfile) {
            unlink($tmpfile) ;
            recycle($workorder) ;
        }

#   wrap_up sends error_notification if any ORDER_FAILURE work orders are found

        wrap_up($workorder) ;

        return(0) ;
    }

#   if PENDING files (not for this partno) exist, then we're not quite
#   ready to create an output work order.  We need to save off the ORDER_FAILURE
#   work order for future error_notification.

    elsif (glob("$state{service_complete}/PENDING.*")) {
#   copy input work order to order_failure subdirectory
        my $tmpfile = "$state{order_failure}/ORDER_FAILURE.$jobID.log" ;
        readpipe("/bin/cp -f $workorder $tmpfile") ;
        unless ($? == 0) {
            S4P::perish(26,"Error in copying input work order $workorder to ORDER_FAILURE log $tmpfile.") ;
        }
        return(0) ;
    } 

#   if READY files do exist but no PENDING, then at least a partial shipment
#   is possible, so we create an output work order, but also send a failure notice

    elsif (glob("$state{service_complete}/READY.*")) {
        ignore_failure($workorder) ;
    } 

#   If none of those cases are true, we have an error we need to pass to the handler

    else {
        S4P::perish(30, "Error detected in intervening processing.  Operator must invoke error handler.") ;
    } 

    return(1) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub create_output_workorder {

    my $workorder = shift ;

    unless (mkdir("$trackdir/.create_lock",0755)) {
        S4P::logger("WARNING","create_output_work order detected existing .create_lock directory - possible race condition.") ;
        exit(0) ;
    }
    my ($trackID,$partno) = get_trackID($workorder) ;

    # Determine if order came from V0 Gateway or WHOM direct
    my $is_v0gwy = ($trackID =~ /o\d+r\w+/);

    my @requestODL = () ;

    # V0 gateway orders are exported using PDR mechanism
    # Note that we use EXPORT_EPD instead of EXPORT to distinguish
    # from recycled EXPORT work orders
    my $newworkorder = ($is_v0gwy ? "EXPORT_EPD" : "SHIP") . ".$trackID.wo" ;
    my $odlfile = "$trackdir/$trackID.ODL.txt" ;
    my $reqfile;
    my ($export_pdr, $node_name);
    if ($is_v0gwy) {
        # Copy ODL to a .REQ file for PDR
        $export_pdr = S4P::PDR::read_pdr($workorder) or
            S4P::perish(101, "Cannot read PDR $workorder: $!");
        my @science_files = $export_pdr->files_by_type('SCIENCE') or 
            S4P::perish(102, "Cannot find any SCIENCE files in $workorder");
        $node_name = $export_pdr->file_groups->[0]->node_name or
            S4P::perish(103, "Cannot find any NODE_NAME in $workorder");
        # Find location of the parent DATA directory
        my $data_dir = dirname(dirname($science_files[0]));
        my $orig_system = $export_pdr->originating_system;

        # TBD:  Do we need $partno in $reqfile to ensure uniqueness?
        $reqfile = "$data_dir/REQUESTS/$orig_system.$trackID.REQ";
        copy($odlfile, $reqfile) or 
            S4P::perish(103, "Cannot copy ODL file $odlfile to $reqfile: $!");
    }

    S4P::logger("INFO", "Beginning creation of output work order $newworkorder") ;

#   read request ODL from running request_ID directory
#   error if read fails

    open(ODL,"$odlfile") or S4P::perish(15,"Error opening odlfile $odlfile") ;

    my $h = "   " ;
    my $p ;
    my $v ;
    my $inCONTACT_ADDRESS = 0 ;
    my $inORDER_SPEC = 0 ;
    my $inSPECIALIZED_CRITERIA = 0 ;
    my %info ;
    my %temp ;

    while (<ODL>) {

        s/^\s+// ;
        if (/CONTACT_ADDRESS$/) { $inCONTACT_ADDRESS++ ; }
        if (/ORDER_SPEC/) { $inORDER_SPEC++ ; }
        if (/SPECIALIZED_CRITERIA/) { $inSPECIALIZED_CRITERIA++ ; }
        if ( $inCONTACT_ADDRESS % 2 ) { push @{$info{CONTACT_ADDRESS}},$_ unless (/CONTACT_ADDRESS$/); }
        if ( ( $inORDER_SPEC % 2 ) and ( $inSPECIALIZED_CRITERIA % 2 ) ) {
            my $line = <ODL> ;
            chomp($line) ;
            #$line =~ s/\s+//g ;
            $line =~ s/^\s+//g ;
            $line =~ s/\s+=\s+/=/g ;
            $line =~ s/\"//g ;
            ($p,$v) = split /=/,$line ;
            $temp{$p} = $v ;
            $line = <ODL> ;
            chomp($line) ;
            #$line =~ s/\s+//g ;
            $line =~ s/^\s+//g ;
            $line =~ s/\s+=\s+/=/g ;
            $line =~ s/\"//g ;
            ($p,$v) = split /=/,$line ;
            $temp{$p} = $v ;
            $line = <ODL> ;
            chomp($line) ;
            #$line =~ s/\s+//g ;
            $line =~ s/^\s+//g ;
            $line =~ s/\s+=\s+/=/g ;
            $line =~ s/\"//g ;
            ($p,$v) = split /=/,$line ;
            $temp{$p} = $v ;
            $info{$temp{CRITERIA_NAME}} = $temp{CRITERIA_VALUE} ;
        }
        if (/MEDIA_TYPE/) { $info{MEDIA_TYPE} = $_ ; }
        if (/MEDIA_FORMAT/) { $info{MEDIA_FORMAT} = $_ ; }

    }

    close(ODL) ;

#   read in each "READY" job part file (work orders from export)

    my $total_file_count = 0 ;
    my $total_file_size = 0 ;
    my $dataset_id ;
    my $dataset_size ;
    my $metadata_id ;
    my $metadata_size ;
    my $output_path ;
    my $esdt ;
    my %lineitem ;

    my @output_file_specs; # For V0 Gateway Orders

    foreach my $readyfile (glob("$state{service_complete}/READY.*.txt")) {
        my $PDR = S4P::PDR::read_pdr($readyfile) ;
        $total_file_size += $PDR->total_file_size ;
        $total_file_count += $PDR->total_file_count ;
        foreach my $file_group (@{$PDR->file_groups}) {
            $esdt = sprintf("%s.%3.3d",$file_group->data_type,$file_group->data_version) ;
            foreach my $file_spec (@{$file_group->file_specs}) {
                if ($is_v0gwy) {
                    my $fs = $file_spec->copy;
                    push @output_file_specs, $fs;
                }
                else {
                    my $type = $file_spec->file_type ;
                    my $size = $file_spec->file_size ;
                    $output_path = $file_spec->pathname ;
                    my $name = (split /\//,$output_path)[-1] ;
                    $output_path =~ s/\/$name// ;
                    if ($type eq "SCIENCE") {
                        $dataset_id = $name ;
                        $dataset_size = $size ;
                    } else {
                        $metadata_id = $name ;
                        $metadata_size = $size ;
                    }
                }
            }
            $lineitem{$esdt} .= $h.$h."GROUP = LINE_ITEM\n" ;
            $lineitem{$esdt} .= $h.$h.$h."ESDT = \"INSERT_ESDT_HERE\"\n" ;
            $lineitem{$esdt} .= $h.$h.$h."OUTPUT_PATH = \"$output_path\"\n" ;
            $lineitem{$esdt} .= $h.$h.$h."DATASET_ID = \"$dataset_id\"\n" ;
            $lineitem{$esdt} .= $h.$h.$h."DATASET_SIZE = $dataset_size\n" ;
            $lineitem{$esdt} .= $h.$h.$h."METADATA_ID = \"$metadata_id\"\n" ;
            $lineitem{$esdt} .= $h.$h.$h."METADATA_SIZE = $metadata_size\n" ;
            $lineitem{$esdt} .= $h.$h."END_GROUP = LINE_ITEM\n" ;
        }
    }
    open(NWO,">$newworkorder") or S4P::perish(16,"Error creating new work order $newworkorder") ;

    if ($is_v0gwy) {
        # Add the ODL request file to the list of FILE_SPECs
        my $fs = new S4P::FileSpec;
        $fs->pathname($reqfile);
        $fs->file_size(-s $reqfile);
        push @output_file_specs, $fs;
        # External subsetter ICD does not include FILE_TYPEs
        map {delete $_->{'file_type'}} @output_file_specs;

        # Create a FILE_GROUP of type SBSTDATA
        my $fg = new S4P::FileGroup;
        $fg->data_type('SBSTDATA');
        $fg->node_name($node_name);
        $fg->file_specs(\@output_file_specs);

        # Reuse the EXPORT PDR, but replace with new FILE_GROUPS
        $export_pdr->file_groups([$fg]);
        $export_pdr->recount();
        my $pdr_text = $export_pdr->sprint;

        # External subsetter ICD does not include DATA_VERSION
        $pdr_text =~ s/\n\s*DATA_VERSION=000;//;

        print NWO $pdr_text;
    }
    else {
        print NWO "GROUP = PRODUCT_REQUEST\n" ;
        print NWO $h."GROUP = CONTACT_ADDRESS\n" ;
        foreach my $line (@{$info{CONTACT_ADDRESS}}) { print NWO $h.$h.$line ; }
        print NWO $h."END_GROUP = CONTACT_ADDRESS\n" ;
        print NWO $h."GROUP = SHIP_INFO\n" ;
        print NWO $h.$h.$info{MEDIA_TYPE} ;
        print NWO $h.$h.$info{MEDIA_FORMAT} ;
        print NWO $h.$h."FTPPUSH_DEST = \"".$info{FTPPUSHDEST}."\"\n" ;
        print NWO $h.$h."FTP_HOST = \"".$info{FTPHOST}."\"\n" ;
        print NWO $h.$h."FTP_USER = \"".$info{FTPUSER}."\"\n" ;
        print NWO $h.$h."FTP_PASSWORD = \"".$info{FTPPASSWORD}."\"\n" ;
        print NWO $h.$h."USERSTRING = \"".$info{USERSTRING}."\"\n" ;
        print NWO $h.$h."GROUP = OUTPUT_INFO\n" ;
        print NWO $h.$h.$h."TOTAL_FILE_COUNT = $total_file_count\n" ;
        print NWO $h.$h.$h."TOTAL_FILE_SIZE = $total_file_size\n" ;
        my @esdts = keys(%lineitem) ;
        my $esdt_token = $esdts[0] ;
        if ($#esdts > 0) { $esdt_token = "MULTIPLE" ; }
        print NWO $h.$h.$h."ESDT = \"$esdt_token\"\n" ;
        print NWO $h.$h."END_GROUP = OUTPUT_INFO\n" ;
        foreach $esdt (keys(%lineitem)) {
            $lineitem{$esdt} =~ s/INSERT_ESDT_HERE/$esdt_token/g ;
            print NWO $lineitem{$esdt} ;
        }
        print NWO $h."END_GROUP = SHIP_INFO\n" ;
        print NWO "END_GROUP = PRODUCT_REQUEST\n" ;
    }
    close(NWO) ;

#   copy READY files and output work order to waiting_for_distribution

    my $nwocopy = "$state{waiting_for_distribution}/".(split /\//,$newworkorder)[-1] ;
    open(IN,"$newworkorder") or S4P::perish(15,"Error opening new work order $newworkorder") ;
    open(OUT,">$nwocopy") or S4P::perish(16,"Error creating copy of new work order $nwocopy") ;
    while(<IN>) { print OUT ; }
    close(IN) ;
    close(OUT) ;

    foreach my $readyfile (glob("$state{service_complete}/READY.*.txt")) {
        my $file = "$state{waiting_for_distribution}/".(split /\//,$readyfile)[-1] ;
        rename $readyfile,$file or S4P::perish(13,"Failure renaming file $readyfile to $file.") ;
    }
    
#   return success if no error
 
    rmdir "$trackdir/.create_lock" or S4P::perish(12,"Rmdir failed for $trackdir/.create_lock") ;

    return(0) ;

}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub recycle {

    my $workorder = shift ;

    S4P::perish(70,"Recycling work order $workorder") ;

    my $file = (split /\//,$workorder)[-1] ;
    readpipe("/bin/cp -f $workorder ../$file.wo") ;
    unless ($? == 0) {
        S4P::perish(13,"Failure renaming file $workorder to ../$file.wo") ;
    }
    return(0) ;
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub get_trackID {

    my $workorder = shift ;
    my $requestID = "" ;
    my $partno = "" ;
    my $jobID = (split /\./,$workorder)[2] ;
    ($requestID,$partno) = split /_/,$jobID ;
    if ($requestID eq "") {
        S4P::perish(23,"Workorder $workorder has unrecognizeable request ID") ;
    }
    return($requestID,$partno) ;
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# change_state returns 0 on success, 1 on failure
sub change_state {

    my $workorder = shift ;
    my $sourcedir = shift ;
    my $sourcetag = shift ;
    my $targetdir = shift ;
    my $targettag = shift ;

    my ($trackID,$partno) = get_trackID($workorder) ;
    S4P::logger("INFO","Change state:$workorder,$sourcedir,$sourcetag,$targetdir,$targettag") ;
    if ($partno eq "") {
        S4P::perish(24,"Workorder $workorder has unrecognizeable partnumber") ;
    }

    my $sourcefile = $sourcetag.".".$trackID."_".$partno.".txt" ;
    
    unless (-d "$sourcedir") {
        S4P::logger("INFO", "Source directory $sourcedir for part number file $sourcefile not found") ;
        return(1) ;
    }

    $sourcefile = "$sourcedir/$sourcefile" ;
    unless (-e "$sourcefile") {
        S4P::logger("INFO", "Part number status file $sourcefile not found in expected directory") ;
        return(2) ;
    }

    if (($targetdir != "") and (!-d "$targetdir")) {
       S4P::logger("INFO", "Source directory $targetdir for part number file $sourcefile not found") ;
       return(3) ;
    }

    unless ($targetdir eq "") {
        my $now = S4P::TimeTools::CCSDSa_Now() ;
        my $timestamp = $now." ".$trackID."_".$partno." ".(split /\//,$targetdir)[-1] ;
        my $targetfile = $targetdir."/".$targettag.".".$trackID."_".$partno.".txt" ;
        open (SOURCE,"$sourcefile") or S4P::perish(15,"Failure opening $sourcefile") ;
        open (TARGET,">$targetfile") or S4P::perish(16,"Failure opening $targetfile") ;
        while (<SOURCE>) { print TARGET ; }
        print TARGET "$timestamp\n" ;
        close TARGET ;
        close SOURCE ;
    }

    unlink "$sourcefile" or S4P::perish(11,"Unlink in change_state failed for $sourcefile") ;

    return(0) ;
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub wrap_up {

    my $workorder = shift ;

#   Send notification if there are any ORDER_FAILURE .  error_notification only
#   sends if an ORDER_FAILURE part is found

    error_notification($workorder) ;

    my ($trackID,$partno) = get_trackID($workorder) ;
    S4P::logger("INFO", "Beginning wrap_up of $trackID") ;

#   Tar up remaining files and stuff it in archivedir
    unless (-d $archivedir) {
        S4P::perish(10,"Archive directory $archivedir does not exist") ;
    }

    my $tarfile = "$trackID.tar" ;
    readpipe("cd $trackdir \; /bin/tar -cf ../$tarfile *") ;
    if ($? != 0) {
        S4P::logger("ERROR", "Error creating tarfile of $trackdir") ;
        return(1) ;
    }
    my $newtarfile = "$archivedir/$tarfile" ;
    $tarfile = "$trackdirtop/$tarfile" ;
    rename $tarfile,$newtarfile or S4P::perish(13,"Failure renaming file $tarfile to $newtarfile") ;

#   Remove working directories
    foreach my $dir (values(%state)) {
        opendir DIR,"$dir" or S4P::perish(14,"Opendir failed on directory $dir") ;
        foreach my $entry (readdir DIR) {
            next if (($entry eq ".") or ($entry eq "..")) ;
            my $file = "$dir/$entry" ;
            unlink $file or S4P::perish(11,"Unlink failed for $file") ;
        }
        closedir DIR ;
        rmdir "$dir" or S4P::perish(12,"Rmdir failed for $dir") ;
    }
    rmdir "$trackdir" or S4P::perish(12,"Rmdir failed for $trackdir") ;

    return(0) ;
}
