#!/usr/bin/perl

=head1 NAME

s4pm_poll_data.pl - polls the ECS Datapool for particular data types

=head1 SYNOPSIS

s4pm_poll_data.pl
[B<-c> I<config_file>]
[B<-h>]

=head1 DESCRIPTION

B<s4pm_poll_data.pl> gets a list of data types it needs to poll in the ECS	
Datapool from the configuration specified with the -c option or uses
the default name datatype_list.cfg. Those data types are then searched
for in the Datapool. The behavior of the Datapool search is dictated by 
the following configuration parameters in the configuration file:

=over 4

=item %cfg_poll_data_max_days

Number of data days to process per run.

=item %cfg_poll_data_max_files_daily

The maximum number of files per data day. This is the means by which 
the script knows that a data day directory has been completed. For
MODIS, the number is 288; for AIRS it is 240. To cover both MODIS and AIRS,
you can use 288.

=item %cfg_poll_data_wait_age

The age in days to wait before processing a particular directory. This
allows time for the full set of data to populate a give data day's directory.

=item $cfg_poll_data_dp_dir - The full path to the ECS Datapool

=item %cfg_poll_datatypes

This hash specifies the data types to poll for. The hash keys are the
data types and the hash values are strings containing both the Datapool
directory and the version ID, both halves separated by a space. For
example, 

$cfg_poll_datatypes{MOD021KM} = "MOGT 004";
$cfg_poll_datatypes{MOD02HKM} = "MOGT 004";
$cfg_poll_datatypes{MOD06_L2} = "MOAT 004";

=back

=head1 FILES

=over 4

=item $DATATYPEstat.txt - one/datatype. Keeps track of processed files

=item $tLastProcessed - time last processed in minutes?

=item $aLastSubdir - age of the last subdir processed

=item $DATATYPEproc.db - one/datatype.  A Berkeley DB. uses ResPool.pm to list
the subdirectory name (dates) of subdirectories processed and the "unique time"
they were processed (ie. 2004/03/02 => "20040310121212"

=back

=head1 AUTHOR

Eunice Eng, NASA/GSFC, Code 586 for Code 610.2

=head1 HISTORY

03/09/2004 - first version

=cut

################################################################################
# s4pm_poll_data.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
 
use Getopt::Std;
use vars qw ($opt_a
             $opt_c
             $opt_h
);
use Cwd;
use File::Copy;
use File::Basename;
use S4P;
use S4P::ResPool;
use S4P::PDR;
use S4P::TimeTools;
use S4PM;
use Safe;

my $current_dir = cwd();
my $datatype_list_file = "$current_dir/../datatype_list.cfg";
my $sym_link_dir = "";

getopts('a:c:d:h');

if ($opt_c) { # override datatype_list.cfg
    $datatype_list_file = $opt_c;
}

my $allocate_disk_cfg = $opt_a || "../s4pm_allocate_disk.cfg" ;
my $compartment1 = new Safe 'CFG';
$compartment1->share('%datatype_pool');
$compartment1->rdo($allocate_disk_cfg) or 
    S4P::perish(30, "main: Failed to read in configuration file $allocate_disk_cfg in safe mode: $!");

# Link directory is the INPUT disk pool

$sym_link_dir = $CFG::datatype_pool{'INPUT'};

if ($opt_h){
    print STDERR "Usage: Poll.pl [-c datatype_list_name.cfg] [-d path_symLink_directory] -h\n";
    exit 0;
}

my $compartment2 = new Safe 'CFG2';
$compartment2->share('$cfg_poll_data_dp_dir', '$cfg_poll_data_max_days', '$cfg_poll_data_max_files_daily', '$cfg_poll_data_wait_age', '%cfg_poll_datatypes');
$compartment2->rdo($datatype_list_file) or 
    S4P::perish(30, "main: Failed to read in configuration file $datatype_list_file in safe mode: $!");

my $ptime = unique_time();

my $save_age;

# Process for each datatype

my $wo_number = 0;
my $compartment3 = new Safe 'CFG3';
$compartment3->share('$tLastProc', '$aLastSubDir');

foreach my $type (keys %CFG2::cfg_poll_datatypes) {
    $wo_number++;
    my @dates_done = ();
    my $idate = 0;
    my @Files2Process =();
    my $lastdonedb = "$type"."stat.txt";
    my $dbname = "$current_dir/../$type"."proc.db";

### Get a list of datatype subdirectories 

    my ($cat, $ver) = getCatver($type, $CFG2::cfg_poll_datatypes{$type});
    my @DirList = ();
    my $rtc = 0;
    ($rtc, @DirList) = get_subdirInfo("$CFG2::cfg_poll_data_dp_dir","$cat","$ver", $type);
  
    if ($rtc == 0) {

####### Sort the subdirectories by age (oldest to youngest) S/R

        my $noDirs = scalar(@DirList);

        my(@SDirs) = SortDirNAge($noDirs, @DirList);
     
        my @DDirs = ();
        my @SAges = ();
        my @SSrc  = (); 
        my @tempA; 
        for (my $it = 0; $it < $noDirs; $it++) {
            @tempA = split(";",$SDirs[$it]);
            $DDirs[$it] = $tempA[0]; 
            $SAges[$it] = $tempA[1]; 
            $SSrc[$it] = $tempA[2]; 
        }
    
####### Collect the first maximum day's worth of files 
####### ($CFG2::cfg_poll_data_max_days * $CFG2::cfg_poll_data_max_files_daily)

        my $desStAge = 9999.99;
        my $pdir = cwd();
        my $nDirAge = 0;

        if (-e "$current_dir/../$lastdonedb") {
            $compartment3->rdo("$current_dir/../$lastdonedb") or 
                S4P::perish(30, "main: Failed to read in configuration file $current_dir/../$lastdonedb in safe mode: $!");
            use vars qw ($tLastProc $aLastSubDir);

            $nDirAge = current_age($ptime, $CFG3::tLastProc, $CFG3::aLastSubDir);
            $desStAge = $nDirAge - 0.00005;
        } else { # No config file exists, i.e. first subscription run
        }
    
        for (my $it = 0; $it < $noDirs; $it++) {
            my $date = $DDirs[$it];

########### Test to see if file is > than 

            if (($SAges[$it] > $CFG2::cfg_poll_data_wait_age ) && ($SAges[$it] <= $desStAge)) {

                my $currDir = "$SSrc[$it]/$date"; 
                my @tempArray = ();
                my $rtcode = 0;

                if (-e "$dbname") {
                    my $procdate = S4P::ResPool::read_from_pool($date,$dbname);
                    if ($procdate) {  # here directory has been processed
                        my $old_age = current_age($ptime,$procdate, 0);
                        ($rtcode, @tempArray) = getHDFfiles($currDir, $old_age);

####################### This directory has been checked and processed

                    } else { # Directory has not been processed
                        ($rtcode, @tempArray) = get_allHDFfiles($currDir);
                    }
                } else { # No files processed for this datatype, yet.
                    ($rtcode, @tempArray) = get_allHDFfiles($currDir);
                }

                if (($rtcode == 1)&&(scalar(@tempArray)>0)){ # this directory has been processed 
                    $dates_done[$idate] = $date;
                    $idate++;
                }

                if ($rtcode == 1){
                    if (scalar(@tempArray) > 0) { #data was found
                        $save_age = $SAges[$it]; # save age
                        my $list_len = scalar(@tempArray);
                        my $ind = $#Files2Process + 1;
                        for (my $it = 0; $it < $list_len; $it++) {
                            $Files2Process[$ind] = $tempArray[$it];
                            $ind++;
                        }
        
                        my $size = scalar(@Files2Process);
                        if (($size + $CFG2::cfg_poll_data_max_files_daily) > ($CFG2::cfg_poll_data_max_days * $CFG2::cfg_poll_data_max_files_daily)) {
                            last;
                        } # End testing or size 
                    } # End getting files from this directory
                } else { # Error return from getHDFfiles
#                   S4P::perish (105, "main: Error return from Poll.pl/getHDFfiles or get_allHDFfiles of $rtcode\n");
                }
            } # Processed this date subdirectory
        } # Processed every date for this datatype
    } # Else bad return code from get_DPsubdirs

    my $size = scalar(@Files2Process);

    if ($size > 0) { # if some files saved

####### Build PDR

        my $pdrcd = 0;
        my $date;
        if ($sym_link_dir ne "") { #if symlink directory specified
            my @newList = create_symlinks($sym_link_dir, @Files2Process);
            my $pdrcd = create_pdr(\@newList,"$current_dir",$type,$ver, $wo_number);
        } else { # No symlink directory 
            my $pdrcd = create_pdr(\@Files2Process,$current_dir,$type,$ver, $wo_number);
        }

        if ($pdrcd != 0) {
            print "No PDR created for datatype $type\n";
        } else { # PDR successfully created, update done information
            foreach $date(@dates_done) {
                S4P::ResPool::write_to_pool($date, $ptime, $dbname);
            }

############ Save some information

            if (open (SUBOUT, ">$current_dir/../$lastdonedb")) {
                print SUBOUT "\$tLastProc= $ptime\;\n";
                print SUBOUT "\$aLastSubDir = $save_age\;\n";
                close (SUBOUT);
            } else {
                print "unable to open $current_dir/../$lastdonedb\n";
            }
        }

    } # End of delivering files to process for this datatype

}  # End processing for all datatypes

print "Polling processing Done.\n\n";

exit;

################################################################
# subroutine SortDirNAge - sort the Data pool subdirectories by
#              age.  Keep the corresponding age subdirectory
#              in sync, too.
#
# input: $noDirs = number of items in @Dirs
#        @Dirs   array of subdirectories
#
# output: @SDirs sorted subdirs by age, oldest first
############################################################3

sub SortDirNAge {

  my ($noItems, @Dirs) = @_;

  my @SDirs;
  my @Tages = ();
  my $indx = 0;

###+++
# extract the dates from the string descriptions
###---
  my @tempA; 
  my @Temp = (); 
  for (my $it = 0; $it < $noItems; $it++) {
    @tempA = split(";",$Dirs[$it]);
    $Tages[$indx] = $tempA[1];
    $indx++;
  }
###++
# get list of unique dates.
###---
  my @Sages = ();
  my $cage = 0;  
  $indx = 0;
  my @ages = sort {$a<=>$b} @Tages; # sort ages
  @Tages = @ages;
  
  foreach my $date (@Tages) {
    if ($indx == 0) {
      $Sages[$indx] = $date;
      $indx++;
      $cage = $date;
    }elsif ($date > $cage) {
      $Sages[$indx] = $date;
      $indx++;
      $cage = $date;
#   }elsif ($date == $cage) {
#      # no opt skip
    }
  }   
  @ages = sort {$b<=>$a} @Sages; # reverse sort ages

###+++
# note the Dirs array is already sorted by date
#   Collect the days 
###---  
  my $tage;
  $indx = 0;
  foreach my $date (@ages) {
    foreach my $item (@Dirs) {
      @tempA = split(";",$item);
      $tage = $tempA[1];
  
      if ($tage == $date) {
        chomp $item;
        $SDirs[$indx] = "$item";
        $indx++;
      }
      
    } # end cycling through string descriptions
  } # end cycling through dates 
 
return (@SDirs);
}
#############################################
# sub current_age: updates the current age of the
#                  file, in the datapool
#
# input: $pProc =     current process "YYYYMMDDHHmmss";
#        $tLastProc = last process "YYYYMMDDHHmmss";
#        $alstSubDir = age of last processed time subdir
#                      at time of last execution in 
#                      days. i.e. N.n
#
# output: curAge:     - current age in days N.n
###########################################
sub current_age {

  my ($pdateTime,$ldateTime, $ldirAge) = @_;
  my $curAge = 0;

  my ($lyear, $lmo, $lday, $lhr, $lmin, $lsec, 
       $year,  $mo,  $day,  $hr,  $min,  $sec); 

  $year = substr($pdateTime,0,4);
  $lyear =  substr($ldateTime,0,4);
  $mo = substr($pdateTime,4,2);
  $lmo = substr($ldateTime,4,2);
  $day = substr($pdateTime,6,2);
  $lday = substr($ldateTime,6,2);
  $hr = substr($pdateTime,8,2);
  $lhr = substr($ldateTime,8,2);
  $min = substr($pdateTime,10,2);
  $lmin = substr($ldateTime,10,2);
  $sec = substr($pdateTime,12,2);
  $lsec = substr($ldateTime,12,2);

  my ($ddays, $dhr, $dmin, $dsec) = S4P::TimeTools::delta_dhms($lyear, $lmo, 
    $lday, $lhr, $lmin, $lsec, $year, $mo, $day, $hr, $min, $sec);

  my $deltaday = ($ddays * 1.0) + ($dhr/24.0) + ($dmin/1440.0) + ($dsec/86400.0);
  
##print "current_age: $ddays, $dhr, $dmin, $dsec and $deltaday\n";

  $curAge = $deltaday + $ldirAge;

  return ($curAge);
}

###########################################################
# get_subdirInfo - get list of sub-directory names under
#                 the datapool datatype.ver directory
#                 return the subdir information, too
#
# my ($rtcode, @DirList) = 
#         get_subdirInfo($dp_dir, $cat, $ver, $DType);
#
# input  $dp_dir    path to datapool dir
#        $cat       datapool catagory type (i.e. MOAT, MOGT, etc)
#        $ver       data version number (003 or 004, etc)
#        $DType     data type
# output $rtcode    return code 0 = okay\
#                              -1 = cannot open Datapool dir
#        @DirList   list of directories
#version 2.00
###########################################################

sub get_subdirInfo {

my ($dp_dir, $cat, $ver, $DType) = @_;
my $rtcode = 0;
my @DirList = ();

# get to datatype directory
my $datadir = "$dp_dir/$cat/$DType.$ver";
#print "get_subdirInfo: $datadir\n";
my $indx = 0;

if (chdir "$datadir") {
  my @pooldirs = glob ("*");
  my $subdir;
  if ($#pooldirs != -1) {
    foreach $subdir (@pooldirs) {
      if (!($subdir =~ /^\./)) {
#      if (($subdir ne ".") && ($subdir ne "..")) {
        #if (chdir $subdir) {
          #my @tempArray = glob("*.hdf"); #looking for hdf files
          #my $nofiles = $#tempArray;
          #if ($nofiles !=  -1) { #if sub-dir not empty
            #print "non-zero directory $subdir\n";  
         if (-d "$datadir/$subdir") {
            my $longname = "$datadir/$subdir";
            my $mtime = (-M $longname);
#            $mtime = int ($mtime + .5);
            $DirList[$indx] = "$subdir;"."$mtime;". "$datadir";
#            print "<br>mtime: $Age[$indx]\n";
            $indx++;
          #} # end non-empty subdir
        #}else{
          #print "cannot change to subdir: $subdir: $!\n";
          #chdir "$datadir" || die "cannot return to parent directory $datadir: $!\n";
          #$rtcode = -1;
        #}# end chdir to subdir
        #chdir "../" || die "cannot return to parent directory: $!\n";
         }# end of test for non-directory entries
      }# end processing non . or .. subdirs
    } # end of foreach subdir
  }# done with non-zero pool directory
}else{ #Cannot open $datadir
  $rtcode = -1;
}
return ($rtcode, @DirList);
} # end of get_subdirInfo

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  unique_time - routine to create unique time
#  input - none
#  output - unique time
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub unique_time {
   # Get local time for unique id
   my ($secs, $min, $hr, $mday, $mnth, $yr) = localtime();

   # Calculating the unique id
   $yr = $yr + 1900;
   $mnth++;
   my $time_format = sprintf("%4d%02d%02d%02d%02d%02d",$yr,$mnth,$mday,$hr,$min,$secs);
   return($time_format);
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

############################################################
#  sub getHDFfiles - given the datapool subdirectories,
#                    sorted (names are dates YYYY.MM.DD),
#                    get the *.hdf file names. 
#
#  my ($return, @array) = getHDFfiles($dpSubDirdates,$old_age);
#
#  input $dpSubDir  datapool subdir with full path
#        $old_age   how long ago the subdirectory was prev processed
#
#  output $return   return code 1= good
#                               3= cannot open ECS datapool directory
#                               
#         @arrray   output array of complete path and *.hdf files
#                   for the days in the @dates
################################################################

sub getHDFfiles {

  my ($dpSubDir,$in_old_age) = @_;
  my $rtcode = 1;
  my @FArray = ();

  my $old_age = int (($in_old_age + 0.00005)* 10000);
  $old_age = $old_age/10000.0;

  my $indx = 0;
  if (opendir(POOL,"$dpSubDir")) {
    foreach my $file (sort readdir(POOL)){
      if (!($file =~ /^\./)){
        my @tempA = split (/\./,$file);
        my $test = pop(@tempA);
        if ($test eq "hdf") {
          my $longname = "$dpSubDir/$file";
          my $file_age = -M $longname;
     
          $file_age = int(($file_age + 0.00005) * 10000);
          $file_age = $file_age/10000.0;
  
          if ($file_age < $old_age) { # test if file is young
			#enough not to have been processed
            $FArray[$indx] = $longname;
            $indx ++;
          }else {
          }
        } # end testing .hdf files
      } # end for this file
    } # end for this sub-directory
    close POOL;
  }else { # unable to open a directory
    $rtcode = 3;
  }
  my $test = scalar(@FArray);
# print "Poll/getHDFfiles dir: $dpSubDir output size: $test\n";
  return ($rtcode, @FArray);
}

############################################################
#  sub get_allHDFfiles - get all the hdf files is this
#                        subdirectory.
#
#  my ($return, @array) = get_allHDFfiles($dpSubDirdates);
#
#  input $dpSubDir  datapool subdir with full path
#
#  output $return   return code 1= good
#                               3= cannot open ECS datapool directory
#
#         @arrray   output array of complete path and *.hdf files
#                   for the days in the @dates
################################################################
sub get_allHDFfiles {

  my ($dpSubDir) = $_[0];
  my $rtcode = 1;
  my @FArray = ();

  my $indx = 0;
  if (opendir(POOL,"$dpSubDir")) {
    foreach my $file (sort readdir(POOL)){
      if (!($file =~ /^\./)){
        my @tempA = split (/\./,$file);
        my $test = pop(@tempA);
        if ($test eq "hdf") {
          my $longname = "$dpSubDir/$file";
            $FArray[$indx] = $longname;
          $indx ++;
        }else {
        }
      } # end for this file
    } # end for this sub-directory
    close POOL;
  }else { # unable to open a directory
    $rtcode = 3;
  }
  my $test = scalar(@FArray);
  print "Poll/get_allHDFfiles dir>>>>: $dpSubDir output size: $test\n";
  return ($rtcode, @FArray);
}

#######
###########################################
sub get_datatype_info {

  my $cat = "";
  my $ver = "000";

  my $temp = $_[0];
  chomp $temp; 
  my @atemp = split(" ",$temp);
  $cat = $atemp[0];
  $ver = $atemp[1];
  return ($cat, $ver);
} 
#  
#   Retrieve category and the version # from the file name list
#
#   Input: datatype and list of file name
#   Output: category and version #
#
#   Author: Long B. Pham
#
#   Creation date: March 12, 2004
#
sub getCatver {

   my ($key,$data) = @_;
   my $category;
   my $version;
 
   # Get values from key
   $category = (split " ", $data)[0];
   $version = (split " ", $data)[1];

   return($category, $version);
}

#  
#   Create directory and symbolic link to files
#
#   Input: directory to be linked and list of files
#   Output: list of symbolic linked files
#
#   Author: Long B. Pham
#
#   Creation date: March 12, 2004
#
sub create_symlinks {

   my ($link_dir, @filelist) = @_;
   my @file_array;

   # Create directory if doesn't exist
   if (!(-d $link_dir)) {
      # Create link directory
      my $rtcode = S4P::exec_system("mkdir",$link_dir);
      if ($rtcode == 0) {
         print "$link_dir has been created.\n"
      }else{
         S4P::perror ("1","$link_dir has not been created.\n");
      }
   }  #if
   else {
      warn "$link_dir exists.\n";
   }

   # Loop through each file to create soft link
   my $filepath;
   foreach $filepath (@filelist) {

      chomp($filepath);

      # Split file path
      my @link_file = split "/", $filepath;
     
      # Attain the actual file name
      my $last_file = pop(@link_file);

      # Softlink the files
      my $rtcode = S4P::exec_system("ln", "-s","-f", "$filepath", "$link_dir"."/".$last_file);
      if ($rtcode == 0) {
#         print "$filepath has been soft linked.\n"
      }else{
         warn "$filepath has NOT been soft linked.\n";
      }
       
      # Softlink the xmlfiles
      $rtcode = S4P::exec_system("ln", "-s","-f", "$filepath".".xml", "$link_dir"."/".$last_file.".xml");
      if ($rtcode == 0) {
#         print "$filepath.xml has been soft linked.\n"
      }else{
         warn "$filepath.xml has NOT been soft linked.\n";
      }
      # Add symbolic linked files to list
      push (@file_array,$link_dir."/".$last_file)
   }  #foreach

   #  Return list of symbolic linked files
   return(@file_array);
}

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  create_pdr - routine to create PDR
#  input - array of files and their locations, output PDR location
#  output - PDR and return value
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
sub create_pdr {
   my($pdr_arr, $pdr_loc, $jobtype, $ver, $wo_number) = (@_);
   $pdr_loc |= ".";
   my $pdr;
   my $PDR_ORDER;
   my $req_id;
   my @file_out;
   my $ur_cnt;
   my $i = 0;
   my $j = 0;
   my $k = 111;
   my @ur;
   my @granule;
   my @data_type;
   my @data_version;
   my @fname;
   my @dname;
   my @file_size;
   my $pdr_element;

   # Check to see if file is available 
   if (scalar(@$pdr_arr) == 0)
   {
      S4P::perish(20,"create_pdr(): No information available.  No PDR created.\n");
      return 1;
   }

   # Loop through all the files to extract PDR information 
   foreach $pdr_element(@$pdr_arr)
   {
      # Split the array to retrieve the file name
      #my @tname = split("/",@$pdr_arr[$i++]);
      my @tname = split("/",$pdr_element);

      # Extract the file name
      my $filename = pop @tname;

      # Push file name on to new array 
      push(@fname, $filename);

      # Push the rest to create new directory array
      push(@dname, (join('/',@tname)));

      # Extract data type
      my @fname = split(m"\.+",$filename); 

      # Push data_version
      push (@data_version, $ver);

      # Push data type to new array
      push(@data_type, $jobtype); 

      # Get file size
      push(@file_size, (-s $pdr_element)); 

      # Set unused variables
      # NOTE: These variables are not used. Future usage.
      push(@ur, $j);
      push(@granule, $j++);

   }

   # Get local time for unique id
   my ($secs, $min, $hr, $mday, $mnth, $yr) = localtime();
   my $keep_utime = unique_time() + $wo_number;
   my $wo_base = 1; 
   $req_id = "$keep_utime"."_"."$wo_base";

   $PDR_ORDER = $pdr_loc . "/REGISTER." . $req_id . ".wo";

   # Start a new S4P::PDR for output
   $pdr = S4P::PDR::start_pdr();

   # Temporary assignment of input array
   my @file_tmp = @$pdr_arr;

   # Create PDR output
   for ($ur_cnt = 0;$ur_cnt < scalar(@ur);$ur_cnt++) {
      # Clear the temporary file
      undef(@file_out);

      # Push files for PDR
      my $curr_file = shift(@file_tmp);
      push(@file_out, $curr_file);
      push(@file_out, "$curr_file".".xml");

      # Add granule info to pdr
      my ($ur_fn, undef) = fileparse($file_out[0]);
      my ($esdt, $version, undef, undef, undef) =
          S4PM::parse_patterned_filename($ur_fn);
      my $ur_str = "LGID:$esdt:$version:" . $ur_fn;
      $pdr->add_granule('ur'=>$ur_str, 'data_type'=>$data_type[$ur_cnt],
                        'data_version'=>$data_version[$ur_cnt],
                     'files'=>\@file_out);
      # Output new S4P::PDR to file
      $pdr->write_pdr($PDR_ORDER);

      $wo_base++;
      $req_id = "$keep_utime"."_"."$wo_base";
     
      $pdr = S4P::PDR::start_pdr();
      $PDR_ORDER = $pdr_loc . "/REGISTER." . $req_id . ".wo";
   }
   return (0);

}

