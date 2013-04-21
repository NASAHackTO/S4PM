=head1 NAME

QC - execute quality control scripts on PGE output

=head1 SYNOPSIS

use S4PM::QC;

($fatal_error, $ra_clean_files, $ra_block_export) = S4PM::QC::apply_qc($config_file, $export_pdr, $catch_pdr);

=head1 DESCRIPTION

qc runs a set of configurable scripts or programs, taking appropriate actions
depending on their outputs.  The qc scripts/programs can take any number of
arguments, so long as the last arguments are the paths of the metadata file 
and/or science file(s) respectively.

If the check is successful, the data will be exported and sent to local_catcher.
If the check is unsuccessful, configuration parameters will specify whether
to export the data, send it to local_catcher, and pass the job.

=head1 FILES

There is a configuration file (I<qc.cfg> by default) with the specifications
as follows:

  $qc{$data_type} = ["bbbbb script1", "bbbbb script2",...];

where bbbbb is a string of 0's and 1's, expressing the following,
    $use_met
    $use_science
    $block_export
    $block_catch
    $fatal

The $cmd argument is the script/executable with fixed arguments.
For example:
  $qc{'MOD01'} = "01111 /tools/gdaac/OPS/bin/DPS/is_hdf"

If $use_met is specified, the metadata file will be the first argument
appended to $cmd.

If $use_science is specified, the science files are appended onto the
command (after the metadata files if $use_met is specified).

If $block_export is set to 1, a failed qc script will block the inclusion
of the granule in an output EXPORT work order.  
If EXPORT is blocked for a granule, but CATCH is not blocked, the granule
will have a left-over use and must be cleaned manually. 
(This will be fixed with full-granule-accounting).

If $block_catch is set to 1, a failed qc script will block the inclusion
of the granule in an output REGISTER work order. 
If CATCH is blocked, and EXPORT is blocked, the granule
will be included in a CLEAN work order to clean the data off disk.
If EXPORT is I<not> blocked, but CATCH is blocked, the granule will have
left-over uses and must be cleaned manually.
(This will be fixed with full-granule-accounting).

If $fatal is set to 1, a failed qc script will cause the job to fail.
No further qc scripts will be executed on that granule.

Thus in the example, the I<is_hdf> program will be called with the
science file(s) as the arguments.  If it returns non-zero, the error
is treated as fatal.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# QC.pm,v 1.6 2008/01/02 15:32:01 mtheobal Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::QC;
use strict;
use Safe;
use S4P;
use S4P::PDR;
1;

sub apply_qc {
    my ($config_file, $export_pdr, $catch_pdr) = @_;
    # Open configuration file with %qc in it
    my $compartment = new Safe 'CFG';
    $compartment->share('%qc');
    $compartment->rdo($config_file) or
        S4P::perish(1, "Cannot read config file $config_file in safe mode: $!\n");

    # Initialize file_groups
    my @export_fg = @{$export_pdr->file_groups};
    my @catch_fg =  @{$catch_pdr->file_groups};
    my (@allow_export, @allow_catch, @block_export);
    my ($fatal_error, @update_files, @clean_files);

    # Convert "bbbbb script" to a more structured entity:
    # $qc_spec{$esdt} = [ [$script, $use_met, $use_sci, $block_export, $block_catch, $fatal], ... ]
    my %qc_spec;
    foreach my $esdt(keys %CFG::qc) {
        foreach my $qc(@{$CFG::qc{$esdt}}) {
            my ($bitstring, $script) = split('\s', $qc, 2);
            my @bits = split('', $bitstring);
            push(@{$qc_spec{$esdt}}, [$script, @bits]);
        }
    }

    # N.B.:  We assume that the same file_group contents are in the catch and
    # export PDRs
    my $ngran = scalar(@catch_fg);
    my $i;

    # Loop through granules, applying ESDT-specific checks
    for ($i = 0; $i < $ngran; $i++) {
        my $granule = $catch_fg[$i];
        my ($block_export, $block_catch, $fatal) = 
            check_granule($granule, $qc_spec{$granule->data_type});

        $fatal_error++ if ($fatal);

        # Making a list,
        # Checking it twice
        # Gonna find out who's naughty or nice...
        if ($block_export) {
            my @science_files = $export_fg[$i]->science_files;
            push (@block_export, @science_files);
            push (@clean_files, @science_files) if $block_catch;
        }
        else {
            push (@allow_export, $export_fg[$i]);
        }
        push (@allow_catch, $catch_fg[$i]) unless ($block_catch);
    }

    # Quit here if we hit any fatal errors
    S4P::logger('FATAL', "$fatal_error fatal QC conditions encountered.") 
        if $fatal_error;

    # Modify export and catch PDRs
    $catch_pdr->file_groups(\@allow_catch);
    $catch_pdr->recount();
    $export_pdr->file_groups(\@allow_export);
    $export_pdr->recount();
    return ($fatal_error, \@clean_files, \@block_export);
}

sub check_granule {
    my ($granule, $ra_checks) = @_;

    # Form file list for arguments.
    my (@science_files) = $granule->science_files;
    my $met_file = $granule->met_file;

    # Now ESDT-specific checks
    my ($block_export, $block_catch, $fatal);
    foreach my $ra_check(@$ra_checks) {
        my ($cmd, $use_met, $use_science, 
            $cfg_block_export, $cfg_block_catch, $cfg_fatal) = @$ra_check;
        # Form command and execute
        my @files;
        push(@files, $met_file) if ($use_met);
        push(@files, @science_files) if ($use_science);
        my $cmd_string = join(' ', $cmd, @files);
        my ($errstr, $rc) = S4P::exec_system($cmd_string);

        # If $rc is 0, go on to the next check
        if (! $rc) {
            S4P::logger('INFO', "QC SUCCESS: $cmd_string");
            next;
        }
        # Fail if command did not execute properly
        S4P::perish(221, "QC ERROR: failed to execute $cmd_string") if ($rc == 0xff00);
        S4P::logger('ERROR', "QC FAILURE: $cmd_string failed with return $rc");
        if ($cfg_fatal) {
            $fatal = 1;
        }
        if ($cfg_block_catch) {
            $block_catch = 1;
        }
        if ($cfg_block_export) {
            $block_export = 1;
        }
        last if $fatal;
    }
    return ($block_export, $block_catch, $fatal);
}


#==============================================================================
# production_summary()
# Extracts information from the PCF, the LogStatus file,and the runLog. 
# The extracted information is then archived in S4PA
# Input:  $pcf          : PCF object
#         $ps           : reference to %cfg_production_summary  
#         $userstring   : originating system name
#         $log_file     : runlog file name
# Output: $ps_file      : AIRS production summary file
#         $xml_met_file : AIRS production summary met file
#==============================================================================
sub production_summary {

use Fcntl;
use S4P::MetFile;

  my ($pcf, $ps, $user_string, $log_file) = @_;
    
  my $ps_file;
  my $status_file;
  my @run_log;
  my $start_time;
  my $stop_time;
  my $production_time;
  my $xml_met_file;
  my $node_name;
  my %log_files;
  my $status ;
  my $rs;
  my @pcf_text;
  my $ps_dir;
  my %ps_hash;
  my $ps_lun;

  if ( ! $pcf || ! $ps || ! $user_string || ! $log_file)
    { 
      my $job_type;
      my $job_id;
      my $pcf_file;
      my $cfg_file;
      my $compartment;

      opendir DIR , "." or S4P::perish(30, "S4PM::QC:production_summary(): Failed to opendir current directory");
      my @files = readdir DIR;

      foreach ( @files )
	{
	  if ( /DO\.RUN_(.*)\.(.*)/ )
	    {
	      $pcf_file = $_;	      
	      $job_type = $1;
	      $job_id = $2;
	    }       
	  $log_file = $_ if ( /\.Log$/ );
	}

      closedir DIR ;

      $cfg_file = '../'. $job_type.'.cfg';
      
      $compartment = new Safe 'CFG';
      $compartment->share(
			  '$cfg_originating_system', 
			  '%cfg_production_summary'
			 );
      $compartment->rdo( $cfg_file ) or
	S4P::logger('ERROR', "Failed to read in configuration file $cfg_file in safe mode: $!") 
	    and return 0;

      %ps_hash = %CFG::cfg_production_summary;  
      $user_string = $CFG::cfg_originating_system;
      if ( ! $user_string )
	{
	  S4P::logger('ERROR', "Cannot find originating system string in configuration file $cfg_file");
	  return 0;
	}
      if ( exists $ps_hash{'pcf_file'} )
	{
	  $pcf = S4P::PCF::read_pcf($pcf_file) or
	    S4P::logger('ERROR', "Cannot read/parse PCF $pcf_file: $!")
	    and return 0;
	}
    } else {
      %ps_hash = %{$ps};
    }
  
  if (! -f $log_file && exists $ps_hash{'runlog_file'} )
    {
      S4P::logger( 'ERROR', "Cannot find log file $log_file: $!" ); return 0;
    }

  my %log_files = %{ $pcf->log_files };
  $status_file = $log_files{'status'};
  
  if (! -f $status_file &&  exists $ps_hash{'logstatus_file'} )
    {
      S4P::logger( 'ERROR',  "Cannot find Status file $status_file: $!" ) ; return 0;
    }

  my $text = $pcf->text;
  @pcf_text = split ( '\n', $text );
  
  $ps_lun = $ps_hash{'lun'} || 90909;
  my @tmp_array = grep ( /^$ps_lun\|/, @pcf_text );
 
  ( undef, $ps_file, $ps_dir, undef, undef,  $xml_met_file, undef ) = split ( /\|/, $tmp_array[0] ) ;
    
  unless ( sysopen( PS, "$ps_dir/$ps_file", O_WRONLY | O_TRUNC | O_CREAT ) ) 
    {
      S4P::logger( 'ERROR', "Failed to create production summary file $ps_file: $!" ); return 0;
    } 
  
  $node_name = S4P::PDR::gethost();

  my ($secs, $min, $hr, $mday, $mnth, $yr) = localtime();
  $mnth++ ;
  $production_time = sprintf("%4d-%02d-%02d %02d:%02d:%02d",$yr + 1900, $mnth, $mday, $hr, $min, $secs);
   
  S4P::logger("INFO", "Starting up production summary......");

  #extracts information from run log

  print PS "# Production summary header begins #\n";
  print PS "HOST: $node_name\n";
  print PS "STRING: $user_string\n";

  if ( exists $ps_hash{'runlog_file'} )
    {
      @run_log = @{$ps_hash{'runlog_file'}};   
      foreach ( @run_log ) 
	{  
	  my $line = `/bin/grep $_ $log_file`;
	  $line =~ s/#// if ( $line =~ m/#/ );
	  $line =~ s/^\s+// if ( $line =~ m/^\s+/ );
	  print PS $line;
	}
    }

  print PS "# Production summary header ends #\n";

  if ( exists $ps_hash{'logstatus_file'} )
  {
    print PS "\n# LogStatus excerpt begins #\n";
    my @BEGIN_PGE = `/bin/grep BEGIN_PGE $status_file`;
    my $begin_pge_n = @BEGIN_PGE;

    #Separate LogStatus into Context-based (BEGIN) sections.
  
    my $command = "csplit -f begin_pge $status_file";

    foreach ( @BEGIN_PGE ) 
      { 
	$command .= ' /BEGIN_PGE/'; 
      }

    $status = system( $command );

    if ( $status ) 
      {
	S4P::logger( 'ERROR', "Failed to split LogStatus file $status_file: $!" ); return 0;
      }
  
    #Get excluded partner from <pgename>.cfg
    my @excludes = @{$ps_hash{'logstatus_file'}{'exclude'}};
  
    unlink 'begin_pge00';

    my $i;

    for ( $i = 1; $i <=  $begin_pge_n; $i++ ) 
      {
	print PS "$BEGIN_PGE[$i - 1]\n";
	my $file =  sprintf("%s%02d",'begin_pge', $i ); 
	unless ( sysopen( STATUS, $file, O_RDONLY ) ) 
	  {
	    S4P::logger( 'ERROR', "Failed to read status file $file: $!" ) ;
	    return 0;
	  }
    
	my $start_line = 0;
	my %wef_hash = ();
	my @message = ();

	# We want the messages in the summary to appear in the same order that they appear 
	# in the LogStatus file.
	my @messages = ();

	# For LogStatus, extracts every warning, error, fatal message, and non 
	# excluded partner with full text of at least one and a count.  
	while ( <STATUS> ) 
	  {
	    if ( m/\w+\(\):(.*):\d+$/ ) 
	      {
		push @message, $_;
		$start_line = 1;
	      } 
	    elsif ( $start_line ) 
	      {
		push @message, $_;
		my $message_str = "@message";
	    
		my $is_exclude = exclude ($message_str, @excludes );
	  
		if ( ! $is_exclude ) 
		  { 
		    $wef_hash{$message_str}++;
		    push @messages, $message_str;
		  }

		$start_line = 0;
		@message = ();
	      }
	  }

	my $hash_size = scalar(keys %wef_hash );

	if ( $hash_size > 0 ) 
	  {
	    for my $key ( @messages )
	      {
		if ( exists $wef_hash{$key} )
		  {
		    print PS "$wef_hash{$key}\n";
		    my @tmp = split ('\n', $key);
		    foreach ( @tmp )
		      {
			next if ( $_ !~ m/\w/g );
			s/^\s+//; print PS "$_\n"; 
		      }
		    print PS "\n";
		  }
	      }
	  }
	unlink $file;
      }

    print PS "# LogStatus excerpt ends #\n";
    close ( STATUS );
  }

  # Extracts full entry of the highest version number from PCF file 
  # based on LID which is defined in <PGE_NAME>.cfg, 
  if ( exists $ps_hash{'pcf_file'} ) {

      my @pcf_info = @{$ps_hash{'pcf_file'}};

      print PS "\n# PCF excerpt begins #\n";

      foreach my $lun ( @pcf_info )
        { 
          my $find_highest_version = 0;
          foreach ( @pcf_text )
	    {
	      if ( $_ =~ m/^$lun\|/ )
	        {
	          print PS "$_\n";
	          if (  m/Collection\sStart\sTime\sUTC/ )
		    {
		      (undef, undef, $start_time ) = split ( /\|/, $_);
		      chop $start_time;
		    }
    
	          if (  m/Collection\sStop\sTime\sUTC/ )
		    {
		      (undef, undef, $stop_time) = split ( /\|/, $_);
		      chop  $stop_time;
		    }
	          $find_highest_version = 1;
	          last;
	        }
	    }
    
        unless( $find_highest_version )
          {
	    S4P::logger( 'WARN', "Could not find LUN $lun from PCF file: $!" ) ;
          }
        }
      
      print PS "# PCF excerpt ends #\n";
  }

  close ( PS);
  
  my ($startdate,$starttime) = split /T/,$start_time ;
  my ($stopdate,$stoptime) = split /T/,$stop_time ;
  ( $rs, $status ) = S4P::exec_system("gzip $ps_dir/$ps_file");
 
  S4P::perish( 'ERROR', "Failed to compress $ps_dir/$ps_file file: $!" ) if ( $status ) ;
  my $tmp_file = $ps_file.".gz";
  unless ( rename ( "$ps_dir/$tmp_file", "$ps_dir/$ps_file" ) )
    {
        S4P::logger ('ERROR', "Failed to rename $ps_dir/$tmp_file to $ps_dir/$ps_file: $rs");
	return 0;
    }

  S4P::logger('INFO', "Successfully creates AIRS production summary file $ps_dir/$ps_file");
  
  my $xml_tmp_str = s4pa_xml_template();
  $tmp_file =~ s/hdf/txt/;
  my ($job_type, undef, undef, $version_id, undef, undef ) = split ( '\.', $xml_met_file );
  my $xml_str = sprintf($xml_tmp_str, $job_type, $version_id, $tmp_file, $tmp_file, $production_time, 
			$startdate, $starttime, $stopdate, $stoptime );

  unless ( sysopen( XML, "$ps_dir/$xml_met_file", O_WRONLY | O_TRUNC | O_CREAT ) ) 
    {
      S4P::logger( 'ERROR', "Failed to create AIRS production xml met file $xml_met_file: $!" ) ;
      return 0;
    }

  my $odl = S4P::MetFile::xml2odl($xml_str);                                       
  print XML $odl; 

  close ( XML );
  S4P::logger('INFO', "Successfully Creates AIRS production summary met file $ps_dir/$xml_met_file");

  return 1;
}

sub exclude {

  my ( $msg, @exclude_array ) = @_;
  
  my $is_exclude = 0;

  foreach  ( @exclude_array ) 
    {
     
      if ( $msg =~ m/$_/ && $msg !~ m/Error:|Warning:|Fatal:/i)
	{
	      $is_exclude = 1 ; last;   
	} 
    }

  return $is_exclude;
}
      
sub s4pa_xml_template 
{  
  my $template_str = <<EOF;
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile>
    <SchemaVersion>1.0</SchemaVersion>
    <DataCenterId>GSFC</DataCenterId>
    <CollectionMetaData>
        <ShortName>%s</ShortName>
        <VersionID>%s</VersionID>
    </CollectionMetaData>
    <DataGranule>
        <GranuleID>%s</GranuleID>
        <LocalGranuleID>%s</LocalGranuleID>
        <ProductionDateTime>%s</ProductionDateTime>
    </DataGranule>
    <RangeDateTime>
        <RangeBeginningDate>%s</RangeBeginningDate>
        <RangeBeginningTime>%s</RangeBeginningTime>
        <RangeEndingDate>%s</RangeEndingDate>
        <RangeEndingTime>%s</RangeEndingTime>
    </RangeDateTime>
</S4PAGranuleMetaDataFile>
EOF

return $template_str;

}

