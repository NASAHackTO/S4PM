package S4PM::GdMSSDbi;

=pod

=head1 NAME

GdMSSDbi - database utility routines.

=head1 SYNOPSIS

use S4PM::GdMSSDbi;

=head1 AUTHORS

Yangling Huang

=head1 CREATED

Jun 08, 2004

=cut

################################################################################
# GdMSSDbi.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4P;
use S4PM;
use DBI;

################################################################################
# mss_db_connect
# Makes an attempt to connect to Sybase using standard connection protocols  
################################################################################
sub mss_db_connect {

  my $SRVRNAME = 'g0acg01_srvr';
  my $USERNAME = 'EcMsAcOrderSrvr'; 
  
  my $ecs_mode ||= S4PM::mode_from_path();
  my $DBNAME = ( $ecs_mode eq "OPS") ? 'mss_acct_db'
    : 'mss_acct_db'.'_'.$ecs_mode;

  #ECS program identifier for MSS database
  my $mss_program_id = 7000165;
  
  #Path to ECS utility for retrieving MSS database password
  my $dcrp = "/usr/ecs/$ecs_mode/CUSTOM/bin/COM/EcUtDcrp";
  
  my $PASSWORD = `$dcrp $ecs_mode $USERNAME $mss_program_id`;

  my ( $dbh, $db );
	
  undef $dbh; # make sure there is no DB connection to begin with
 	
  until ($dbh = DBI->connect("dbi:Sybase:server=$SRVRNAME",$USERNAME,
			     $PASSWORD, {RaiseError => 1}) ) {

    S4P::logger('ERROR', "DBI Connection failed: $DBI::errstr\n" );
    return undef;

  }
    	
  if ( $db = $dbh->prepare(qq[use $DBNAME]) ) {
	
    S4P::logger( 'INFO', "Success connect to $DBNAME database...\n");	
  } else {
			
    S4P::logger( 'FATAL', "Failed to get into $DBNAME database. $dbh->errstr()");
    return undef; 
  }
	
  $db->execute;
  $db->finish;

  return $dbh;
}

#####################################################################################
# get_orderId
# Generate EcAcOrderId in MSS database
######################################################################################
sub get_orderID  {

  my $mssdbh = shift;
   	
  my $sth = $mssdbh->prepare(qq[declare \@OrderIdString varchar(10) 
		exec gd_ProcIncrementOrderId \@OrderIdString output]);	

  if ( !$sth ) {
    S4P::logger( 'ERROR', "Can't execute ProcIncrementOrderId. $mssdbh->errstr()\n"); 
    return 0;
  }
	
  # Execute the prepared database command.
  $sth->execute; 
	
  my $orderId = $sth->fetchrow;
	
  $sth->finish;
	
  return $orderId;
}

#####################################################################################
# insert_EcAcOrder
# Insert user and order info to MSS EcAcOrder table.
######################################################################################
sub  insert_EcAcOrder {

  my ($mssdbh, %record ) = @_;

  my $insert_stat = undef;
	
  $insert_stat = qq[INSERT INTO EcAcOrder ( 				   
		orderId,    
		orderHomeDAAC, 
	        userId,         
		homeDAAC, 
		firstName,           
		lastName,            
         	eMailAddr,                                                     
         	orderDistFormat,                                                 
         	orderMedia,           
		orderGranule,
         	shipAddrStreet1,                  
		shipAddrStreet2,                               
		shipAddrCity,                       
         	shipAddrState,        
		shipAddrZip,     
		shipAddrCountry,               
         	shipAddrPhone,
		externalRequestId
		) VALUES (
		"$record{orderId}",    
		"GSF", 
		"ECSGuest",         
		"GSF",  
		"$record{firstName}",            
		"$record{lastName}",            
        	"$record{eMailAddr}",                                         
         	"$record{tapeFormat}",                                          
         	"$record{mediaType}",           
		$record{orderGranule}, 
         	"$record{shipAddrStreet1}",   
		"$record{shipAddrStreet2}",                                   
		"$record{shipAddrCity}",                       
         	"$record{shipAddrState}",        
		"$record{shipAddrZip}",     
		"$record{shipAddrCountry}",               
         	"$record{shipAddrPhone}",
		"$record{externalRequestId}")];

  my $sth = $mssdbh->prepare($insert_stat);
	    
  if ( !$sth ) {
    S4P::logger( 'ERROR', "Failed Prepare $insert_stat. $mssdbh->errstr()");	
    return 0;
  }
	
  $sth->execute();
	
  $sth->finish;
  return 1;
	
}

####################################################################################
# get_requestId
# Creates request id from MSS database
######################################################################################
sub  get_requestId {

  my $mssdbh = shift;
   	
  my $sth = $mssdbh->prepare(qq[declare \@RequestIdString varchar(10) 
			exec gd_ProcIncrementEcAcRequestId \@RequestIdString output]);
	
  if ( !$sth ) {
    S4P::logger( 'ERROR', "Can't execute ProcIncrementEcAcRequestId. $mssdbh->errstr()"); 
    return undef;
  }
	
  $sth->execute; 
	
  my $requestId = $sth->fetchrow;
	
  $sth->finish, if $sth;
	
  return $requestId;
}

######################################################################################
# insert_EcAcRequest
# Insert user and order info in MSS EcAcOrder.
######################################################################################
sub  insert_EcAcRequest {

  my ($mssdbh, %record) = @_;
	
  my $insert_stat = undef;
       
  $insert_stat = qq[INSERT INTO EcAcRequest (
		orderId,    
		orderHomeDAAC, 
		requestId,  
		requestProcessingDAAC, 
         	firstName,            
		lastName,            
         	eMailAddr,
		numFiles,    
		numBytes,             
		numGranule,        
		tapeFormat,           
		mediaType,           
         	ESDT_Id,             
		shipAddrStreet1,                 
         	shipAddrStreet2,                               
         	shipAddrCity,                        
		shipAddrState,       
         	shipAddrZip,     
		shipAddrCountry,                
		shipAddrPhone,                                    
         	ftpAddress, 
		destinationNode, 
		destinationDirectory 
                ) VALUES (
		"$record{orderId}",    
		"GSF", 
		"$record{requestId}",  
		"GSF",	
       		"$record{firstName}",            
		"$record{lastName}",            
         	"$record{eMailAddr}",
                $record{numFiles},     
		$record{numBytes},             
		$record{orderGranule},        
		"$record{tapeFormat}",           
		"$record{mediaType}",           
         	"$record{ESDT_Id}",             
		"$record{shipAddrStreet1}",                 
         	"$record{shipAddrStreet2}",                              
         	"$record{shipAddrCity}",                        
		"$record{shipAddrState}",       
        	"$record{shipAddrZip}",     
		"$record{shipAddrCountry}",                
		"$record{shipAddrPhone}",                      
         	"$record{ftpAddress}",   
                "$record{destinationNode}",  
		"$record{destinationDirectory}")];              
	
  my $sth = $mssdbh->prepare($insert_stat);
	 
  if ( !$sth ) {
	
    S4P::logger( 'ERROR', "Can't PrePare $insert_stat. $mssdbh->errstr()");
    return 0;	
  }
	
  $sth->execute();
	
  $sth->finish;
	
  return 1;
		
}

1;

