#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_request_subscription.pl - script to send subscription request email.

=head1 SYNOPSIS

s4pm_request_subscription.pl B<[-u]> B<[-p]> DO.REQUEST_SUBSCRIPTION.jobid.wo

=head1 DESCRIPTION

s4pm_request_subscription processes input work order in which specification 
is made of subscription criteria(REQUEST_TYPE, EOS_DATA_PRODUCT, BEGINDATE, 
ENDDATE, SUBSCRIPTION_START_DATE, SUBSCRIPTION_EXPIRATION_DATE, etc.), and 
rest of subscription information are obtained from input Parameter file. 
 
=head1 ARGUMENTS

=item B<-u>

user svcs email, such as 'help-disc@listserv.gsfc.nasa.gov'

=item B<-p>

Parameter file - static file resident in request_data station directory
passed to the s4pm_request_subscription script, it contains information
necessary for the subscription(username, push directory, etc.).

=head1 AUTHOR

=head1 HISTORY

=item 2005/07/05

added argument -s to pass the user's unique string name for DNs.

=cut

################################################################################
# s4pm_request_subscription.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict;
use vars qw($opt_u $opt_p
            $do
            $jobtype
            $jobid
          );

use Getopt::Std;
use S4P;

my @subparms = ('REQUEST_TYPE',
	        'SUBSCRIPTION_STATUS',
	        'ECS_ACCOUNT_NAME',
                'USERSTRING', 
                'senderemail',
	        'EOS_DATA_PRODUCT',
	        'EVENT_TYPE',
	        'SUBSCRIPTION_START_DATE',
	        'SUBSCRIPTION_EXPIRATION_DATE',
	        'SUBSCRIPTION_MESSAGE',
	        'local_username',
	        'local_host',
	        'BEGINDATE',
	        'ENDDATE',
);
#	        'DELIVERY_EMAIL_ADDRESS', removed from list 7/14/2005 not
#                    needed for insert notification only
#	        'DELIVERY_METHOD',
#	        'local_password',
#	        'local_directory',

# get run time tokens 

getopts('u:p:');

my $userSvcsEmail = $opt_u;
my $SCLI_parmfile = $opt_p;

if (! -r $SCLI_parmfile) {
   S4P::perish(2,"Cannot read subscription parameter from $SCLI_parmfile");
}

# get filename to process (input workorder name)
my $filename = $ARGV[0];

if (! -r $filename) {
    S4P::perish(2,"Cannot read work order from $filename");
}

S4P::logger("INFO", "*** s4pm_request_subscription.pl starting for work order: $filename");

# parse the input workorder filename
($do, $jobtype, $jobid) = split(/\./, $filename, 4);

#check jobtype - should be like REQUEST_SUBSCRIPTION 

unless ($jobtype =~ /^REQUEST_SUBSCRIPTION/) {
    S4P::perish(1,"Input jobtype not REQUEST_SUBSCRIPTION; jobtype: $jobtype");
}

# read REQUEST_SUBSCRIPTION file

my $subscriptionParms =  ReadParameterFile($filename);

# read ACQParmfile
my $aqParms = ReadParameterFile($SCLI_parmfile);

# gets the parameters and sends email

#$subscriptionParms->{ECS_ACCOUNT_NAME} = $aqParms->{ECSUSERPROFILE};
$subscriptionParms->{DELIVERY_EMAIL_ADDRESS} = "s4pm_dme_clone_me";
#$subscriptionParms->{EVENT_TYPE} = "INSERT";
#$subscriptionParms->{DELIVERY_METHOD} = $aqParms->{DDISTMEDIATYPE};
$subscriptionParms->{USERSTRING} = "$aqParms->{USERSTRING}"." (UserString)";
$subscriptionParms->{SUBSCRIPTION_MESSAGE} = $aqParms->{USERSTRING};
$subscriptionParms->{local_username} = $aqParms->{FTPUSER};
#$subscriptionParms->{local_password} = "clone_me";
$subscriptionParms->{local_host} = $aqParms->{FTPHOST};
$subscriptionParms->{local_directory} = $aqParms->{FTPPUSHDEST};
$subscriptionParms->{senderemail} = "$subscriptionParms->{senderemail}"."(for problems)";

my $sendmail = "/usr/lib/sendmail";
my $subject = "SUBSCRIPTION REQUEST";
if (! -x $sendmail) {
   S4P::perish(2,"Cannot execute $sendmail");
}

my ($message, $subparm, $parmval);
foreach $subparm (@subparms) {
    $parmval = ($subscriptionParms->{$subparm})? $subscriptionParms->{$subparm} : "N/A";
    if ($subparm eq "REQUEST_TYPE") {
       $parmval = "$parmval"." (notification subscription only)";
    }
    $message .= sprintf("%s:\t%s\n", $subparm, $parmval);
}
 
if (! open(MAIL, "| $sendmail -t -i")) {
    S4P::logger("FATAL", "Couldn't open $sendmail $!");
}

print MAIL "To: $userSvcsEmail\n";
#print MAIL "cc: $subscriptionParms->{senderemail}\n" if ($subscriptionParms->{senderemail});
print MAIL "Subject: $subject\n";
print MAIL "\n";
print MAIL "\n$message";

close MAIL;

S4P::logger("INFO", "*** s4pm_request_subscription.pl successfully completed for workorder: $filename");

exit 0;

sub ReadParameterFile(@) {

  my $filename = shift;
  my %cfg;
  open CFG, $filename;
  while (<CFG>)
  {
        next if /^\s*#/;  # Ignore comment lines
        next if /^\s*$/;  # Ignore blank lines

        s/^\s+//;
        s/\s+$//;
        eval{$cfg{$1}=$2||"" if(/(\w+)\s*\=\s*(.*)/)};
  }
  close CFG or warn $!;
  return \%cfg;
}
