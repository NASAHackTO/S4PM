#!/usr/bin/perl

=head1 NAME

s4pm_failed_order_handler.pl - forces order to complete after partial failure

=head1 SYNOPSIS

s4pm_failed_order_handler.pl
I<workorder>

=head1 DESCRIPTION

=head1 ARGUMENTS

=over 4

=item I<workorder>

The input workorder filename.

=back

=head1 EXIT STATUS VALUES

=head2 Status = 0

Successfully completed processing specified workorder.
To wit, the input workorder (type ORDER_FAILURE) was
copied to output wokrorder type IGNORE_FAILURE.

=head2 Status = 1

Unsuccessful in processing input wokrorder.

=head2 Status other than 0 or 1.

An internal script error was encountered.

=head1 AUTHOR

Mike Theobald

=cut

################################################################################
# s4pm_failed_order_handler.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict ;
require 5.6.0 ;
use S4P;

my $pwd = `/usr/bin/pwd` ;
chomp($pwd) ;

my $wo = glob("$pwd/DO.ORDER_FAILURE*") ;
my $newwo = (split /\//,$wo)[-1] ;
my $jobID = (split /\./,$newwo)[2] ;
$newwo = "../DO.IGNORE_FAILURE.$jobID.wo" ;
readpipe("/bin/cp -f $wo $newwo") ;
if ($? != 0) { S4P::perish("4","Failure copying $wo to $newwo") ; }

my $log = glob("$pwd/ORDER_FAILURE*log") ;
my $newlog = "../IGNORE_FAILURE.$jobID.log" ;
readpipe("/bin/cp -f $log $newlog") ;
if ($? != 0) { S4P::perish("4","Failure copying $log to $newlog") ; }

exit(0) ;
