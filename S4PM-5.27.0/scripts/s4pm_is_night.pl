#!/usr/bin/perl

=head1 NAME

s4pm_is_night.pl - QC script for AIRS PGEs to detect nighttime granules

=head1 SYNOPSIS

s4pm_is_night.pl

=head1 DESCRIPTION

This QC script is used with AIRS PGEs to detect nighttime granule. If a 
nighttime granule is found, this QC script returns a failure.

=head1 AUTHOR

Mike Theobald, NASA/L3, Code 610.2

=cut

################################################################################
# s4pm_is_night.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

$Attribute = "DAYNIGHTFLAG" ;
$file = shift ;

$InProgress = 0 ;
$AttrValue = undef ;

open(IN,"$file") or die "Failure opening $file for read\n" ;
while(<IN>) {
  s/\s+//g ;
  $line = $_ ;
  $line = $line."\n" ;
  next unless ((uc($line) =~ /OBJECT=$Attribute\n/) || ($InProgress == 1)) ;
  if ($AttrValue eq "") { $InProgress = 1 ; }
  ($param,$value) = split "=" ;
  $param =~ s/\s*//g ;
  $value =~ s/\s*//g ;
  $value =~ s/\"//g ;
  $param = uc($param) ;
  if ($param =~ /VALUE/)
  {
    $AttrValue = uc($value) ;
    $InProgress = 0 ;
  }
}
close(IN) ;

if ($AttrValue eq "NIGHT") {
#  print "DayNightFlag is Night for $file\n" ;
  exit(1) ;
} else {
#  print "DayNightFlag is not Night for $file\n" ;
  exit(0) ;
}
