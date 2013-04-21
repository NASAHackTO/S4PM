#! /usr/bin/perl

=head1 NAME

s4pm_reseq_l0_files.pl - resequence files in L0 granule based on packet times

=head1 SYNOPSIS

s4pm_reseq_l0_files.pl 

=head1 DESCRIPTION

Dices, slices, bakes, roasts, boils, sautees, with fewer calories, with
less fat, with more vitamins and iron, with less fuss, with more fun,
with less hassle, with more joy, with less money, with all the happiness
in a grapefruit.

=head1 AUTHOR

Chris Lynnes, NASA/GSFC, Code 902, Greenbelt, MD  20771.

=cut

################################################################################
# s4pm_reseq_l0_files.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use strict ;
use S4P;
use S4P::TimeTools;

my $l0_cmd = "../l0.pl" ;
unless (-x $l0_cmd) {
   S4P::perish(1,"l0 command $l0_cmd is not executable\n") ;
}


my %Times ;
my %Files ;
my @filenames = () ;
my $FTPDIR ;

my $DN ;
my @DNFiles = glob("S4PM.DN.*") ;

if ($#DNFiles > 0) {
   S4P::perish(1,"Found more than one DN in current directory\n") ;
}

$DN = $DNFiles[0] ;
if ( $DN eq "" ) {
   S4P::perish(1,"No DN files found in directory.\n") ;
}

open(DN,"<$DN") or S4P::perish(1,"Failed opening DN $DN for read.\n") ;

while (<DN>) {
  chomp() ;
  $FTPDIR = $1 if /^FTPDIR: (.+)/ ;
  push @filenames,$1 if /^\s+FILENAME: (.+[1-9]\.PDS)$/ ;
}
close(DN) ;

map { $_ = $FTPDIR."/".$_ } @filenames ;

foreach my $file (@filenames) {
   foreach (readpipe("$l0_cmd -v -s 0 -e 0 -p $file")) {
      next unless /Time/ ;
      s/^\s+// ;
      my $time = S4P::TimeTools::CCSDSa2yyyydddhhmmss((split /=/,(split /\s/)[0])[1]) ;
      $Times{$file} = $time ;
      $Files{$time} = $file ;
      S4P::logger("DEBUG","DEBUG: $file: $time\n") ;
   }
}

my @DistOrderedFiles = sort(values(%Files)) ;
my @OrderedTimes = sort(values(%Times)) ;
my @TimeOrderedFiles = () ;

for (my $i = 0 ; $i < 1+$#OrderedTimes ; $i++) {
   $TimeOrderedFiles[$i] = $Files{$OrderedTimes[$i]} ;
   if ($TimeOrderedFiles[$i] ne $DistOrderedFiles[$i]) {
      S4P::logger("INFO","INFO: moving $TimeOrderedFiles[$i] to $DistOrderedFiles[$i].tmp\n") ;
      `mv $TimeOrderedFiles[$i] $DistOrderedFiles[$i].tmp` ;
   }
}

for (my $i = 0 ; $i < 1+$#OrderedTimes ; $i++) {
   if (-e "$TimeOrderedFiles[$i].tmp") {
      S4P::logger("INFO","INFO: moving $TimeOrderedFiles[$i].tmp to $TimeOrderedFiles[$i]\n") ;
      `mv $TimeOrderedFiles[$i].tmp $TimeOrderedFiles[$i]` ;
   }
}

S4P::logger("INFO","INFO: Done re-sequencing L0 files\n") ;
exit(0) ;

