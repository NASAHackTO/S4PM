#!/usr/bin/perl

=head1 NAME

s4pm_tk_trmon.pl - GUI for monitoring the Track Requests station

=head1 SYNOPSIS

B<s4pm_tk_trmon.pl>

=head1 DESCRIPTION

This Tk GUI provides a way to monitor status of orders via the Track
Requests station in on-demand processing S4PM strings.

=head1 AUTHOR

Mike Theobald, Emergent, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_trmon.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

#This version creates a unique file dialog box

use Tk ;
use Tk::Pane ;
use strict ;

my $mw = MainWindow->new ;
my $repeat_interval = 10 ;
$mw->withdraw() ;
$mw->geometry("800x300+10+10") ;
my $mf = $mw->Frame->pack(-expand => 1,-fill => 'both') ;
my $tr_frame ;
my @track_states = ("ODL.txt",
                    "A.waiting_for_REQUEST_DATA",
                    "B.waiting_for_PREPARE",
                    "C.waiting_for_ALLOCATE",
                    "D.waiting_for_RUN",
                    "E.waiting_for_EXPORT",
                    "F.service_complete",
                    "G.waiting_for_CLOSE",
                    "H.distribution_complete",
                    "Z.order_failure",
) ;

my %map = ( "ODL.txt" => "ODL",
            "A.waiting_for_REQUEST_DATA" => "Request Data",
            "B.waiting_for_PREPARE" => "Find Data",
            "C.waiting_for_ALLOCATE" => "Prepare Run",
            "D.waiting_for_RUN" => "Allocate Disk",
            "E.waiting_for_EXPORT" => "Run Algorithm",
            "F.service_complete" => "Parts Expected",
            "G.waiting_for_CLOSE" => "Parts Processed",
            "H.distribution_complete" =>  "Order Complete",
            "Z.order_failure" => "Part Failure",
) ;

my $ts_ra = \@track_states ;
my $crawltext = "" ;

#my $state_fr = $mf->Frame->pack(-side => 'left', -fill => 'both', -anchor => 'ne') ;
#my $state_fr = $mf->Scrolled(qw/Pane -scrollbars s/)->pack(-side => 'left', -fill => 'x', -anchor => 'nw', expand => 1) ;
#my $track_fr = $mf->Scrolled(qw/Pane -scrollbars s/)->pack(-side => 'left', -fill => 'x', -anchor => 'nw', expand => 1) ;
my $state_fr = $mf->Frame->pack(-side => 'left', -fill => 'x', -anchor => 'nw', expand => 1) ;
#my $track_fr = $mf->Frame->pack(-side => 'left', -fill => 'x', -anchor => 'nw', expand => 1) ;
my $track_fr = $state_fr ;
my $crawl_fr = $mw->Frame(-relief=>'sunken',-borderwidth=>2)->pack(-side => 'top', -fill => 'both', -anchor => 'nw') ;
my $buttn_fr = $mw->Frame->pack(-side => 'top', -fill => 'both', -anchor => 'nw') ;
my $crawl_lbl = $crawl_fr->Label(-textvariable=>\$crawltext)->pack(-side => "left",-fill =>"both") ;
my $tf = $track_fr->Frame->pack(-side => 'left', -fill => 'x') ;
my $tg = $state_fr->Frame->pack(-side => 'left', -fill => 'x') ;

&Init($ts_ra) ;

$mw->deiconify() ;

&Track(\$tf) ;

$mw->repeat($repeat_interval*1000,[\&Track,\$tf]) ;

MainLoop ;

exit ;


sub Init {
  $buttn_fr->Button(-text => "Exit",-command => [sub{exit}])->pack ;
  $mw->title("Track Requests Monitor") ;
 
  foreach my $state (@track_states) {
    my $label = $map{$state} ;
#   print "$label\n" ;
    my $f = $tg->Button(-textvariable=>\$label,-padx=>0, -pady=>0, -highlightthickness=>1)
                     ->grid(-padx =>1,-pady=>0,-sticky=>'we') ;
  }
  return(0) ;
}

sub Track {
  my $ACTIVE_REQUESTS = "./ACTIVE_REQUESTS" ;

  my %trinfo ;
  foreach (sort(readpipe("/bin/find $ACTIVE_REQUESTS -type f"))) {
    chomp() ;
    s/^$ACTIVE_REQUESTS\/// ;
    my ($reqID,$state,$info) = split /\// ;
    push @{$trinfo{$reqID}{$state}},$info ;
  }

  $tf->destroy() ;
  $tf = $track_fr->Frame->pack(-side => 'left', -fill => 'x') ;
  foreach my $state (@track_states) {
    my @buttons = () ;
    foreach my $reqID (sort(keys(%trinfo))) {
      my $count = "" ;
      my $dir = "$ACTIVE_REQUESTS/$reqID/$state" ;
      if ($state eq "ODL.txt") { $dir = "$ACTIVE_REQUESTS/$reqID/$reqID.ODL.txt" ; }
      if (defined($trinfo{$reqID}{$state})) { $count = scalar(@{$trinfo{$reqID}{$state}}) ; }
      #print "$dir $count\n" ;
      #my $g = $tf->Button(-textvariable=>\$count,-width=>2,-padx=>0, -pady=>0, -bd=>0, -highlightthickness=>1) ;
      my $g = $tf->Button(-textvariable=>\$count,-width=>2,-padx=>0, -pady=>0, -highlightthickness=>1) ;
      if ($count ne "") { $g->configure(-bg=>'#228b22',-fg=>'white') ; }
      $g->configure(-command=>[\&drill_down,$reqID,$state]) ;
      $g->bind('<Enter>',[sub{$crawltext = "./$reqID/$state"}]) ;
      $g->bind('<Leave>',[sub{$crawltext = "!"}]) ;
      push @buttons,$g ;
    }
    my $f = shift @buttons ;
    next unless ($f) ;
    $f->grid(@buttons,-padx=>1, -pady=>0, -sticky=>'we') ;
  }
#  foreach (glob("$ACTIVE_REQUESTS/*")) {
#    chomp() ;
#    my $reqID = (split /\//)[-1] ;
#    my $g = new_track($reqID,$tf,\$crawltext,%trinfo) ;
#  }

  return($tf) ;
}

sub new_track {
    my $reqID = shift ;
    my $track_fr = shift ;
    my $crawltext_r = shift ;
    my @states = @_ ;

    my $state = shift @states ;

    my $track = $track_fr->Frame
      ->pack(
        -side => 'left',
        -fill => 'x') ;

    my $text = $reqID ;
    my $g = $track->Button() ;
    $g->pack (
        -side => 'top',
        -anchor => 'w',
        -expand => 1,
        -fill => 'x',
    );

    foreach my $state (@states) {
       my $text ;
       if ($state eq "Z.order_failure") {$text = "Z" ;}

       my $g = $track->Button(-text => get_info($reqID,$state)) ;
       $g->configure(-command=>[\&drill_down,$reqID,$state]) ;
       $g->bind('<Enter>',[sub{${$crawltext_r} = "./$reqID/$state"}]) ;
       $g->bind('<Leave>',[sub{${$crawltext_r} = "!"}]) ;

       $g->pack (
          -side => 'top',
          -fill => 'x',
          -anchor => 'nw') ;

   }

   return ($track) ;
}

sub get_info
{
    my $reqID = shift ;
    my $state = shift ;
    my $dir = "./ACTIVE_REQUESTS/$reqID/$state" ;
    my $count = "" ;
    if (-d $dir) { $count = grep /./,readpipe("/bin/ls -1 $dir") ; }
    if ($count < 1) { $count = "" ; }

    return($count) ;
}

sub drill_down
{
    my $reqID = shift ;
    my $state = shift ;

    my $dir = "./ACTIVE_REQUESTS/$reqID/$state" ;
    my $pw = new MainWindow ;
    $pw->geometry("400x400+20+20") ;
    my $pf = $pw->Frame->pack(-side => 'top', -fill => 'both', -anchor => 'nw', -expand => 1) ;
    my $filelist = $pf->Scrolled("Listbox",-selectmode=>'single',-scrollbars=>'oe')->pack(-side =>'top',-fill=>'both',-expand=>1) ;
    foreach my $file (glob("$dir/*")) {
      next if (($file eq /\./) or ($file eq /\.\./)) ;
      $file =~ s/$dir\/// ;
      $filelist->insert('end',$file) ;
    }
    $filelist->pack(-side => 'top', -fill => 'both', -anchor => 'nw', -expand => 1) ;
    print "Vrrrr...\n" ;
    return("2") ;
}
