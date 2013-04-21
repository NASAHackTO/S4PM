#!/usr/bin/perl

=head1 NAME

s4pm_tk_modify_pcf.pl - GUI for modifying PCF runtime parameters

=head1 SYNOPSIS

B<s4pm_tk_modify_pcf.pl>
[I<PCFfile>]

=head1 DESCRIPTION

GUI for modifying and saving changes to the runtime parameters in a Process
Control File (PCF). 

=head1 ARGUMENTS

=over 4

=item I<PCFfile>

If specified, B<s4pm_tk_modify_pcf.pl> will start up with I<PCFfile> loaded 
into the GUI. If no PCF is specified, a dialog box with known PCFs will be 
brought up.

=back

=head1 AUTHOR

Mike Theobald, Emergent, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_tk_modify_pcf.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Tk ;
use strict ;

my %Params, my %Values, my %ModValues ;
my @LUNs = () ;
my @selection ;
my @filelist ;
my $mw = MainWindow->new ;
my $top = $mw->Toplevel ;
my $load_state = 0 ;
my $SearchDir = `pwd` ;
my $info ;
my $filename ;
my $fileinfo ;
my $listbox ;
my $top_f1 ;
my $top_f2 ;
my $header ;
my $body ;

&InitMain ;

&ReadFile ;

MainLoop ;

exit ;


sub InitMain

{
  $mw->withdraw() ;
  $top->withdraw() ;
  $header = $mw->Frame->pack(-side => 'top', -fill => 'x') ;
  $body = $mw->Frame->pack(-side => 'top', -fill => 'x') ;

  if ($#ARGV != -1)
  {
    $filename = $ARGV[-1] ;
  }

  $mw->title("Modify PCF Runtime Parameters") ;
 
  my $f = $header->Frame
    ->pack(
      -side => 'top',
      -fill => 'x') ;

  $f->Label (
      -textvariable => \$fileinfo,
      -relief => 'ridge')
    ->pack (
      -side => 'left',
      -anchor => 'w',
      -expand => 1,
      -fill => 'x') ;
 
  $f->Button(
      -text => "Exit",
      -command => \&Terminate)
    ->pack(-side => 'right') ;

  $f->Button(
      -text => "Save",
      -command => \&Save)
    ->pack(-side => 'right') ;

  $f->Button(
      -text => "New",
      -command => \&New)
    ->pack(
      -side => 'right',
      -anchor => 'e') ;
 
  $mw->Label(
      -textvariable => \$info,
      -relief => 'ridge')
    ->pack(
      -side => 'bottom',
      -fill => 'x') ;
 
  $mw->bind("<Key-Return>", [ \&Load, Ev('K') ]) ;

  if ($filename eq "")
  # Draw file dialog
  {
    &New ;
  }
  else
  # Draw edit window
  {
    $mw->deiconify() ;
  }

}


sub ListDir
{
  my @selection ;
  my $i ;

  chomp($SearchDir) ;

  @filelist = `cd $SearchDir; find . -follow -name \"\*.tpl\"` ;

  for ($i = 0; $i < 1+ $#filelist; $i++)
  {
    $filelist[$i] =~ s/\s// ;
    $filelist[$i] =~ s/^.\/// ;
  }

  if (Exists $listbox)
  {
    $listbox->delete(0,'end') ;
    $listbox->insert('end',@filelist) ;
    $listbox->selectionSet(0) ;
  }

}


sub Terminate
{

  if (&CheckEdit() == 0)
  {
    $mw->destroy() ;
  } else
  {
    my $top = $mw->Toplevel ;
    $info = "WARNING:  Fields have been modified." ;
    $top->title("Dismiss Edits?") ;
    $top->grab() ;

    my $f1 = $top->Frame
      ->pack(
        -side => 'top',
        -fill => 'x') ;

    $f1->Label(-text => "Dismiss Changes and Exit?")
      ->pack(
        -side => 'left',
        -anchor => 'w') ;
 
    $f1->Button(
      -text => "Save",
      -relief => 'raised',
      -command => \&Save)
      -> pack(-side => 'right') ;
 
    $f1->Button(
      -text => "Cancel",
      -relief => 'raised',
      -command => sub { $top->destroy() ; })
      -> pack(-side => 'right') ;
 
    $f1->Button(
      -text => "Exit",
      -relief => 'raised',
      -command => sub { $mw->destroy() ; })
      -> pack(-side => 'right') ;

    $top->deiconify() ;
  }
    
}


sub Save
{
  my $oldfilename, my $newfilename, my $status ;

  $oldfilename = $filename.".orig" ;
  $newfilename = $filename.".new" ;

  $info = "Saving '$filename'" ;
  $fileinfo = "Filename: $filename" ;

  open(PCF,"$filename") ;
  open(NEWPCF,">$newfilename") ;

  while(<PCF>) {
  
    if ( (/^\D/) || !(/\|/) || (/^\d+\|.*\|.*\|.*\|.*\|.*\|.*/) )
    {
      print NEWPCF ;
      next ;
    }
 
    (my $lun,my $param,my $val) = split /\|/ ;
 
    unless ( ($lun < 10000) || ($lun > 10999) || ($lun == 10117) || ($lun == 10118) || ($lun == 10119) )
    {
      print NEWPCF ;
      next ;
    }

    print NEWPCF "$lun|$Params{$lun}|$ModValues{$lun}\n" ;
    $Values{$lun} = $ModValues{$lun} ;

  }

  close (PCF) ;
  close (NEWPCF) ;

  $status = system("/bin/mv -f $filename $oldfilename") ;
  $status = system("/bin/mv -f $newfilename $filename") ;

  $info = "Saved." ;

}


sub New

{
  &ListDir ;

  if (!(Exists $top)) { $top = $mw->Toplevel ; }

  $top->title("Select PCF") ;

  $top->grab() ;

  $top->Label(
    -text => "Directory")
    ->pack(
      -side => 'top',
      -anchor => 'w',
      -expand => 1) ;

  $top_f1 = $top->Frame()->pack(-side => 'top', -fill => 'x') ;

  $top_f1->Entry(
    -textvariable => \$SearchDir,
    -width => 50)
    ->pack(
      -side => 'left',
      -anchor => 'w',
      -expand => 1) ;

  $top_f1->Button(
    -text => "List",
    -width => 7,
    -command => \&ListDir)
    ->pack(
      -side => 'right',
      -anchor => 'e') ;

  $top->Label(
    -text => "PCF .tpl Files")
    ->pack(
      -side => 'top',
      -anchor => 'w',
      -expand => 1) ;

  $top_f2 = $top->Frame()->pack(-side => 'top', -fill => 'x') ;

  $listbox = $top_f2->Scrolled(
    "Listbox",
    -scrollbars => 'se',
    -selectmode => 'single',
    -width => 50,
    -height => 6)
    ->pack(-side => 'left') ;

  $listbox->insert('end',@filelist) ;
  $listbox->selectionSet(0) ;

  $top_f2->Label(-text => "")->pack(-anchor => 'n') ;

  $top_f2->Button(
    -text => "Open",
    -width => 7,
    -command => sub { &Load ; $top->destroy() ; })
    ->pack(-anchor => 'n') ;

  $top_f2->Label(-text => "")->pack(-anchor => 'n') ;

  $top_f2->Button(
    -text => "Cancel",
    -width => 7,
    -command => sub
      { if ($filename eq "")
        { exit ; }
        else
        { $top->destroy() ; } })
    ->pack(-anchor => 's') ;

  $top_f2->Label(-text => "")->pack(-anchor => 'n') ;

  $top->bind("<Key-Return>", [ sub { &Load ; $top->destroy() ; }, Ev('K') ]) ;
  $listbox->bind("<Double-Button-1>", [ sub { &Load ; $top->destroy() ; }, Ev('b') ]) ;

  $top->deiconify() ;

}


sub Load

{
  $mw->deiconify() ;
  if (Exists $listbox)
  {
    @selection = $listbox->curselection() ;
    $filename = $SearchDir."/".$filelist[$selection[-1]] ;
    $fileinfo = "Filename: $filename" ;
  }

  if (&CheckEdit() == 0)
  {
    &ReadFile ;
  }
  else
  {
    my $top = $mw->Toplevel ;
    $info = "WARNING:  Fields have been modified." ;
    $top->title("Dismiss Edits?") ;
    $top->grab() ;

    my $f1 = $top->Frame->pack(-side => 'top', -fill => 'x') ;

    $f1->Label(
      -text => "Dismiss changes to current file?")
      ->pack(
        -side => 'left',
        -anchor => 'w') ;

    $f1->Button(
      -text => "OK",
      -relief => 'raised',
      -command => sub { &ReadFile ; $top->destroy() ; })
      ->pack(-side => 'right') ;

    $f1->Button(
      -text => "Cancel",
      -relief => 'raised',
      -command => sub { $top->destroy() ; })
      -> pack(-side => 'right') ;

    $top->deiconify() ;
  }

  return ;

}


sub ReadFile

{

  my $b, my $i ;

  @LUNs = () ;
  $load_state = 0 ;
  $body->destroy() ;

  $info = "Loaded file '$filename'..." ;

  if (! Exists $body)
  {
    $body = $mw->Frame->pack(-side => 'top', -fill => 'x') ;
  }

  if (!open(PCF, "$filename")) {
        $info = "ERROR: Could not open $filename" ;
        return ;
  }

  while (<PCF>) {

    next if (/^\D/) ;
    next if !(/\|/) ;
    next if (/^\d+\|.*\|.*\|.*\|.*\|.*\|.*/) ;
    chomp() ;

    (my $lun,my $param,my $val) = split /\|/ ;

    next unless ( ($lun < 10000) || ($lun > 10999) || ($lun == 10117) || ($lun == 10118) || ($lun == 10119) ) ;

    push @LUNs, $lun ;
    $Params{$lun} = $param ;
    $Values{$lun} = $val ;
    $ModValues{$lun} = $val ;

  }

  close(PCF) ;

  if ($load_state == 0) {

    my $f = $body->Frame->pack(-side => 'top', -fill => 'x') ;
    my $width = 40 ;

    $b = $f->Button(
      -text => "  LUN  ",
      -state => 'disabled',
      -relief => 'groove',
      -disabledforeground => 'black')
      ->pack(-side => 'left') ;

    $f->Button(
      -text => "Parameter",
      -state => 'disabled',
      -relief => 'groove',
      -disabledforeground => 'black')
      ->pack(-side => 'left',
        -expand => 1,
        -fill => 'x') ;

    $f->Button(
      -text => "Value",
      -state => 'disabled',
      -relief => 'groove',
      -disabledforeground => 'black',
      -width => $width-2)
      ->pack(
        -side => 'left',
        -anchor => 'e',
        -fill => 'x') ;

    $f = $body->Frame->pack(-side => 'top', -fill => 'x') ;

    for ($i=0 ; $i <= $#LUNs ; $i++)
    {

      my $lun = $LUNs[$i] ;
      my $tag = sprintf("%7s",$lun) ;

      $f = $body->Frame->pack(-side => 'top', -fill => 'x') ;

      $f->Button(
        -text => "$tag",
        -state => 'disabled',
        -relief => 'flat',
        -disabledforeground => 'black')
        ->pack(-side => 'left') ;

      $f->Button(
        -text => "$Params{$lun}",
        -state => 'disabled',
        -relief => 'flat',
        -anchor => 'w',
        -disabledforeground => 'black')
        ->pack(
          -side => 'left',
          -expand => 1,
          -fill => 'x') ;

      $f->Entry(
        -textvariable => \$ModValues{$lun},
        -justify => 'right',
        -width => $width)
        ->pack(
          -side => 'left',
          -anchor => 'e',
          -fill => 'x') ;

      next ;
    }
  }
    else
  {
    for ($i=0 ; $i <= $#LUNs ; $i++)
    {
      my $lun = $LUNs[$i] ;
      $ModValues{$lun} = $Values{$lun} ;
    }
  }
  $load_state = 1 ;
  return ;
}


sub CheckEdit

{

  my $NumMods = 0 ;
  my $i ;

  for ($i=0 ; $i <= $#LUNs ; $i++)
  {
      my $lun = $LUNs[$i] ;
      if ($Values{$lun} != $ModValues{$lun}) { $NumMods++ ; }
  }

  if ($NumMods == 0)
  {
    return(0) ;
  }
  else
  {
    return(1) ;
  }

}
