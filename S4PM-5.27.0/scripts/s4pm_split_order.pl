#!/usr/bin/perl

=head1 NAME

s4pm_split_order.pl - split a ODL into multiple working orders

=head1 SYNOPSIS

B<s4pm_split_order.pl>

=head1 EXIT

0 = Successful, 1 = Failed.

=head1 AUTHOR

Yangling Huang

=cut

################################################################################
# s4pm_split_order.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Fcntl;
use File::Copy;
use S4P;
use S4P::OdlGroup;
use S4P::OdlTree;
use Getopt::Std;
use Safe;
use strict;

################################################################################
# Global variables                                                             
################################################################################
use vars qw( $opt_f
	     $order_id
	   );

getopts('f:');

# Reading an ODL file
my $odl = S4P::OdlTree->new( FILE =>  $ARGV[0]);

#check if the order comes from WHOM

foreach ( $odl->search( NAME => 'VERSION' ) ) {
  my $sender = $_->getAttribute( 'SENDER_VERSION' );
  if ( $sender =~ m/WHOM/g ) {
     my $new_order_name = $ARGV[0] ;
     $new_order_name =~ s/DO.SERVICE/ORDER/;
     $new_order_name .= '.wo';
    
     copy ( $ARGV[0], $new_order_name)
        or S4P::perish( 1, "main: Cannot copy $ARGV[0] to $new_order_name: $!");
    
     S4P::logger('INFO', "main: Copying $ARGV[0] to $new_order_name");
      exit 0;
   }
}

foreach ( $odl->search( NAME => 'PRODUCT_REQUEST' ) ) {
  $order_id =  $_->getAttribute( 'REQUEST_ID' );
}

$order_id = sprintf ("o%sr%s", $order_id =~ /"(.*):(.*)"/  );

my $new_order_name = 'ORDER.'.$order_id.'.wo';

copy ( $ARGV[0], $new_order_name)
    or S4P::perish( 1, "main: Cannot copy $ARGV[0] to $new_order_name: $!");

S4P::logger('INFO', "main: Rename $ARGV[0] to $new_order_name.....");

# Read the station specific S4P configuration file.

my $compartment = new Safe 'CFG';
$compartment->share('$max_item_count');
$compartment->rdo($opt_f) or
    S4P::perish(30, "main: Failed to read in configuration file $opt_f in safe mode: $!");

my @items = get_item_counts ( $odl );

my $item_count = scalar( @items );

if ( $item_count == 0 ) {

  S4P::perish(1, "main: Order Split: can't split; there is no item in $ARGV[0])\n" );
} elsif ( $item_count <= $CFG::max_item_count ) {

  # If the item count is under the limit, there is no need for splitting.
  S4P::logger( 'INFO', "main: There is no need for splitting  $new_order_name....");

} else {

  # Otherwise, split the order
  my $split_count = POSIX::ceil( $item_count / $CFG::max_item_count );
  S4P::logger( 'INFO', "main: $new_order_name will split to $split_count Sub-Orders.....");

  my $MONITOR_GROUP =  get_MONITOR_REQUEST_group ( $odl );
  my $VERSION_GROUP = get_VERSION_REQUEST_group ( $odl ); 

  for (my $i = 1; $i <= $split_count; $i++) {

    # Create a new order file name like <new order file name>.N<natural number>  
    my $split_order_name = 'ORDER.'. $order_id . 'N' . $i . '.wo';

    unless ( sysopen( TREE, $split_order_name, O_WRONLY | O_TRUNC | O_CREAT ) ) {

      S4P::perish ( 1 , "main: Failed to Create splited order file  $split_order_name :$!");  
    }

   
    # clones OdlTree object 
    my $new_tree = $odl->clone();
    my @split_tree  =  $new_tree->search ( NAME => 'PRODUCT_REQUEST' ) ; 
   
    for ( my $j = 1; $j <=  $CFG::max_item_count; $j++ ) {
      
        if ( @items ) {
          my $item = pop ( @items );
          $split_tree[0]->insert( $item );
        }
    }
 
    $split_tree[0]->insert( $MONITOR_GROUP );
    $split_tree[0]->insert( $VERSION_GROUP );
    my $odl_string = $split_tree[0]->toString (INDENT => '    ' );

    print TREE $odl_string;
    close ( TREE);

    S4P::logger( 'INFO', "main: Sub-Order $split_order_name is created ....");
  }

  S4P::perish ( 1, "main: Failed to remove $new_order_name" )
      unless unlink $new_order_name;
}

S4P::logger( 'INFO',"main: Exit From Split order in Split Service Station");

exit 0;
    

###############################################################################
# get_item_counts  
###############################################################################

sub get_item_counts {

  my $odl_tree = shift;
  
  my @item_list = ();

  if (  my @item_sets = $odl_tree->search( NAME => 'LINE_ITEM_SET' ) ) {
    
    push @item_list,  @item_sets;
    if ( $odl_tree->delete ( NAME =>'LINE_ITEM_SET' ) ) { 
      S4P::logger ('ERROR', "get_item_counts(): Could not Delete ITEM_LINE_SET from original ODL file" );
	return undef;
    }
  }

  if ( my @items = $odl_tree->search( NAME => 'LINE_ITEM' ) ) {
  
     push @item_list, @items;
     if ( $odl_tree->delete ( NAME =>'LINE_ITEM' )  ) {
      S4P::logger ('ERROR', "get_item_counts(): Could not Delete ITEM_LINE from original ODL file" );
	return undef;
    }

   }

  return @item_list;
}

###############################################################################
# get_MONITOR_REQUEST_group    
###############################################################################
sub get_MONITOR_REQUEST_group {

   my $odl_tree = shift;
   my $group ;
   if ( my @monitor  = $odl_tree->search ( NAME =>'MONITOR' ) ) {
     $group = $monitor[0];
     $odl_tree->delete ( NAME =>'MONITOR' ) ;
   } else {
     return undef;
   }

   return $group;
}

  
###############################################################################
# get_VERSION_REQUEST_group    
###############################################################################
sub get_VERSION_REQUEST_group {

   my $odl_tree = shift;
   my $group ;
   if ( my @version  = $odl_tree->search ( NAME =>'VERSION' ) ) {
     $group = $version[0];
     $odl_tree->delete ( NAME =>'VERSION' ) ;
   } else {
     return undef;
   }

   return $group;
}



