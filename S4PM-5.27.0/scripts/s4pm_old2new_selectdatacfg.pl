#!/tools/gdaac/COTS/perl-5.8.5/bin/perl

=head1 NAME

s4pm_old2new_selectdatacfg.pl - convert old style to new Select Data config

=head1 SYNOPSIS

s4pm_old2new_selectdatacfg.pl
B<-i> I<input>
B<-o> I<output>
B<-c> I<s4pm_pge_esdt.cfg>

=head1 DESCRIPTION

This stand-alone script converts old style Select Data configuration files
(aka Specify Data configuration files) into the new style that serves both
Stringmaker and the Select Data station.

The input is specified with the B<-i> argument and the output with the
B<-o> argument. The B<-c> argument specifies the old style s4pm_pge_esdt.cfg
file from which this script gets vital information about the algorithm
being converted.

After conversion, certain fields in the new file are left unset and need
to be set manually.

=head1 AUTHOR

Stephen berrick

=cut

################################################################################
# s4pm_old2new_selectdatacfg.pl,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use S4PM::Algorithm;
use Safe;
use Getopt::Std;
use strict;
use vars qw($opt_i $opt_o $opt_c);

getopts('i:o:c:');
unless ( $opt_i ) {
    die "\nUsage: $0 -i <infile> -o <outfile> -c <s4pm_pge_esdt.cfg>\n\n";
}
unless ( $opt_o ) {
    die "\nUsage: $0 -i <infile> -o <outfile> -c <s4pm_pge_esdt.cfg>\n\n";
}
unless ( $opt_c ) {
    die "\nUsage: $0 -i <infile> -o <outfile> -c <s4pm_pge_esdt.cfg>\n\n";
}

my $infile = $opt_i;
my $outfile = $opt_o;

open(OUT, ">$outfile") or die "Failed to open $outfile for write: $!\n\n";

my $algorithm = new S4PM::Algorithm($infile);

my $exec_name = get_exec($algorithm->algorithm_name);

print OUT "\$algorithm_name = '" . $algorithm->algorithm_name . "';\n";
print OUT "\$algorithm_version = '" . $algorithm->algorithm_version . "';\n";
print OUT "\$algorithm_exec = '$exec_name';\n";
print OUT "\$processing_period = " . $algorithm->processing_period . ";\n";
print OUT "\$pre_processing_offset = " . $algorithm->pre_processing_offset . ";\n";
print OUT "\$post_processing_offset = " . $algorithm->post_processing_offset . ";\n";
print OUT "\$metadata_from_metfile = " . $algorithm->metadata_from_metfile . ";\n";
print OUT "\$apply_leapsec_correction = " . $algorithm->apply_leapsec_correction . ";\n";
if ( $algorithm->apply_leapsec_correction ) {
    print OUT "\$leapsec_datatypes = '" . $algorithm->leapsec_datatypes . "';\n";
}
print OUT "\$pcf_path = '" . $algorithm->pcf_path . "';\n";
print OUT "\$product_coverage = " . $algorithm->product_coverage . ";\n";
print OUT "\n";
print OUT "\n# CHANGE THE SETTING BELOW IF YOU WANT PH FILES MADE\n";
print OUT "\$make_ph = 1;\n\n";
print OUT "\$run_easy = 0;\n";

my @inputs = @{ $algorithm->input_groups };
my @outputs = @{ $algorithm->output_groups };
my $stats_dts = "(";
foreach my $out ( @outputs ) {
    $stats_dts .= "'" . $out->data_type . "', ";
}
$stats_dts .= ")";

my %inputs;
my %outputs;
map { $inputs{$_->data_type}  = 1 } @inputs;
map { $outputs{$_->data_type} = 1 } @outputs;

print OUT "\n\%inputs = (\n";
my $count = 1;
foreach my $inp ( @inputs ) {
    print OUT "    'input" . $count . "' => {\n";
    print OUT "        'data_type' => '" . $inp->data_type . "',\n";
    print OUT "        'data_version' => '" . $inp->data_version . "',\n";
    print OUT "        'need' => '" . $inp->need . "',\n";
    print OUT "        'lun' => '" . $inp->lun . "',\n";
    print OUT "        'timer' => " . $inp->timer . ",\n";
    print OUT "        'currency' => '" . $inp->currency . "',\n";
    print OUT "        'coverage' => " . $inp->coverage . ",\n";
    print OUT "        'boundary' => '" . $inp->boundary . "',\n";
    print OUT "    },\n";
    $count++;
}
print OUT ");\n";

print OUT "\n\%outputs = (\n";
my $count = 1;
foreach my $outp ( @outputs ) {
    print OUT "    'output" . $count . "' => {\n";
    print OUT "        'data_type' => '" . $outp->data_type . "',\n";
    print OUT "        'data_version' => '" . $outp->data_version . "',\n";
    print OUT "        'lun' => '" . $outp->lun . "',\n";
    print OUT "        'currency' => '" . $outp->currency . "',\n";
    print OUT "        'coverage' => " . $outp->coverage . ",\n";
    print OUT "        'boundary' => '" . $outp->boundary . "',\n";
    print OUT "    },\n";
    $count++;
}
print OUT ");\n";

print OUT "\n# REVIEW THE USES BELOW. THE ONES HERE NOW MAY BE WRONG!:\n";
print OUT "\%input_uses = (\n";
foreach my $dt ( keys %inputs ) {
    print OUT "   '$dt' => " . get_uses($algorithm->algorithm_name, $dt, "in") . ",\n";
}
print OUT ");\n";

print OUT "\n# REVIEW THE USES BELOW. THE ONES HERE NOW MAY BE WRONG!:\n";
print OUT "\%output_uses = (\n";
foreach my $dt ( keys %outputs ) {
    print OUT "   '$dt' => " . get_uses($algorithm->algorithm_name, $dt, "out") . ",\n";
}
print OUT ");\n";

#############################################

#$preselect_data_args = '-i 7200 -threshold 2/8 -timer 86400';
#$trigger_block_args = '../s4pm_regular_block.pl -b START_OF_DAY -d 86400 -t 0';
#############################################

print OUT "\n\@stats_datatypes = $stats_dts" . ";\n";
print OUT "\n# REVIEW THE CHOICE OF AN INDEX DATA TYPE BELOW AND CHANGE IF NEEDED\n";
my @parts = split(/\s/, $stats_dts);
my $p = $parts[0];
$p =~ s/^\(//;
$p =~ s/,$//;
print OUT "\$stats_index_datatype = $p;\n";

print OUT "\n# UNCOMMENT THE LINE BELOW TO HAVE ALTERNATE RUN ALGORITHM STATION\n# FOR THIS ALGORITHM AND SET APPROPRIATELY:\n";
my $sta = get_station($algorithm->algorithm_name);
if ( $sta ) {
    print OUT "\$algorithm_station = '$sta';\n";
} else {
    print OUT "#\$algorithm_station = '[STATION DIRECTORY]';\n";
}

print OUT "\n# IF APPROPRIATE, COMMENT OUT LINES BELOW TO SET UP ALGORITHM FOR\n# PRE-SELECT DATA AND DEFINE BLOCKS:\n";
my $pargs = get_preselect_data_args($algorithm->algorithm_name);
if ( $pargs ) {
    print OUT "\$preselect_data_args = '$pargs';\n";
} else {
    print OUT "#\$preselect_data_args = '-i 7200 -threshold 2/8 -timer 86400';\n";
}
my $bargs = get_block_args($algorithm->algorithm_name);
if ( $bargs ) {
    print OUT "\$trigger_block_args = '$bargs';\n";
} else {
    print OUT "#\$trigger_block_args = '../s4pm_regular_block.pl -b START_OF_DAY -d 86400 -t 0';\n";
}

my $special = $algorithm->specialized_criteria;
print OUT "\n\%specialized_criteria = (\n";
foreach my $key ( keys %{$special} ) {
    print OUT "    '$key' => '" . $special->{$key} . "',\n";
}
print OUT ");\n\n";

my $checknspec = is_checknspec($algorithm->algorithm_name);
print OUT "\n\$check_n_spec_algorithm = $checknspec;\n";

print OUT "\n1;\n";

close OUT;

print "\nConversion has been completed. You will need, however, to review the\nresulting file before you can declare success.\n\n";

sub get_station {

    my $algorithm = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('%pge_station');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    if ( exists $CFG::pge_station{$algorithm} ) {
        return $CFG::pge_station{$algorithm}
    } else {
        return undef;
    }
}
sub get_uses {
  
    my $algorithm = shift;
    my $datatype  = shift;
    my $dir       = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('%pge_input_uses', '%pge_output_uses');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    if ( $dir eq "in" ) {
        return  $CFG::pge_input_uses{$algorithm}{$datatype};
    } else {
        return $CFG::pge_output_uses{$algorithm}{$datatype};
    }
}

sub get_exec {
  
    my $algorithm = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('%pge_exec');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    return $CFG::pge_exec{$algorithm};
}

sub get_preselect_data_args {

    my $algorithm = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('%prespecify_data_pges');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    if ( exists $CFG::prespecify_data_pges{$algorithm} ) {
        return $CFG::prespecify_data_pges{$algorithm}
    } else {
        return undef;
    }
}

sub get_block_args {

    my $algorithm = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('%trigger_block');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    if ( exists $CFG::trigger_block{$algorithm} ) {
        return $CFG::trigger_block{$algorithm}
    } else {
        return undef;
    }
}

sub is_checknspec {

    my $algorithm = shift;

    my $compartment = new Safe 'CFG';

    $compartment->share('@check_n_spec_pges');
    $compartment->rdo($opt_c) or die "Cannot import $opt_c: $!\n\n";

    foreach my $c ( @CFG::check_n_spec_pges ) {
        if ( $c eq $algorithm ) {
            return 1;
        }
    }

    return 0;

}


