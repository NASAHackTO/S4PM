# Test s4pm_run_easy.pl
# Author:  Chris Lynnes

################################################################################
# run_easy.t,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Test::More tests => 7;

# Locate proper version of script
use FindBin;
my $script = "$FindBin::Bin/../blib/script/s4pm_run_easy.pl";
ok((-f $script), "found script");

# Make temporary directory
my $tmpdir = "/var/tmp/tmp.run_easy.$$";
if (! -d $tmpdir) {
    mkdir($tmpdir) or die "Cannot mkdir $tmpdir: $!";
}

# Run the tests
run_test($tmpdir, $script, 1);  # with %cfg_output
run_test($tmpdir, $script, 0);  # without %cfg_output

# Cleanup
$rc = system("/bin/rm -rf $tmpdir");
warn "Cannot rmdir $tmpdir: $!" if $rc;
exit(0);

sub run_test {
    my $tmpdir = shift;
    my $script = shift;
    my $use_cfg_output = shift;
    chdir($tmpdir) or die "Cannot chdir $tmpdir: $!";

    # Write config file
    my $outfile = "echo.out";
    my $config_file = write_config_file($use_cfg_output ? $outfile : undef);

    my $cmd = "cp.pl";
    open CMD, ">$cmd" or die "Cannot write to $cmd: $!";
    print CMD << "EOF";
open OUT, ">$outfile";
while (<>) { print OUT; }
close OUT;
exit(0);
EOF
    close CMD;

    my $pcf_path = "tmp.pcf";
    write_pcf($pcf_path);
    $ENV{'PGS_PC_INFO_FILE'} = $pcf_path;
    my $rc = system("$script -l -v perl cp.pl");
#   my $rc = system("/tools/gdaac/COTS/bin/perl -d $script -v perl cp.pl");
    if ($rc != 0) {
        warn "System call of s4pm_run_easy.pl failed: rc=$rc\n";
    }
    ok{$rc == 0, "exec s4pm_run_easy.pl"};
    die unless ($rc == 0);
    foreach my $tf($pcf_path, $cmd, $config_file, "echo.out") {
        if (-f $tf) {unlink $tf or die "Cannot unlink $tf: $!"};
    }
    chdir('..') or die "Cannot chdir to ..: $!";
    my $data = 'MOD02QKM.A2003053.0320.004.2003053160831.hdf';
    my $lgid = get_lgid("$data.xml");
    my $expected = $use_cfg_output 
      ? qr/MOD02QKM.A2003053.0330.004.\d\d\d\d\d\d\d\d\d\d\d\d\d.out/
      : qr/MOD02QKM.A2003053.0320.004.\d\d\d\d\d\d\d\d\d\d\d\d\d.tar/;
    like($lgid, $expected, 'Check local granule_id');

    unlink($data) or warn "Cannot unlink $data: $!";
    ok(-f "$data.xml");
#   open DATA, "$data.xml";
#   warn "\n";
#   while (<DATA>) {
#       warn $_;
#   }
#   warn "\n";
    unlink("$data.xml") or warn "Cannot unlink $data.xml: $!";
    return;
}
sub get_lgid {
    my $met = shift;
    if (!open MET, $met) {
        warn "Cannot open metfile $met: $!";
        return;
    }
    local($/) = undef;
    my $s = <MET>;
    close MET;
    my ($lgid) = ($s =~ m#<LocalGranuleID>(.*?)</LocalGranuleID>#i);
    return $lgid;
}
sub write_config_file {
    my $outfile = shift;
    my $config_file = "run_easy.cfg";
    open CONFIG, ">$config_file" or die "Cannot write to $config_file: $!";
    print CONFIG << 'EOF';
%cfg_static_input = ( 999990 => 'RUNTIME_PARAMETERS');
%cfg_dynamic_input = ( 700050 => 'MOD02SSH');
$cfg_use_midtime_in_name = 1;
EOF
    print CONFIG ("\%cfg_output = (700000 => '$outfile');\n") if ($outfile);
    close CONFIG;
    return $config_file;

}
sub write_pcf {
    my $filename = shift;
    open OUT, ">$filename" or die "Cannot write to $filename: $!";
print OUT << 'EOF';
?   SYSTEM RUNTIME PARAMETERS
# Production Run ID - unique production instance identifier
1
# Software ID - unique software configuration identifier
1
?   PRODUCT INPUT FILES
!  DATA
700050|MOD02SSH.A2003053.0330.004.2003053162444.hdf|DATA||MOD02SSH.A2003053.0330.004.2003053162444.hdf|MOD02SSH.A2003053.0330.004.2003053162444.hdf.met|3
700050|MOD02SSH.A2003053.0325.004.2003053160411.hdf|DATA||MOD02SSH.A2003053.0325.004.2003053160411.hdf|MOD02SSH.A2003053.0325.004.2003053160411.hdf.met|2
700050|MOD02SSH.A2003053.0320.004.2003053160831.hdf|DATA||MOD02SSH.A2003053.0320.004.2003053160831.hdf|MOD02SSH.A2003053.0320.004.2003053160831.hdf.met|1
999990|RUNTIME_PARAMETERS|.||RUNTIME_PARAMETERS||3
?   PRODUCT OUTPUT FILES
! /usr/modis/RUN/output/PGE02/Function
700000|MOD02QKM.A2003053.0320.004.2003053160831.hdf|..||||1
10255|asciidump|||||1
?   SUPPORT INPUT FILES
!  ~/runtime
411411|run_easy.cfg|.||run_easy.cfg|run_easy.cfg|1
10301|leapsec.dat|~/database/common/TD||||1
10401|utcpole.dat|~/database/common/CSC||||1
10402|earthfigure.dat|~/database/common/CSC||||1
10601|de200.eos|~/database/sgi32/CBP||||1
10801|sc_tags.dat|~/database/common/EPH||||1
?   SUPPORT OUTPUT FILES 
! /usr/modis/RUN/output/PGE02/run_logs
10100|LogStatus|||||1
10101|LogReport|||||1
10102|LogUser|||||1
10103|TmpStatus|||||1
10104|TmpReport|||||1
10105|TmpUser|||||1
10110|MailFile|||||1
10111|ShmMem|||||1
?   USER DEFINED RUNTIME PARAMETERS
10114|Logging Control; 0=disable logging, 1=enable logging|1
10115|Trace Control; 0=no trace, 1=error trace, 2=full trace|0
10116|Process ID logging; 0=don't log PID, 1=log PID|0
10118|Disabled seed list|0
10119|Disabled status code list|0
10220|Toolkit version string|DAAC B.0 TK5.2.1
10507|ephemeris data quality flag mask|65536
10508|attitude data quality flag mask|65536
10911|ECS DEBUG; 0=normal, 1=debug|0
10258|Start Time|2003-02-22T03:00:00Z
10259|Stop Time|2003-02-22T04:00:00Z
?   INTERMEDIATE INPUT
!  ~/runtime
?   INTERMEDIATE OUTPUT
! /usr/modis/RUN/output/PGE02/Function
?   TEMPORARY I/O
! /usr/modis/RUN/output/PGE02/Function
?   END
EOF
    close OUT;
}
