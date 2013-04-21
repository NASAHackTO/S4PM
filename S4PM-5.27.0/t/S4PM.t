# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl S4PM.t'
################################################################################
# S4PM.t,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Test::More tests => 6;
BEGIN { use_ok('S4PM') };
#my $modis_file = S4PM::make_patterned_filename('^E.A^Y^j.^H^M.^V.~N.hdf',
#    "MOD021KM", "005", "2005-02-01T23:05:00Z", 0);
#like($modis_file, qr/MOD021KM.A2005032.2305.005.\d\d\d\d\d\d\d\d\d\d\d\d\d.hdf/);
my $modis_file = S4PM::make_patterned_filename(
    "MOD021KM", "005", "2005-05-01T05:05:00Z", 0);
like($modis_file, qr/MOD021KM.A2005121.0505.005.\d\d\d\d\d\d\d\d\d\d\d\d\d.hdf/);
$modis_file = S4PM::make_patterned_filename(
    "MOD021KM", "005", "2006-01-01T05:05:00Z", 0);
like($modis_file, qr/MOD021KM.A2006001.0505.005.\d\d\d\d\d\d\d\d\d\d\d\d\d.hdf/);
print STDERR "$modis_file\n";
my $aads_path = S4PM::make_patterned_filename('MOD04_L2', '5', '2002-01-06T02:05:00Z', 0,
    'allData/^V/^E/^Y/^j');
is($aads_path, 'allData/5/MOD04_L2/2002/006');
my $default_pat = S4PM::get_filename_pattern;
ok(S4PM::filename_pattern_has_datetime($default_pat));

# AIRS.2006.04.23.090.L2.RetStd.v4.0.9.0.G06114101322.hdf
my $airs_pat = "AIRS.^Y.^m.^d.*.L2.RetStd.v4.0.9.0.*.hdf";
is(S4PM::filename_pattern_has_datetime($airs_pat), 0);

print STDERR "$aads_path\n";
