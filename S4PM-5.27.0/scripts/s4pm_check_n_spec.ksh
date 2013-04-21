#! /usr/bin/ksh

# Top-level script to run s4pm_airs_L0_check.pl and then s4pm_select_data.pl
################################################################################
# s4pm_check_n_spec.ksh,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

# Run the s4pm_airs_L0_check.pl first; bail if it fails.

../s4pm_airs_L0_check.pl
let L0_CHECK_RESULT=$?

# Complete success if L0_CHECK_RESULT = 0

if [ $L0_CHECK_RESULT -eq 0 ] ; then
    ../s4pm_select_data.pl $*
    let SPEC_DATA_RESULT=$?
    exit $SPEC_DATA_RESULT
fi

# if L0_CHECK_RESULT = result = 60 complete failure
# if L0_CHECK_RESULT = result = 61 incomplete previous data
# if L0_CHECK_RESULT = result = 62 incomplete current data

echo airs_L0_check.pl exited with status = $L0_CHECK_RESULT
echo Exiting without running s4pm_select_data.pl

exit $L0_CHECK_RESULT
