#!/bin/ksh

set -x
##############################################################################
#                                                                            
# Name        : s4pm_run_DCLI.ksh                                     
# Author      : Yangling Huang                                                            
# Description : invoke DCLI.                  
# Date        : May. 27, 2003                                                
#                                                                            
# HISTORY                                                                    
# Date       Who              What                                           
# ----       ---------        --------------------                           
#                                                                            
##############################################################################

################################################################################
# s4pm_run_DCLI.ksh,v 1.2 2006/09/12 20:31:39 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

SUBSYS=DSS
COMPONENT=EcDsSr
EXECUTABLE=EcDsDdDCLI

[ $# -ne 5 ] && print "Usage: $0 MODE [order id] [request id] [dist.in] [param.in ]" && exit


MODE=$1
ORDERID=$2
REQUESTID=$3
DISTFILE=$4
PARAMFILE=$5

# Set up mode directories.
SCRDIR=${ECS_HOME}/${MODE}/CUSTOM/utilities

EcCoScriptlib check_dirs $SCRDIR || exit

COMMON_ENV=EcCoEnvKsh                   # Common environment file.

# Check for the existence of the common environment file.
[ -f "${SCRDIR}/${COMMON_ENV}" ] || {
	print "Environment file \"${SCRDIR}/${COMMON_ENV}\" does not exist. Aborting ..."
	exit $ERROR_MISSING_COMMON_ENV_FILE
}

# Read in the common environment file.
. ${SCRDIR}/${COMMON_ENV}

# Call EcDsSdsrvTest.
$ECS_HOME/$MODE/CUSTOM/bin/$SUBSYS/$EXECUTABLE ${MODE} ${ORDERID} ${REQUESTID} ${DISTFILE} ${PARAMFILE}
