#!/bin/bash
###############################################################################
#
#  PURPOSE: Orhestrate daily job to get and process data coming in from ftp
#  USAGE:   Master.sh
#  AUTHOR:  Rudolph
#  Changes:
#
##############################################################################

###############################################################################
#   START HERE - MAIN SCRIPT
###############################################################################

LOGDIR='/home/cdpappuat/logs/orchestration/'
LOGFILE="cdp_datorama_-`date +%Y-%m-%d-%H:%M`.log"

echo "`date +%Y-%m-%d-%H:%M`:Datorama - Executing cdp_ftp_ext.sh " >> ${LOGDIR}${LOGFILE}

sh /home/cdpappuat/codebase/Scripts/cdp_ftp_ext.sh >> ${LOGDIR}${LOGFILE}

if [ "$?" != "0" ]
then
  echo "`date +%Y-%m-%d-%H:%M`:Datorama ***Error*** executing script cdp_datorama_ext.sh $res" >> ${LOGDIR}${LOGFILE}
  exit 1
else
    echo "`date +%Y-%m-%d-%H:%M`:Datorama Executed cdp_datorama_ext.sh and now executing dataload_ups.py" >> ${LOGDIR}${LOGFILE}
    python /home/cdpappuat/codebase/dataload/source/dataload_ups.py 'datorama' 'dr_media' >> ${LOGDIR}${LOGFILE}
    if [ "$?" != "0" ]
    then
       echo "`date +%Y-%m-%d-%H:%M`:Datorama ***Error*** executing script dataload_ups.py " >> ${LOGDIR}${LOGFILE}
    else
       echo "`date +%Y-%m-%d-%H:%M`:Datorama Execution completed successfully" >> ${LOGDIR}${LOGFILE}
    fi
fi

