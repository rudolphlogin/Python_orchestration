#!/bin/ksh  
###############################################################################
#
#  PURPOSE:				Copy cpsmyid data from FTP Location
#  USAGE:       		cdp_ftp_ext.sh
#  DEPENDENCY: 			LEVEL is defined to "dev","test", or "prod".
#  AUTHOR: 				Rudolph
#  Changes:
# 
###############################################################################

###############################################################################
#   START HERE - MAIN SCRIPT
############################################################################### 

LOGDIR='/home/cdpappuat/logs/datorama/'
LOGFILE="cdp_datorama_ext-`date +%Y-%m-%d-%H:%M`.log"

TodayDate=`date +%Y%m%d`
USRID='redacted'
PASWD='redacted'
HOST='ftp'
PORT='10022'
SRCDIR='/'
TMPDIR='/home/cdpappuat/SrcFiles_tmp/cdp/datorama'

############################################################################
######## Deleting previous files from tmp location on HD cluster############
############################################################################

echo "Changing directory to delete previous datorama files from HD Cluster  at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
cd ${TMPDIR}

cdres=$?

if [ "$cdres" != "0" ]  
then    
  echo "***Error*** change directory to delete previous datorama files from HD Cluster failed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
  exit 1
 else    
  echo "Success - changed directory to ${TMPDIR}  at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
fi


echo "Deleting folders and files from datorama HD location at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
rm MM*

filedel=$?

if [ "$filedel" != "0" ]  
then    
  echo "***Error*** deleting previous datorama files from HD Cluster failed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
 else    
  echo "Success - deleted previous datorama files from HD Cluster  at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
fi


### Fetching files form source ftp server 

echo "Connecting to source ftp server and fetching source files started at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
sshpass -p $PASWD sftp -oKexAlgorithms=diffie-hellman-group14-sha1 -oPort=$PORT $USRID@$HOST  << !   &>> ${LOGDIR}${LOGFILE}
ls -ltrh
lcd ${TMPDIR}
!date
mget MMIndustrial_Brand_Offer.csv 
bye 
!

ftpresult=$?

if [ "$ftpresult" != "0" ]  
then    
  echo "****FTP Error**** occurred at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
  exit 1
 else    
  echo "FTP Script to get datorama files completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
fi



##unzip the source file 
##commented on 31 oct as .zip files are replaced by .csv files

#cd ${TMPDIR}
#unzip *.zip

#unzrc=$?

#if [ "$unzrc" != "0" ]  
#then    
#  echo "****Unzip Error**** occurred at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
#  exit 1
# else    
#  echo "unzip completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
#fi


##############################################################################
#	Cleansing Starts
#############################################################################

python /home/cdpappuat/codebase/dataload/source/drcleanup.py ${TMPDIR}/

###############################################################################
#   ADL Data copy starts here
############################################################################### 

echo "Creating folder with name `date +%Y%m%d` on raw ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}

hadoop fs -mkdir adl://cdpuat.azuredatalakestore.net/cdp_raw_store/datorama/$TodayDate

echo "Completed creating folder with name `date +%Y%m%d` on raw ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}

echo "Creating folder with name `date +%Y%m%d` on cleansed  ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}

hadoop fs -mkdir adl://cdpuat.azuredatalakestore.net/cdp_raw_store/datorama/cleansed/$TodayDate

echo "Completed creating folder with name `date +%Y%m%d` on cleansed  ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}

fldr=$?

if [ "$fldr" != "0" ]  
then    
  echo "****Folder creation error**** at  `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
  exit 1
 else    
  echo "All Folder creation completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
fi


echo "Copying files to raw ADL started at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}

hadoop fs -put /home/cdpappuat/SrcFiles_tmp/cdp/datorama/*.csv adl://cdpuat.azuredatalakestore.net/cdp_raw_store/datorama/$TodayDate/

echo "Copying files to raw ADL completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}

echo "Copying files to cleansed ADL started at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}

hadoop fs -put /home/cdpappuat/SrcFiles_tmp/cdp/datorama/cleansed/*.csv adl://cdpuat.azuredatalakestore.net/cdp_raw_store/datorama/cleansed/$TodayDate/

echo "Copying files to cleansed ADL completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}



#############################################
########   ADL archive data copy ############
#############################################

echo "Creating folder with name `date +%Y%m%d` on archive ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}
hadoop fs -mkdir adl://cdpuat.azuredatalakestore.net/cdp_archive/datorama/$TodayDate
echo "Completed creating folder with name `date +%Y%m%d` on archive  ADL for datorama source at `date +%Y-%m-%d-%H:%M` " >> ${LOGDIR}${LOGFILE}


echo "Copying files to archive ADL started at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}
hadoop fs -put /home/cdpappuat/SrcFiles_tmp/cdp/datorama/*.csv adl://cdpuat.azuredatalakestore.net/cdp_archive/datorama/$TodayDate/
echo "Copying files to archive ADL completed at `date +%Y-%m-%d-%H:%M`" >> ${LOGDIR}${LOGFILE}

##############################################



echo "Removing all previous partitions " >> ${LOGDIR}${LOGFILE}
hive -e "alter table cdp_raw_data.rw_dr_media drop partition (file_dt > '0')"; 

echo "Adding new partition for ${TodayDate} date" >> ${LOGDIR}${LOGFILE}
hive -e "alter table cdp_raw_data.rw_dr_media add partition (file_dt="$TodayDate") location 'adl://cdpuat.azuredatalakestore.net/cdp_raw_store/datorama/cleansed/"$TodayDate"'"; 

echo "-----------End of Script------------"  >> ${LOGDIR}${LOGFILE}
