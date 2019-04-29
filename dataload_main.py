'''
# --------------------------------------------------------------------------------------------------------------- ----  
# SUMMARY OF THE PROGRAM:    
# This shell script      
#                get feed information from database
#                generate feed names based on the date
#                copy data from temp hive table to main hive table for each date folder
#                get row count from main table for each date.
#                add execution status in app_run_stat database
#                add exception logs to error table
#        
# USAGE:        following arguments need to be pass:
                
#                 {ZoneName}     =     'ZONE'
#                 {country}     =      'US'
#                 {SourceEnv}   =      'KRUX
#                             
# PROGRAM:
#----------------------------------------------------------------------------------------------------------------------
'''
''' python libraries '''
import commands
import math
import datetime as dt
import string

''' application libraries'''    
import codebase.dataload.cfg as cfg
from codebase.dataload.env import ArgParser
from codebase.dataload.log import ABILogger
import codebase.dataload.util as util
import codebase.dataload.exceptions as ex
from codebase.dataload.constant import DB_NULL, STATUS_SUCCESS, STATUS_FAILED, STATUS_STARTED #,PGM_CLASSPATH
from azure.storage.blob import BlockBlobService
from codebase.dataload.processor.postprocess.Archive import Archive
from codebase.dataload.azureutils import AzureUtils


''' instantiating global methods '''

LOGGER = ABILogger('codebase.dataload.processor.dataload')
parser = ArgParser()
args = parser.getArgsForDataLoad()

'''  Load data from Temp table to main table'''

class Dataload():
    
    def __init__(self):
        global LOGGER
        
        self.zone = args.zone
        self.country = args.country
        self.sourceEnv = args.sourceEnv
        self.workflowId = args.workflowId
        self.programName = args.programName
        self.processName = args.processName
        
        self.executionId = 0

        LOGGER = ABILogger('codebase.dataload.source.dataload',logFileName=self.sourceEnv)
        self.LOGGER = LOGGER
    
    def getMappingIdByPorgramANDProcess(self):
        ''' get Process id
        :param str processName: process Name
        :returns str processId: process Id
        :raises DataloadSystemException: Exceptions while deleting data from directory
        '''
        self.LOGGER.debug("Get Process id")
        try:
             
            
            processMapId = util.getMappingIdByPorgramANDProcessName(self.programName, self.processName)        
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('DATALOAD_SET_PRCSID_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        self.LOGGER.debug("Process Map Id is {}".format(processMapId))
        
        return processMapId
        
    def setExecutionId(self,execDt,processID,statusCd,result,post_run_ct,eligible_next_run,execution_id):
        ''' makes entry in the app Run stat and returns the execution id
        :param int sourceId: source id
        :param int feedId: feed id
        :param int processID: process id
        :param date execStartDt: execution start date
        :param str statusCd: status code
        :returns :
        :raises DataloadSystemException: Exceptions while deleting data from directory
        '''
        self.LOGGER.debug("Set execution id")
        try:
            executionId = util.logRunStat(result.source_id,
                                          result.feed_id,
                                          processID,
                                          self.workflowId,
                                          execDt,
                                          statusCd,
                                          post_run_ct,
                                          eligible_next_run,execution_id )
            
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('DATALOAD_SET_EXECID_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        
        self.LOGGER.debug("Execution id {}".format(executionId))
        return executionId
    
    def setInitialExecId(self,processID,statusCd,execution_id):
        ''' makes entry in the APP_RUN_STAT to mark beginning of load data process
        :param:
        :returns : executionid
        :raises DataloadSystemException: Exceptions while making entry in the table
        '''        
        self.feedId     =   DB_NULL
        self.statusCd   =   statusCd
        self.sourceId   =   DB_NULL
        self.processId  =   processID
        startDate       =   dt.date.today()
        startDate       =   startDate.strftime('%Y-%m-%d')
        initialDate     =   DB_NULL
        postRunCt       =   0
        eligible_next_run=DB_NULL
        workflowid      =  self.workflowId

        
        self.LOGGER.debug("Set Initial execution id")
            
        try:
            executionId = util.logRunStat(self.feedId,
                                          self.sourceId,
                                          self.processId,
                                          workflowid,
                                          initialDate,
                                          self.statusCd,
                                          postRunCt,eligible_next_run,execution_id)
            #return executionId
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('DATALOAD_SET_EXECID_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
            
        
        self.LOGGER.debug("Initial execution id {}".format(executionId))
        return executionId

        
    def getConfigFromDb(self,executionId): 
        
        ''' get feed details from database    
        :param str zone: the zone for which data is to be copied
        :parm str country: the country for which data is to be copied
        :parm str sourceEnv: the source environment for which data is to be copied
        
        :returns list resultSet: All the active rows from source_config and feed_config
        
        :raises Exception e: Critical exception in database connectivity 
        '''
        self.LOGGER.info("get feed details from database")   
        self.LOGGER.debug("Supplied arguments are zone={}, country={}, platform={}".format(self.zone, self.country, self.sourceEnv))
        query = cfg.query.getQueryFeedDetail().format(self.zone, self.country, self.sourceEnv)
        self.LOGGER.debug("select query is: {} ".format(query)) 
        
        try:
            resultSet = util.getResults(query)
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('DATALOAD_GET_CONFIG_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        
        else:
            self.LOGGER.debug("found {} feed files for - {} ".format(len(resultSet),self.sourceEnv))
            return resultSet
         
    def loadMainTable(self,execDt,result):
        ''' transfere data from temp table to main table
        :param:
        :returns : 
        :raises DataloadSystemException: Exceptions while making entry in the table
        '''        

        self.LOGGER.info("Start Loading data in Main Hive Table : {}  for Date {}".format(result.hive_main_tbl_nm,execDt))
        try:
            quotedLoadMainTable="/home/cdpappdev1/codebase/dataload/source/hivemaintableload.hql"
            hiveMainTableColumnName = result.hive_main_tbl_column_nm
            hiveTempTableColumnName = result.hive_temp_tbl_column_nm
            yyyy= dt.datetime.strptime(str(execDt),'%Y%m%d').year   
            qtr = int(math.ceil(float(dt.datetime.strptime(str(execDt),'%Y%m%d').month)/3))
            
            cmd = "hive -hiveconf year=" + str(yyyy) + " -hiveconf qtr=" + str(qtr) + " -hiveconf load_date=" + execDt + " -hiveconf  load_maintablename=" + result.hive_main_tbl_nm + " -hiveconf load_zone_nm=" + result.zone_nm + " -hiveconf load_country_cd=" + result.country_cd + " -hiveconf load_temptablename=" + result.hive_temp_tbl_nm + " -hiveconf load_columnname=" +  hiveMainTableColumnName + " -hiveconf temp_columnname=" + hiveTempTableColumnName +  "  -f " + quotedLoadMainTable

            status, output = commands.getstatusoutput(cmd)
            
        except ex.DataloadApplicationException as dae:
            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_HIVE_QUERY_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadSystemException(errorMsg,code=errorCd)
        else:
            if status > 0:
                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_HIVE_QUERY_ERR')
                message = output[0:3900]
                message = string.replace(message,"'","")
                errorMsg = errorMsg.format(message)
                raise ex.DataloadSystemException(errorMsg,code=errorCd)
            else:
                self.LOGGER.debug("Data loaded in main table : {} for filedate : {}".format(result.hive_main_tbl_nm, execDt))
                           
    def getTempHiveTableDistinctDate(self,result,executionId):
        ''' function perform following actions.. 
            - execute hdfs command to list all the files location for current data load process.
            - split the output by new line and store in a list
            - run for loop for each items in the list 
                - extract date - split by '/' and take last item 
                - execute hive table add partition command to assign partition for a date to the external table.
            - return all folders name in a list to main function.
        '''
        self.LOGGER.info("Get list of folders for which data exists in the Temp table : {}".format(result.hive_temp_tbl_nm))
        
        try:
            
            hdfsCmd = "hdfs dfs -ls '" + result.raw_feed_data_url + "'"
            output = commands.getstatusoutput(hdfsCmd)
            status = output[0]
            listOutPut=list(output)[1].split('\n',1000)
            listOutPut = listOutPut[1:]
            
                
        except ex.DataloadApplicationException as dae:
                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_BLOB_PATH_ERR')
                errorMsg = errorMsg.format(dae.message)
                raise ex.DataloadSystemException(errorMsg,code=errorCd,exeId=executionId)
        
        else:
            if status > 0:
                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_BLOB_PATH_ERR')
                message = output[1]
                errorMsg = errorMsg.format(message)
                errorMsg = string.replace(errorMsg,"'","")
                raise ex.DataloadSystemException(errorMsg,code=errorCd,exeId=executionId)
            else:
                '''  add parttion to temp hive table '''
                listDateOutPut = []
                tempTableName = result.hive_temp_tbl_nm
                
    
                for val in listOutPut:
                    try:
                        partDate = int(val.split('/')[-1])
                        listDateOutPut.append((val.split('/')[-1]).split('\n'))

                    except ex.DataloadApplicationException as dae:
                        
                        errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_FLDR_FRMT')
                        errorMsg = errorMsg.format(dae.message)
                        raise ex.DataloadApplicationException(errorMsg,code=errorCd,exeId=executionId)
              
                    else:
                        
                        try:
                            partDate = val.split('/')[-1]
                            partLoc = result.raw_feed_data_url + "/" + partDate
                            alterTableQuery=("ALTER TABLE {} add IF NOT EXISTS partition(filedate={}) LOCATION '{}';"
                                         .format(tempTableName, partDate, partLoc))
                            quotedAlterTableQuery = '"' + alterTableQuery + '"'
                            cmd = "hive -S -e " + quotedAlterTableQuery
                            status, output = commands.getstatusoutput(cmd)
                            
                        except ex.DataloadApplicationException as dae:
                            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_PART_ADD_ERR')
                            errorMsg = errorMsg.format(dae.message)
                            raise ex.DataloadApplicationException(errorMsg,code=errorCd,exeId=executionId)
                        
                        else: 
                            if status > 0:
                                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_PART_ADD_ERR')
                                
                                fm = output.index("FAILED")
                                message = output[fm:]
                                message = string.replace(message,"'","")
                                errorMsg = errorMsg.format(message)
                                
                                raise ex.DataloadSystemException(errorMsg,code=errorCd,exeId=executionId)
                            else:
                                self.LOGGER.debug("Partition Created for table : {} for filedate : {}".format(tempTableName, partDate))
                                
                return listDateOutPut
                                   
    def dropTempTablePartition(self,execDt,result):
        ''' function perform following actions.. 
                - Drop partition from table for a specific date if it exists..
        '''
        self.LOGGER.debug("Start dropping partition for table : {} for filedate : {}".format(result.hive_temp_tbl_nm, execDt))
        try:
            tempTableName = result.hive_temp_tbl_nm
            alterTableQuery="ALTER TABLE {} DROP IF EXISTS PARTITION(FileDate = {});".format(tempTableName,execDt)
            quotedAlterTableQuery = '"' + alterTableQuery + '"'
            cmd = "hive -S -e " + quotedAlterTableQuery
            status, output = commands.getstatusoutput(cmd)
            
            return status,output
        except ex.DataloadSystemException as dae:
            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_PART_DROP_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadSystemException(errorMsg,code=errorCd)
        else:
            if status > 0:
                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_PART_DROP_ERR')
                fm = output.index("FAILED")
                message = output[fm:]
                message = string.replace(message,"'","")
                errorMsg = errorMsg.format(message)
                raise ex.DataloadSystemException(errorMsg,code=errorCd)
            else:
                self.LOGGER.debug("Partition dropped for table : {} for filedate : {}".format(tempTableName, execDt))
                 
    def getMainHiveTableCount(self,execDt,result):
        ''' function perform following actions.. 
                - execute hivemaintablecount.hql
                    - count rows in table for a date
                    - write output in a file "hivecount/000000_0" 
        '''
        hqlScript="/home/cdpappdev1/codebase/dataload/source/hivemaintablecount.hql"
        cmd = "hive -hiveconf load_date=" + execDt + " -hiveconf load_maintablename=" + result.hive_main_tbl_nm + " -f " + hqlScript

        self.LOGGER.info("Get Main Hive Table : {} record count for Date {}".format(result.hive_main_tbl_nm,execDt))
        try:
            status, output = commands.getstatusoutput(cmd)
            
        except ex.DataloadApplicationException as dae:
            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_HIVE_QUERY_ERR')
            errorMsg = errorMsg.format(execDt,str(dae))
            LOGGER.error(errorCd +errorMsg, dbLoad=True)
        else:
            if status > 0:
                errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_HIVE_QUERY_ERR')
                fm = output.index("FAILED")
                message = output[fm:]
                message = string.replace(message,"'","")
                errorMsg = errorMsg.format(message)
                raise ex.DataloadSystemException(errorMsg,code=errorCd)
            else:
                self.LOGGER.debug("Main Hive Table record count for filedate : {}".format(execDt))
                return  status,output
    
    def getBlobFileCount(self,result,execDt,executionId):
        try:
            ''' extract year and qtr from the date received from feed Data folder '''
            yyyy= dt.datetime.strptime(str(execDt),'%Y%m%d').year   
            qtr = int(math.ceil(float(dt.datetime.strptime(str(execDt),'%Y%m%d').month)/3))
            ''' concatenate with target directory to make full blob path '''
            targetDataDir = '/year=' + str(yyyy) + '/qtr='+ str(qtr)
            targetDataDir=result.raw_data_store_dir + str(targetDataDir)
            LOGGER.debug("Listing files from blob location : {}".format(targetDataDir))
            destCredUsrNm=result.src_storage_acc_nm
            destCredPwd=result.src_storage_acc_key
            destCntnr = result.raw_data_store_cntnr_nm
        
            blobConnect = BlockBlobService(account_name=destCredUsrNm,
                                            account_key=destCredPwd)           
            listBlobObjFile = blobConnect.list_blobs(container_name=destCntnr, prefix=targetDataDir)
            
        except ex.DataloadApplicationException as dae:
            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_BLOB_PATH_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadSystemException(errorMsg,code=errorCd,exeId=executionId)
        else:
            return listBlobObjFile
                
    def DoCleanUp(self,result,listBlobObjFilePre,listBlobObjFilePost,executionId):
        
        self.LOGGER.debug("Delete data from Azure Blob storage")
        destCredUsrNm=result.src_storage_acc_nm
        destCredPwd=result.src_storage_acc_key
        destCntnr = result.raw_data_store_cntnr_nm
        #targetDataDir = result.raw_data_store_dir + str(targetDataDir)
        try:
            ''' list itesm from blob object '''
            listPreFileCount = []
            for val in listBlobObjFilePre:
                listPreFileCount.append(val.name)
            ''' list itesm from blob object '''
            listPostFileCount = []
            for val in listBlobObjFilePost:
                listPostFileCount.append(val.name)
            ''' put deltas in the list '''
            listFileToDelete = list(set(listPostFileCount) - set(listPreFileCount))       
        
            blobConnect = BlockBlobService(account_name=destCredUsrNm,
                                            account_key=destCredPwd)
            
            ''' Delete extra files ( post -pre) from Azure Blob Storage ''' 
            for val in listFileToDelete:
                blobConnect.delete_blob(destCntnr, val)
                self.LOGGER.debug("Deleted blob {}".format(val)) 
            
            ''' Update statistics of ORC tables after deleting files '''
            LOGGER.debug("Update Statistics for ORC table : {}".format(result.hive_main_tbl_nm))
            hiveQuery = "ANALYZE TABLE " + result.hive_main_tbl_nm + " PARTITION(year,qtr) COMPUTE STATISTICS noscan;"
            quotedHiveQuery = '"' + hiveQuery + '"'
            hiveCommand = "hive -S -e " + quotedHiveQuery
            commands.getstatusoutput(hiveCommand)  
            
        except ex.DataloadApplicationException as dae:
            errorCd, errorMsg = cfg.err.getErrorCd('DATALOAD_BLOB_PATH_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadSystemException(errorMsg,code=errorCd,exeId=executionId)
            
    def _archiveLogs(self,destCredUsrNm,destCredPwd,initialExecutionId):
        #Local method to perform application logging.
        # See: Archive().doLogArchive()
        try:
            archive = Archive()
                        
            archive.azureAccStorageKey = destCredPwd
            archive.azureAccStorageName = destCredUsrNm
            archive.azureUtils = AzureUtils(accName=destCredUsrNm, accKey=destCredPwd)
            archive.logContainer = cfg.logConf.getAzureLogContainer()
            archive.executionId = initialExecutionId
            archive.doLogArchive()
            
        except ex.ArchiveException as ae:
            self._handleException('LOAD_DATA_AZURE_HIVE_LOG_ARCHIVE_ERR', ae)
            

      
def main():
    
    ''' this method is intended to load data between temp hive table to main hive table.
        It performs following action:
            - Accept arguments
            
            - Invokes methods to:
                . get feed information from database
                . generate feed names based on the date
                . copy data from temp hive table to main hive table for each date folder
                . count main table inserted data for each date.
                . add execution status in app_run_stat database
                . add exception logs to error table
    
    :argument str zone: the zone for which data is to be copied
    :argument str country: the country for which data is to be copied
    :argument str sourceEnv: the source environment for which data is to be copied
    :returns: 
    
    :raises : 
    :raises:
    '''   
    LOGGER.info("=========================================================================")
    LOGGER.info("==========================Starting Load Data=============================")
    
    global LOGGER
    
    dataLoad=Dataload()
            
    ''' Get process id for current process '''
    prg_proc_id = dataLoad.getMappingIdByPorgramANDProcess()
    
    ''' Set execution id for current process '''
    
    try:
        initialStatusCd=STATUS_STARTED
        initialExecutionId = dataLoad.setInitialExecId(prg_proc_id,initialStatusCd,DB_NULL)
    except ex.DataloadApplicationException as dae:
        LOGGER.error(dae, dbLoad=True)
        raise
    
    ''' Get feed details from database '''
    try:
        resultConfig = dataLoad.getConfigFromDb(initialExecutionId)
    
    except ex.DataloadApplicationException as dae:
        LOGGER.error(dae, dbLoad=True)
        raise
    ''' iterate for each feeds returned in resultConfig '''
    for result in resultConfig:
                
        try:
            ''' Get list of folders to load data for each feed'''
            hiveResultSet=dataLoad.getTempHiveTableDistinctDate(result,initialExecutionId)
                        
        except (ex.DataloadApplicationException, ex.DataloadSystemException), dae:
            executionId = dataLoad.setInitialExecId(prg_proc_id,STATUS_FAILED,initialExecutionId)
            LOGGER.error(dae, dbLoad=True)
            
        else:        
            ''' iterate for each items returned in hiveResultSet    '''
            LOGGER.debug("find {} days of file to process:".format(len(hiveResultSet)))
            
            if len(hiveResultSet) > 0:
                for execDt in hiveResultSet:             
                    try:

                        '''counting files before loading data in hive '''
                        LOGGER.info("Listing files before loading data")
                        listBlobObjFilePre=dataLoad.getBlobFileCount(result,execDt[0],initialExecutionId)
                        
                        ''' Load data in main table.. '''
                        dataLoad.loadMainTable(execDt[0],result)
                                            
                        ''' drop partitions from temp table ''' 
                        dataLoad.dropTempTablePartition(execDt[0],result)
                        statusCd = STATUS_SUCCESS
                        executionId= dataLoad.setExecutionId(execDt[0],prg_proc_id,statusCd,result,0,'Y',DB_NULL)
                        
                        ''' store azure account name and key '''
                        destCredUsrNm=result.src_storage_acc_nm
                        destCredPwd=result.src_storage_acc_key
                                                
                    except ex.DataloadSystemException as  dse:
                        statusCd = STATUS_FAILED
                        initialStatusCd = STATUS_FAILED
                        executionId= dataLoad.setExecutionId(execDt[0],prg_proc_id,statusCd,result,0,'N',DB_NULL)
                        dse.exeId = executionId
                        ''' log error in db '''
                        LOGGER.error(dse,dbLoad=True)
                        ''' list blob objects after loading the data '''
                        try:                       
                            listBlobObjFilePost=dataLoad.getBlobFileCount(result,execDt[0],executionId)
                        except ex.DataloadSystemException as  dse:
                            LOGGER.error(dse,dbLoad=True)
                        ''' pass pre and post count of blobs and do the clean up in case of any difference'''
                        try: 
                            dataLoad.DoCleanUp(result,listBlobObjFilePre,listBlobObjFilePost,executionId)
                        except ex.DataloadSystemException as  dse:
                            LOGGER.error(dse,dbLoad=True)
                        
                    except ex.DataloadApplicationException as dae:
                        statusCd = STATUS_FAILED
                        initialStatusCd = STATUS_FAILED
                        executionId= dataLoad.setExecutionId(execDt[0],prg_proc_id,statusCd,result,0,'N',DB_NULL)
                        dae.exeId = executionId
                        ''' log error in db '''
                        LOGGER.error(dae, dbLoad=True)
                        ''' list blob objects after loading the data '''
                        try:                       
                            listBlobObjFilePost=dataLoad.getBlobFileCount(result,execDt[0],executionId)
                        except ex.DataloadSystemException as  dse:
                            LOGGER.error(dse,dbLoad=True)
                        ''' pass pre and post count of blobs and do the clean up in case of any difference'''
                        try: 
                            dataLoad.DoCleanUp(result,listBlobObjFilePre,listBlobObjFilePost,executionId)
                        except ex.DataloadSystemException as  dse:
                            LOGGER.error(dse,dbLoad=True)
                             
                        
         
    ''' set the final status to success or failure '''      
    if initialStatusCd == STATUS_FAILED:
        dataLoad.setInitialExecId(prg_proc_id,STATUS_FAILED,initialExecutionId)
    else:
        dataLoad.setInitialExecId(prg_proc_id,STATUS_SUCCESS,initialExecutionId)
    
    dataLoad._archiveLogs(destCredUsrNm,destCredPwd,initialExecutionId)
                           
    LOGGER.info("=========================================================================")
    LOGGER.info("============================Ending Load Data=============================")
                       
if __name__ == "__main__":     
    main()
