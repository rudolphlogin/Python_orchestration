'''
Author:            Sapient Corporation
Date:              12-09-2017
--------------------------------------------------------------------------------------
SUMMARY OF THE PROGRAM: This module moves data between cloud environments
It performs following action:
    - Accept arguments
    - Copy data from source cloud to local VM
    - Copy data from local VM to Azure Blob storage
    - clean-up data from local VM
    - Maintain detailed logs

USAGE: It requires following arguments:
    - zone: Zone e.g. UPS
    - country: Country e.g. US
    - sourceNm: Source environment e.g. DCM
    - workflowId: workflow id of parent job
    - programName: Program name e.g. dataload, dataanalytics
    - processName: Process name e.g. getFiles, dataLoad
    - startDate (optional): Date since which data is to be copied; format 'MM/DD/YYYY' 
    - endDate (optional): Date till which data is to be copied; format 'MM/DD/YYYY'
---------------------------------------------------------------------------------------
VERSION        Date            Comments
1.0            12/09/2017      Initial release
---------------------------------------------------------------------------------------
PROGRAM
---------------------------------------------------------------------------------------
'''

''' python libraries '''
import os, shutil, re, datetime

''' application libraries'''
import codebase.dataload.exceptions as ex
import codebase.dataload.cfg as cfg
import codebase.dataload.util as util
from codebase.dataload.env import ArgParser
from codebase.dataload.log import ABILogger 
from codebase.dataload.source.azurCopy import AzureLoad
from codebase.dataload.constant import DB_NULL, BLANK, STATUS_SUCCESS, STATUS_FAILED, STATUS_STARTED, \
            DEFAULT_DATE, FREQUENCY_DAILY, FREQUENCY_WEEKLY, FREQUENCY_MONTHLY, FREQUENCY_YEARLY, \
            PGM_CLASSPATH
            

''' instantiating global methods and variables '''
parser = ArgParser()
args = parser.getArgsForGetFile()
processMapId = ''
workflowId = ''


class GetExecParms():
    ''' This class has methods for initializing execution parameters '''
    
    def __init__(self):
        ''' Accepts the arguments and initialize logger
    
        :argument str zone: Zone e.g. ZONE
        :argument str country: Country e.g. US
        :argument str sourceEnv: Source environment e.g. KRUX
        :argument str workflowId: workflow id of parent job
        :argument str programName: Program name e.g. dataload, dataanalytics
        :argument str processName: Process name e.g. getFiles, dataLoad
        :argument str startDate (optional): Date since which data is to be copied; format 'MM/DD/YYYY' 
        :argument str endDate (optional): Date till which data is to be copied; format 'MM/DD/YYYY'

        :param:
        :returns:
        :raises:
        '''
        global LOGGER, workflowId
        
        ''' Input arguments '''
        self.zone = args.zone
        self.country = args.country
        self.sourceNm = args.sourceEnv
        self.workflowId = args.workflowId
        self.programName = args.programName
        self.processName = args.processName
        self.startDate = args.startDate
        self.endDate = args.endDate
    
        ''' Session parameters '''
        workflowId = self.workflowId
        
        ''' Setting defaults '''
        self.eligForNextRun = DB_NULL
        self.executionId = DB_NULL
        
        ''' Configuring logger '''
        LOGGER = ABILogger('codebase.dataload.source.getfiles',logFileName=self.sourceNm)
        self.LOGGER = LOGGER
        
        self.LOGGER.info("==================================================================")
        self.LOGGER.info("====================== Starting {} - {} ==================="
                         .format(self.processName, self.sourceNm))
        
        self.LOGGER.info("Input parameters are: zone-{} country-{} Source={}, workflow Id-{}, "
                         "program name-{}, process name-{}, start date-{}, end date-{}".format
                        (self.zone, self.country, self.sourceNm, self.workflowId, self.programName,
                         self.processName, self.startDate, self.endDate))
 
 
    def getProcessMapId(self):
        ''' Get process map id from database
        :param:
        :returns:
        :raises:
        '''
        self.LOGGER.debug("getProcessMapId")
        
        global processMapId
        
        try:
            self.processMapId = util.getMappingIdByPorgramANDProcessName(self.programName,
                                                                         self.processName)
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_SET_PRCSID_ERR')
            errorMsg = errorMsg.format(self.programName, self.processName, dae)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        if not self.processMapId:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_PRCSID_NOT_FND')
            errorMsg = errorMsg.format(self.programName, self.processName)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        self.LOGGER.info("Process Map Id is {}".format(self.processMapId))
        
        processMapId = self.processMapId
     
  
    def setUpdateInitialExecId(self, statusCode):
        ''' makes an entry in the APP_RUN_STAT to mark beginning of getFile process
        
        :param str statusCode: Status code for making default entry
        :returns :
        :raises DataloadApplicationException: Exceptions while making entry in the table
        '''
        self.LOGGER.debug("setUpdateInitialExecId")
        self.LOGGER.debug("Process execution id before invoking util is - {}" .format(self.executionId))
        
        self.statusCd = statusCode
        self.sourceId = DB_NULL
        self.feedId = DB_NULL
        startDate = DB_NULL
        postRunCt = DB_NULL
        
        try:
            self.executionId = util.logRunStat(self.sourceId,
                                          self.feedId,
                                          self.processMapId,
                                          self.workflowId, 
                                          startDate,
                                          self.statusCd,
                                          postRunCt,
                                          self.eligForNextRun,
                                          self.executionId)
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_SET_EXECID_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        self.LOGGER.info("Process execution id after invoking util is {}".format(self.executionId))
               
        
    def setDates(self):
        ''' Validates execution start and end date
            Converts the input date to datetime format
            If not provided, End date will be set to the day before current system date
            
        :param:
        :returns tuple execStartDt, execEndDt: Execution start and end date
        :raises DataloadApplicationException: Exception for invalid date format
        '''
        self.LOGGER.debug("setDates")
        
        try:
            if self.startDate != 'None':
                execStartDt = datetime.datetime.strptime(self.startDate,'%m/%d/%Y').date()
            else:
                execStartDt =  self.startDate
                
            if self.endDate != 'None':
                execEndDt = datetime.datetime.strptime(self.endDate, '%m/%d/%Y').date()
            else:
                execEndDt = datetime.date.today() - datetime.timedelta(days=1)
                     
        except ValueError:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_DATE_FRMT_ERR')
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        else:
            self.LOGGER.debug("Execution start date is-{} and end date-{}".format(execStartDt, execEndDt))

        return execStartDt, execEndDt
    

    def getConfigFromDb(self):
        ''' Get detail of all the active feeds from Source and Feed config databases
            Raises exception if no matching entry in database
            
        :param:
        :returns list resultSet: A list of rows from source_config and feed_config
        :raises DataloadApplicationException: Critical exception in database connectivity 
        '''
        self.LOGGER.debug("getConfigFromDb")
        
        query = cfg.query.getQueryFeedDetail().format(self.zone,
                                                      self.country,
                                                      self.sourceNm)
        
        self.LOGGER.debug("Select query is: {}".format(query)) 
        
        try:
            resultSet = util.getResults(query)
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_GET_CONFIG_ERR')
            errorMsg = errorMsg.format(dae.message)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        
        if not resultSet:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_NO_FEED_FOUND')
            errorMsg = errorMsg.format(self.sourceNm)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=self.executionId)
        else:
            self.LOGGER.info("found {} active feed details for {} ".format(len(resultSet), self.sourceNm))
            return resultSet
        
        
    def finishProcessing(self, processStatus):
        ''' Makes success / failure entry into APP_RUN_STAT database
            Writes to log once processing completes
        
        :param str processStatus: Process Status
        :returns:
        :raises:
        '''
        self.LOGGER.debug("finishProcessing")
        
        if processStatus == STATUS_STARTED:
            processStatus = STATUS_SUCCESS

        self.setUpdateInitialExecId(processStatus)
        
        self.LOGGER.info("Get File processing completed. Overall processing status-{}".format(processStatus))
        self.LOGGER.info("================= Finished getfiles - {} ======================".format(self.sourceNm))
        self.LOGGER.info("==================================================================")


class GetFiles():
    ''' This class has methods for transferring data between cloud environments '''
    
    def __init__(self):
        ''' initialize parameters
        :param:
        :returns:
        :raises:
        '''
        global LOGGER, processMapId, workflowId
        
        self.processMapId = processMapId
        self.workflowId = workflowId
        self.LOGGER = LOGGER
        
    
    def initializeVars(self, result):
        ''' Initialize class variables
        
        :param tuple result: one row of data from source and feed config
        :returns:
        :raises:
        '''
        self.LOGGER.debug("initializeVars")
        
        ''' source_config details '''
        self.sourceId = result.source_id
        self.zoneName = result.zone_nm
        self.countryCd = result.country_cd
        self.sourceNm = result.source_nm
        self.sourceEnvNm = result.source_env_nm
        self.sourceCredUsrnm = result.source_cred_usrnm
        self.sourceCredPwd = result.source_cred_pwd
        self.sourceHostNm = result.source_host_nm
        self.destEnvNm = result.target_env_nm
        self.destCredUsrNm = result.src_storage_acc_nm
        self.destCredPwd = result.src_storage_acc_key
        
        ''' feed_config details '''
        self.feedId = result.feed_id
        self.fileNm = result.file_nm
        self.sourceFrq=result.source_frequency
        self.sourcedayOfRun=result.day_of_run
        self.sourceCntnr = result.source_data_cntnr        
        self.sourceFeedDir = result.source_feed_dir
        self.tempDataDir = result.temp_data_dir
        self.destCntnr = result.raw_feed_cntnr_nm
        self.targetDataDir = result.raw_feed_data_dir        

        ''' Set default parameters '''
        self.executionDate = datetime.date.today()
        self.eligForNextRun = DB_NULL
        self.postRunCt = 0
        
        self.LOGGER.info("------------- Starting processing for feedId - {} --------------".format(self.feedId))


    def getStartDate(self):
        ''' Get latest execution date from database
            Set the execution day to the day after last execution
            Raise business exception, if no historical data is found
        
        :param:
        :returns date execStartDt: Start date for execution        
        :raises DataloadApplicationException: Exception in getting execution date from database
        '''
        self.LOGGER.debug("getStartDate")
        
        try:
            execStartDt = util.getLastExeDateForGetFilesByFeedId(self.feedId, 
                                                                 processMapId)
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_EXEC_DATE_ERR')
            errorMsg = errorMsg.format(dae.message)
            executionId = self.setExecutionId('FAILED')
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        else:
            if execStartDt:
                execStartDt = execStartDt + datetime.timedelta(days=1)
            else:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_FIRST_RUN_DATE_ERR')
                errorMsg = errorMsg.format(self.feedId)
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadBusinessException(errorMsg, code=erorrCd, exeId=executionId)
            
        self.LOGGER.info("Execution start date updated to-{} for feed Id-{}".format(execStartDt, self.feedId))
            
        return execStartDt


    def checkFrequency(self, executionDate):
        ''' Checks the frequency and day of execution for the file
            Validates if the file is due for given execution date
            No check is required, if frequency of execution is set to 'daily'
        
        :param str executionDate: file date
        :returns boolean processFeed: Indicates whether a feed file is due or not
        :raises DataloadApplicationException: Exception querying database
        '''
        self.LOGGER.debug("checkFrequency")
        
        self.LOGGER.debug("Validate if file is due for execution: sourceId-{}, feedId-{}, frequency-{}, date-()"
                          .format(self.sourceId, self.feedId, self.sourceFrq, executionDate))
        
        self.executionDate = executionDate
        invalidFrequency = False
        processFeed = False
        
        sourceFrqlLw = self.sourceFrq.lower()
        
        if sourceFrqlLw == FREQUENCY_DAILY.lower():
            processFeed = True
        else:
            try:
                lastExecDt = util.getLastExeDateForGetFilesByFeedId(self.feedId, 
                                                                      processMapId)
            except ex.DataloadApplicationException as dae:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_EXEC_DATE_ERR')
                errorMsg = errorMsg.format(dae.message)
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
            else:
                try:
                    if not lastExecDt:
                        lastExecDt = DEFAULT_DATE
                        lastExecDt = datetime.datetime.strptime(lastExecDt,'%m/%d/%Y').date()
                    
                    if sourceFrqlLw == FREQUENCY_WEEKLY.lower():
                        if (lastExecDt.isocalendar()[0] < executionDate.isocalendar()[0]
                        and self.sourcedayOfRun <= executionDate.isoweekday()):
                                processFeed = True
                                
                        elif (lastExecDt.isocalendar()[0] == executionDate.isocalendar()[0]
                        and lastExecDt.isocalendar()[1] < executionDate.isocalendar()[1]
                        and self.sourcedayOfRun <= executionDate.isoweekday()):
                            processFeed = True
                            
                    elif sourceFrqlLw == FREQUENCY_MONTHLY.lower():
                        if (lastExecDt.year < executionDate.year
                        and self.sourcedayOfRun <= executionDate.day):
                            processFeed = True
                            
                        elif (lastExecDt.year == executionDate.year
                        and lastExecDt.month < executionDate.month
                        and self.sourcedayOfRun <= executionDate.day):
                            processFeed = True
                            
                    elif sourceFrqlLw == FREQUENCY_YEARLY.lower():
                        if (lastExecDt.year < executionDate.year
                        and self.sourcedayOfRun <= executionDate.timetuple().tm_yday):
                            processFeed = True
                            
                    else:
                        invalidFrequency = True
                    
                except Exception as e:
                    erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_VALID_FREQ_ERR')
                    errorMsg = errorMsg.format(str(e))
                    executionId = self.setExecutionId(STATUS_FAILED)
                    raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
                
                if invalidFrequency:
                    erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_INVALID_FEED_FRQ')
                    errorMsg = errorMsg.format(self.feedId)
                    executionId = self.setExecutionId(STATUS_FAILED)
                    raise ex.DataloadBusinessException(errorMsg, code=erorrCd, exeId=executionId)

        return processFeed
          
    
    def setFileName(self, executionDate):
        ''' derive actual file names by replacing regular expression with text
        
        :param datetime executionDate: execution date
        :returns:
        :raises:
        '''
        self.LOGGER.debug("setFileName")
        
        self.absFileNm = self._deriveFeedName(self.fileNm, executionDate)
        self.absSourceFeedDir = self._deriveFeedName(self.sourceFeedDir, executionDate)
        self.absTempDataDir = self._deriveFeedName(self.tempDataDir, executionDate)
        self.absTargetDataDir = self._deriveFeedName(self.targetDataDir, executionDate)


    def getSourceModule(self):
        ''' Dynamically load modules depending on source environment
        
        :param:
        :returns str sourceClass: Source class name
        :raises:
        '''
        self.LOGGER.debug("getSourceModule")
        
        klass = self.sourceEnvNm
        moduleNm = PGM_CLASSPATH + '.' + klass.lower()
        sourceClass = util.getInstanceByName(moduleNm,klass)
        
        self.LOGGER.info("Loading source module {}".format(sourceClass))
        
        return sourceClass


    def sourceConnection(self, sourceFeed):
        ''' Invokes method to establish connection with the source system
        
        :param obj sourceFeed: Source module instance object
        :returns obj sourceConnectObj: Source connection object
        :raises DataloadApplicationException: Exception in getting execution date from database
        '''
        self.LOGGER.debug("sourceConnection")
        
        try:
            sourceConnectObj = sourceFeed.connectToSource(self.sourceHostNm,
                                                          self.sourceCredUsrnm,
                                                          self.sourceCredPwd)
            
        except (ex.DataloadApplicationException, ex.DataloadSystemException) as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_SOURCE_CONNECT_ERR')
            errorMsg = errorMsg.format(dae.message)
            executionId = self.setExecutionId(STATUS_FAILED)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        
        self.LOGGER.debug("Connection established with {}".format(self.sourceEnvNm))
        
        return sourceConnectObj
    
    
    def sourceDataCopy(self, sourceFeed, sourceConnectObj):
        ''' Invokes method to copy file from source system to the VM
            Calculates the time taken to copy files from source to VM
            Raises Business exception if no matching file is found
            
        
        :param str sourceFeed: Source module instance object
        :param str sourceConnectObj: Source connection object

        :returns:
        :raises DataloadApplicationException: Application exception
        :raises DataloadBusinessException: Business exception
        '''
        self.LOGGER.debug("sourceDataCopy")
        
        ''' If it doesn't exist already, create the directory on VM '''
        self._createDirectory(self.absTempDataDir)
        copyStartTime = datetime.datetime.utcnow()
        skipListing = False
        
        matchFileName = cfg.gfConf.skipFileListing()
        
        for values in matchFileName:
            if values == self.sourceNm:
                skipListing = True
        
        try:
            fileCount = sourceFeed.transferFeedFromSourceToVM(sourceConnectObj, 
                                                              self.sourceCntnr,
                                                              self.absFileNm,
                                                              self.absSourceFeedDir,
                                                              self.absTempDataDir,
                                                              skipListing)
        except (ex.DataloadApplicationException, ex.DataloadSystemException) as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_SOURCE_COPY_ERR')
            errorMsg = errorMsg.format(dae.message)
            executionId = self.setExecutionId(STATUS_FAILED)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        
        ''' if no matching feed is available, raise exception ''' 
        if not fileCount:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_FILE_NT_RCVD')
            errorMsg = errorMsg.format(self.absFileNm, self.executionDate)
            executionId = self.setExecutionId(STATUS_FAILED)
            raise ex.DataloadBusinessException(errorMsg, code=erorrCd, exeId=executionId)

        ''' Publish time taken to pull files '''
        copyEndTime = datetime.datetime.utcnow()
        self.postRunCt = fileCount
        timeTaken = copyEndTime - copyStartTime
        
        self.LOGGER.info("{} time taken to copy {} files from Source to VM for Feed Id-{} Date-{}"
                         .format(str(timeTaken),fileCount,self.feedId,self.executionDate))


    def targetDataCopy(self, destFeed):
        ''' invokes method to copy data from VM to Azure
        
        :param str destFeed: Destination module instance object
        :returns:   
        :raises DataloadApplicationException: Application Exception
        '''
        self.LOGGER.debug("targetDataCopy")
        
        copyStartTime = datetime.datetime.utcnow()
        
        try:
            fileCount = destFeed.transferFromVMToAzure(self.destCredUsrNm,
                                                       self.destCredPwd,
                                                       self.destCntnr,
                                                       self.absTempDataDir,
                                                       self.absTargetDataDir)
        except (ex.DataloadApplicationException, ex.DataloadSystemException), dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_TARGET_COPY_ERR')
            errorMsg = errorMsg.format(dae.message)
            executionId = self.setExecutionId(STATUS_FAILED)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)

        ''' Publish time taken to load files '''
        copyEndTime = datetime.datetime.utcnow()
        timeTaken = copyEndTime - copyStartTime
        
        self.LOGGER.info("{} time taken to copy {} files from VM to Azure for Feed Id-{} Date-{}"
                          .format(str(timeTaken),fileCount,self.feedId,self.executionDate))


    def targetDataDelete(self, destFeed):
        ''' invokes method to delete data from Azure Blob
        
        :param str destFeed: Destination module instance object
        :returns:
        :raises DataloadApplicationException: Application Exception
        '''
        self.LOGGER.debug("targetDataDelete")
        
        try:
            destFeed.clearFileFromAzure(self.destCredUsrNm,
                                        self.destCredPwd,
                                        self.destCntnr,
                                        self.absTargetDataDir)
        except (ex.DataloadApplicationException, ex.DataloadSystemException), dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_TARGET_DELETE_ERR')
            errorMsg = errorMsg.format(dae.message)
            executionId = self.setExecutionId(STATUS_FAILED)
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)

              
    def cleanupFiles(self):
        ''' Delete entire directory tree, if it exists
        
        :param:
        :returns:
        :raises DataloadSystemException: Exceptions while deleting data from directory
        '''
        self.LOGGER.debug("cleanupFiles")
        
        if os.path.exists(self.absTempDataDir):
            try:
                shutil.rmtree(self.absTempDataDir)
            except Exception as e:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_VM_DIR_SPACE_ERR')
                errorMsg = errorMsg.format(str(e))
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
        
            self.LOGGER.info("Deleted directory {}".format(self.absTempDataDir))
        else:
            self.LOGGER.warning("Cannot find directory {}".format(self.absTempDataDir))
 
        
    def setExecutionId(self, statusCd):
        ''' makes entry in the APP_RUN_STAT and returns the execution id
        
        :param str statusCd: status code
        :returns :
        :raises DataloadSystemException: Exceptions while deleting data from directory
        ''' 
        self.LOGGER.debug("setExecutionId")
        
        ''' converts the date to YYYY-MM-DD string format '''
        execDate = self.executionDate.strftime('%Y-%m-%d')
        nullExecId = DB_NULL
        
        try:
            executionId = util.logRunStat(self.sourceId,
                                          self.feedId,
                                          self.processMapId,
                                          self.workflowId, 
                                          execDate,
                                          statusCd,
                                          self.postRunCt,
                                          self.eligForNextRun,
                                          nullExecId)
            
        except ex.DataloadApplicationException as dae:
            erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_SET_EXECID_ERR')
            errorMsg = errorMsg.format(dae.message)
            self.LOGGER.error('{} {}'.format(errorMsg, str(dae)))
            raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=0)
        
        self.LOGGER.info("Status-{} for Execution id-{}, Feed Id-{}, Execution Date-{}"
                         .format(statusCd, executionId, self.feedId, execDate))
        
        return executionId
        
 
    def _deriveFeedName(self, fileNm, executionDate):
        ''' Create feed name by replacing regular expression variables with 
        appropriate date format.
        
        :param str fileNm: File name
        :param str execDtStr: Date in 'datetime' format
        :returns str fileNm: Modified file name
        :raises:
        '''
        self.LOGGER.debug("_deriveFeedName")
        
        parmsList = cfg.dateRegex()
        
        self.LOGGER.debug("Input file name-{} and date-{}".format(fileNm, executionDate))
        self.LOGGER.debug("List of regular expressions-{}".format(parmsList))
        
        for regexFormat, dateFormat in parmsList:
            execDtStr = executionDate.strftime(dateFormat)
            fileNm = re.sub(regexFormat,execDtStr,fileNm,flags=re.IGNORECASE)
        
        self.LOGGER.debug("updated file name is {}".format(fileNm))
            
        return fileNm
    
    
    def _createDirectory(self, dataDir):
        ''' Create a directory, if it doesn't exists
        
        :param str tempDataDir: directory path
        :returns:
        :raises DataloadApplicationException: Exceptions while creating directory
        '''
        self.LOGGER.debug("_createDirectory")
        
        if not os.path.exists(dataDir):
            try:
                os.makedirs(dataDir)
            except os.errno.EACCES as e:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_VM_DIR_PRMSN_ERR')
                errorMsg = errorMsg.format(dataDir, str(e))
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
            except os.errno.ENOSPC as e:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_VM_DIR_SPACE_ERR')
                errorMsg = errorMsg.format(dataDir, str(e))
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
            except Exception as e:
                erorrCd, errorMsg = cfg.err.getErrorCd('GETFIL_VM_DIR_UNKWN_ERR')
                errorMsg = errorMsg.format(dataDir, str(e))
                executionId = self.setExecutionId(STATUS_FAILED)
                raise ex.DataloadApplicationException(errorMsg, code=erorrCd, exeId=executionId)
            else:
                self.LOGGER.info("created {} directory".format(dataDir))
        else:
            self.LOGGER.debug("{} directory already exists".format(dataDir))
            

def main():
    ''' This method is intended to move data between two systems. It manages following activities:
        - Accept input arguments
        - Validate or derive the start and end dates for data copy.
        - Get source and feed parameters from database
        - Copy data from source system to local VM
        - Copy data from local VM Azure 
        - Delete data from VM once processing complete
        - Add execution status in APP_RUN_STAT table
        - Add error messages to error table
        - Maintain log of events
    :param: 
    :returns:
    :raises DataloadApplicationException: Application Exception
    :raises DataloadSystemException: System Exception
    ''' 
    
    ''' Initialize class to get execution parameters '''
    setParms = GetExecParms()
    
    processStatus = STATUS_STARTED
    
    try:
        ''' get process map id '''
        setParms.getProcessMapId()
        
        ''' set execution id for current process '''
        setParms.setUpdateInitialExecId(processStatus)
    
    except Exception as dae:
        LOGGER.exception(dae)
        raise
        
    try:
        ''' get start and end date for execution '''
        startDate, endDate = setParms.setDates()
        
        ''' Get feed details from database '''
        resultConfig = setParms.getConfigFromDb()
    
    except Exception as dae:
        LOGGER.exception(dae, dbLoad=True)
        setParms.finishProcessing(STATUS_FAILED)
        raise
    
    ''' Initialize class for transferring data between cloud environments ''' 
    getFile = GetFiles()

    for result in resultConfig:        
        ''' initialize variables ''' 
        getFile.initializeVars(result)

        ''' If not provided as argument, infer processing start date from database '''
        if startDate == 'None':
            try:
                execDate = getFile.getStartDate()
            except ex.DataloadBusinessException as dae:
                execDate = BLANK
                processStatus = STATUS_FAILED
                LOGGER.error(dae, dbLoad=True)
            except Exception as dae:
                LOGGER.exception(dae, dbLoad=True)
                setParms.finishProcessing(STATUS_FAILED)
                raise
        else:
            execDate = startDate
            
        ''' Initiate processing, if valid start date is available '''
        if execDate:
            ''' Dynamically acquire source class based on source environment name '''
            sourceClass = getFile.getSourceModule()
            
            ''' Create source class object '''
            sourceFeed = sourceClass()

            ''' Create destination class object '''
            destFeed = AzureLoad()
                
            try:
                ''' Connect to the source environment '''
                sourceConnectObj = getFile.sourceConnection(sourceFeed)
            except Exception as dae:
                processStatus = STATUS_FAILED
                LOGGER.error(dae, dbLoad=True)
            else:
                while (execDate <= endDate):
                    ''' iterate over date range to process the feed for each day '''
                    try:
                        processFeed = getFile.checkFrequency(execDate)
                    except ex.DataloadBusinessException as dbe:
                        LOGGER.error(dbe, dbLoad=True)
                    except Exception as dae:
                        LOGGER.exception(dae, dbLoad=True)
                        setParms.finishProcessing(STATUS_FAILED)
                        raise
                    
                    if processFeed:
                        ''' substitute regular express with appropriate text in file name '''
                        getFile.setFileName(execDate)
                        
                        try:
                            ''' download data to local VM '''
                            getFile.sourceDataCopy(sourceFeed, sourceConnectObj)
                        except Exception as dae:
                            processStatus = STATUS_FAILED
                            LOGGER.error(dae, dbLoad=True)
                        else:
                            ''' upload data to target location'''
                            try: 
                                getFile.targetDataCopy(destFeed)
                            except Exception as dae:
                                processStatus = STATUS_FAILED
                                LOGGER.error(dae, dbLoad=True)
                                ''' In case of failure, delete partially copied files from Azure '''
                                try:
                                    getFile.targetDataDelete(destFeed)
                                except Exception as dae:
                                    LOGGER.exception(dae, dbLoad=True)
                                    setParms.finishProcessing(STATUS_FAILED)
                                    raise
                            else:
                                ''' make success entry in APP_RUN_STAT table '''
                                getFile.setExecutionId(STATUS_SUCCESS)
                        finally:
                            ''' clean-up files from VM '''
                            getFile.cleanupFiles()
                     
                    ''' move on to next day's feed '''
                    execDate = execDate + datetime.timedelta(days=1)
                        
    setParms.finishProcessing(processStatus)


if __name__ == "__main__":
    main()
    
