'''------------------------------------------------------------------------
SUMMARY OF THE PROGRAM: This module moves data local VM to Azure Blob storage
It has methods to performs following action:
    - Initiate connection to Azure storage
    - Copy contents of input directory from local VM to Azure Blob
    - Delete files from Azure blob, if requested
    
---------------------------------------------------------------------------------------
VERSION        Date            Comments
---------------------------------------------------------------------------------------
PROGRAM
---------------------------------------------------------------------------------------
'''
import logging

from os import listdir

import codebase.dataload.cfg as cfg
from azure.storage.blob import BlockBlobService
import codebase.dataload.exceptions as ex


class AzureLoad():
    ''' This class has methods for transferring files from Azure '''
    
    def __init__(self):
        ''' Initiates the logger for source environment
        :param:
        :returns:
        :raises:
        '''
        self.LOGGER = logging.getLogger(__name__)
        
        
    def transferFromVMToAzure(self, storageAccountNm, storageAccountKey, loadDataCntnr, sourceDataDir, destDataDir):
        ''' Copy files from local to Azure Blob storage
        
        :param str storageAccountNm: Name of storage account
        :parm str storageAccountKey: Storage account key
        :parm str loadDataCntnr: Load Data Container
        :parm str sourceDataDir: Directory location of source data
        :parm str destDataDir: Directory of target data
        
        :returns:
        
        :raises Exception DataloadApplicationException: Error while connecting to blob storage 
        '''    
        ''' List all the files in the input directory on VM '''
        listAllFile = listdir(sourceDataDir)
        fileCounter = 0
        
        try:
            ''' connect to the Azure Blob storage account'''        
            blobConnect = BlockBlobService(account_name=storageAccountNm,
                                           account_key=storageAccountKey)
        except Exception as e:
            errorMsg = cfg.err.getErrorMsg('AZR_CONNECTION_ERR')
            errorMsg = errorMsg.format(type(e), str(e))
            raise ex.DataloadApplicationException(errorMsg)
        else:
            self.LOGGER.debug ("connected to Blob Storage - {} container - {}".format(storageAccountNm, loadDataCntnr))

        try:
            ''' move each file sequentially from VM to Azure Blob storage '''
            for fileNm in listAllFile:
                sourceFile = sourceDataDir + fileNm
                destBlob = destDataDir + fileNm
                self.LOGGER.debug ("Uploading File= {} from source= {} as blob= {}"
                              .format(fileNm, sourceDataDir, destBlob)) 
                              
                blobConnect.create_blob_from_path(loadDataCntnr,
                                                  destBlob, 
                                                  sourceFile,
                                                  validate_content=True)
                fileCounter += 1
        except Exception as e:
            errorMsg = cfg.err.getErrorMsg('AZR_FILE_LOAD_ERR')
            errorMsg = errorMsg.format(sourceFile, type(e), str(e))
            raise ex.DataloadApplicationException(errorMsg)
        
        return fileCounter
    
    
    def clearFileFromAzure(self, storageAccountNm, storageAccountKey, loadDataCntnr, destDataDir):
        ''' Delete all files in given Azure directory
        
        :param str storageAccountNm: Name of storage account
        :parm str storageAccountKey: Storage account key
        :parm str loadDataCntnr: Load Data Container
        :parm str destDataDir: Target directory
        
        :returns:
        
        :raises Exception DataloadApplicationException: Error while connecting to blob storage
        '''
        
        try:
            ''' connect to the Azure Blob storage account'''        
            blobConnect = BlockBlobService(account_name=storageAccountNm,
                                            account_key=storageAccountKey)
        except Exception as e:
            errorMsg = cfg.err.getErrorMsg('AZR_CONNECTION_ERR')
            errorMsg = errorMsg.format(type(e), str(e))
            raise ex.DataloadApplicationException(errorMsg)
        
        try:
            ''' Get list of all file at a give path, in Azure Blob '''               
            listAllFile = blobConnect.list_blobs(container_name=loadDataCntnr, prefix=destDataDir)
            
        except Exception as e:
            errorMsg = cfg.err.getErrorMsg('AZR_FILE_LIST_ERR')
            errorMsg = errorMsg.format(destDataDir, type(e), str(e))
            raise ex.DataloadApplicationException(errorMsg)
            
        try:
            ''' Delete file from Azure Blob Storage ''' 
            for val in listAllFile:
                blobConnect.delete_blob(loadDataCntnr, val.name)
                self.LOGGER.debug("Deleted blob {}".format(val.name)) 
            
        except Exception as e:
            errorMsg = cfg.err.getErrorMsg('AZR_FILE_DELETE_ERR')
            errorMsg = errorMsg.format(val, type(e), str(e))
            raise ex.DataloadApplicationException(errorMsg)
            
