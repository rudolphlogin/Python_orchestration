'''
# Author:              Sapient Corporation
# Date:                Nov-2017
# --------------------------------------------------------------------------------------------------------------- ----  
# SUMMARY OF THE PROGRAM:    
# This shell script      
#                get hive script to execute from SQL database configuration table
#                Execute hive script in a loop based on different execution criteria
#                log execution status and time in sql database table

#                             
# PROGRAM:
#----------------------------------------------------------------------------------------------------------------------
'''
''' python libraries '''
import pyodbc
import commands
import sys

''' Module level variable '''

dbDriver="{ODBC Driver 13 for SQL Server}"
dbServer="dce-db-dev.database.windows.net"
dbPort=1433
dbHost="dce-db-dev.database"
dbName="cdp_sql_db_dev"
dbUser="cdpappuser"	
dbPassword="CDPapp2017!"


''' connect to SQL Azure database '''
def getDbConnection():
    try:
        cnxn = pyodbc.connect(driver=dbDriver,
                       server=dbServer,
                       port=dbPort,
                       database=dbName,
                       uid=dbUser,
                       pwd=dbPassword)
        
                
    except Exception as e:
        raise e
    else:
        return cnxn

''' open connection to the db'''
def getCursor():
    try:
        cnxn = getDbConnection()
        cursor = cnxn.cursor()
    except Exception as e:
        raise e
    else:
        return cursor

''' excute sql query to get resultset '''
   
def getResults(query):
    '''
    Execute database select statement and return all rows
    :param - quey - query to be executed.
    
    :return - query result
    '''
    try:
        cursor = getCursor()
        cursor.execute(query)
        resultSet = cursor.fetchall()   
    except Exception as e:
        raise e
    else:
        return resultSet
    
def insertData(query):
    '''
     Execute database insert or update query
     :param - quey - query to be executed.
    '''
    try:
        cursor = getCursor()
    
        cursor.execute('SET QUOTED_IDENTIFIER OFF')
        cursor.execute(query)
        cursor.execute('SET QUOTED_IDENTIFIER ON')
    except Exception as e:
        raise e
    else:
        cursor.commit()    


def main():
    
    runfrequency = sys.argv[1]
    
    selectconfigquery = ("Select Metric_ID,Metric_query from SCH_APP.AGGR_METRIC_CONFIG"
                        "WHERE Active_flg = 'A'  --- all active queries to run"
                            "AND CAST(GETDATE() AS DATE)  BETWEEN start_dt AND end_dt"
                            "AND" 
                            "run_frequency = {}"
                        "ORDER BY ExecutionOrder ASC").format(runfrequency)
    try:
        ''' execute query to get config values from sql db config table '''
        resultconfig = getResults(selectconfigquery)
    except Exception as e:
        raise e
    else:
        ''' iterate for all the record fetch from DB and store the metric_id and Query in local variable
            use metric_query in hive statment, pass hive command to getstatusoutput, which will execute the hive command
            and return the status and execution result
        ''' 
        for result in resultconfig:
            try:
                metric_id = result[0]
                metric_query = result[1]
                cmd = "hive -e  '" + metric_query + "'"
                status, output = commands.getstatusoutput(cmd)
            except Exception as e:
                raise e
            else:
                '''capture the fail or success status in local variable, which will be use to pass this status in db table'''
                if status > 0: 
                    runstatus = 'Failed'
                else:
                    runstatus = 'Success'
                    ''' search for Time taken key word in output, if found extract the time take value 
                        this approach is to extract the acutal time taken for query execution which output string returns
                    ''' 
                    findstring = output.find("Time taken")
                    queryruntime = output[findstring+12:3000]
                insertquery ="insert into SCH_APP.AGGR_METRIC_RUN_STAT(Metric_ID,Run_Status,Query_run_time)  Values('{}','{}','{}')".format(metric_id,runstatus,queryruntime)
                try:
                    ''' call insert data method and pass insert query which will insert execution log in db table '''
                    insertData(insertquery)
                except Exception as e:
                    raise e
                else:
                    print "Query execution status has been logged in a table"
        
if __name__ == "__main__":     
    main()
      
    
    
    
    
    
    
