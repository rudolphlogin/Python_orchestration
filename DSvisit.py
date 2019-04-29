'''
# Author:              Sapient Corporation
# Date:                10-16-2017
# --------------------------------------------------------------------------------------------------------------- ----  
# SUMMARY OF THE PROGRAM:    
# This python script      
#                oAuth Call
#                Request Report
#                Poll Status
#                Pull Report
#                Load to ADL
#                
#        
# USAGE:        following arguments need to be pass:
                
#                 {startAt}     =     '2017-10-01T00:00:00Z'
#                 {endAt}     =      '2017-10-02T00:00:00Z'
#                 
#                             
# PROGRAM:
#----------------------------------------------------------------------------------------------------------------------
'''
''' python libraries '''

import ConfigParser
import requests
import sys
import json
import ast
import time
import commands
import dateutil.parser
import datetime
from datetime import timedelta

config = ConfigParser.ConfigParser()
config.read('/home/cdpappuat/codebase/config/dev/cdpAppConfig.conf')

url = config.get('doubleclickSearch', 'oAuth_end_point')
body = {}
body['Content-Type'] = 'application/x-www-form-urlencoded'
body['client_id'] = config.get('doubleclickSearch', 'client_id')
body['client_secret'] = config.get('doubleclickSearch', 'client_secret')
body['scope'] = 'https://www.googleapis.com/auth/doubleclicksearch'
body['refresh_token'] = config.get('doubleclickSearch', 'refresh_token')
body['grant_type'] = 'refresh_token'

oauthresp = requests.post(url, data=body)
print oauthresp
print oauthresp.json()
access_tkn = oauthresp.json()['access_token']
print access_tkn

start_dt=None
end_dt=None
startAt = None
endAt = None
# To Do: Use ArgParser
if (len(sys.argv) == 3):
	start_dt = sys.argv[1]
	end_dt = sys.argv[2]
	startAt = datetime.datetime.strptime(start_dt, '%Y-%m-%d') 
	endAt = datetime.datetime.strptime(end_dt, '%Y-%m-%d') 
else:
	today = datetime.date.today()
	yesterday = today - timedelta(days=1)
	start_dt = yesterday.strftime('%Y-%m-%d')
    #end_dt = today.strftime('%Y-%m-%d')
	end_dt = yesterday.strftime('%Y-%m-%d')
	startAt = datetime.datetime.strptime(start_dt, '%Y-%m-%d') 
	endAt = datetime.datetime.strptime(end_dt, '%Y-%m-%d') 

dateIter = startAt
#startDtString = startAt.strftime('%Y%m%d')
#endDtString = endAt.strftime('%Y%m%d')	

print 'Startdate is'
print startAt
print 'EndDate is'
print endAt

#------------------AdvertiserId Loop-----------------------------
url = config.get('doubleclickSearch', 'advertiser_end_point')
headers = {'Content-type': 'application/json', 'Authorization': 'Bearer '+access_tkn}
body = {}
body['reportType'] = "advertiser"
body['columns'] = [ { "columnName": "advertiserId" } ]
body['statisticsCurrency'] = config.get('doubleclickSearch', 'statisticsCurrency')
body_json = json.dumps(body)
advertisersRes = requests.post(url, data=body_json, headers=headers)
#print advertisersRes.json()
advertisers = act = [ item['advertiserId'] for item in advertisersRes.json()['rows']]
print advertisers
#-----------------------------------------------

#########################################################################

while(dateIter <= endAt):
	print dateIter
	adl_folder_date_fmt=dateIter.strftime('%Y%m%d')
	dateIterString = dateIter.strftime('%Y-%m-%d')
	
	url = config.get('doubleclickSearch', 'oAuth_end_point')
	body = {}
	body['Content-Type'] = 'application/x-www-form-urlencoded'
	body['client_id'] = config.get('doubleclickSearch', 'client_id')
	body['client_secret'] = config.get('doubleclickSearch', 'client_secret')
	body['scope'] = 'https://www.googleapis.com/auth/doubleclicksearch'
	body['refresh_token'] = config.get('doubleclickSearch', 'refresh_token')
	body['grant_type'] = 'refresh_token'
	
	oauthresp = requests.post(url, data=body)
	print oauthresp
	print oauthresp.json()
	access_tkn = oauthresp.json()['access_token']
	print access_tkn	
	
	''' Adding Cleanup Step to remove Old feed files from HDInsight Cluster SrcFiles_tmp '''
	print 'Removing previous files from HD temp location'
	cmd = 'rm '+config.get('doubleclickSearch', 'temp_folder_for_hist_files')+'*visit*'
	print cmd
	output = commands.getstatusoutput(cmd)
	status = output[0]
	print status
	print output
	''' Code should execute even if cleanup fails'''
	
	for advertiser in advertisers:
		# in python 2+
		# print line
		# in python 3 print is a builtin function, so
		print 'Processing for advertiserId'
		print advertiser
		url = config.get('doubleclickSearch', 'report_end_point')
		headers = {'Content-type': 'application/json', 'Authorization': 'Bearer '+access_tkn}
		body = {}
		body['reportScope'] = {"agencyId": "20700000001133485", "advertiserId": advertiser}
		body['reportType'] = config.get('DSvisit', 'reportType')
		body['columns'] = ast.literal_eval(config.get('DSvisit', 'columns'))
		body['timeRange'] = {"startDate" : dateIterString, "endDate" : dateIterString}
		body['statisticsCurrency'] = config.get('doubleclickSearch', 'statisticsCurrency')
		body['downloadFormat'] = config.get('doubleclickSearch', 'downloadFormat')
		body['maxRowsPerFile'] = config.get('doubleclickSearch', 'maxRowsPerFile')
		body['verifySingleTimeZone'] = config.get('doubleclickSearch', 'verifySingleTimeZone')
		body['includeRemovedEntities'] = config.get('doubleclickSearch', 'includeRemovedEntities')
		
		body_json = json.dumps(body)		
		print body_json
		
		reportRes = requests.post(url, data=body_json, headers=headers)
		print reportRes.json()
		print reportRes.json()['id']
	
		# Sleep before polling for status
		time.sleep(50)
		
		url = config.get('doubleclickSearch', 'report_end_point')+'/'+reportRes.json()['id']
		headers = {'Content-type': 'application/json', 'Authorization': 'Bearer '+access_tkn}
		statusRes = requests.get(url, headers=headers)
		print statusRes.json()
		statusResStat = statusRes.json()['isReportReady']
		
		print 'While loop starts'
		while(statusResStat != True):
			time.sleep(50)
			statusRes = requests.get(url, headers=headers)
			print statusRes.json()
			print statusResStat
			statusResStat = statusRes.json()['isReportReady']
		
		print 'While loop ends'
		fileUrl = statusRes.json()['files'][0]['url']
		print 'FileURL is'
		print fileUrl
		
		def download_file(url, headers, filename):
			local_filename = config.get('doubleclickSearch', 'temp_folder_for_hist_files')+filename
			r = requests.get(url, headers=headers, stream=True)
			with open(local_filename, 'wb') as f:
				for chunk in r.iter_content(chunk_size=1024): 
					if chunk: # filter out keep-alive new chunks
						f.write(chunk)
			return local_filename, filename
		
		url = fileUrl
		headers = {'Content-type': 'application/json', 'Authorization': 'Bearer '+access_tkn}
		
		visit_extract_name,  visit_extract_file= download_file(url, headers, advertiser+'_visit'+adl_folder_date_fmt+'.csv')
		print visit_extract_name
		print visit_extract_file
	
	#########################################################################
	
	#ADL Copy starts here
	print 'ADL folder creation starts'
	hdfsCmd = 'hadoop fs -mkdir '+config.get('doubleclickSearch', 'adl_end_point_his')+'visit/'+adl_folder_date_fmt
	print hdfsCmd 
	output = commands.getstatusoutput(hdfsCmd)
	status = output[0]
	print status
	print output
	
	print 'ADL data copy starts '
	# hdfsCmd = 'hadoop fs -put '+visit_extract_name+' '+config.get('doubleclickSearch', 'adl_end_point_his')+'visit/'+adl_folder_date_fmt+"/"+visit_extract_file
	hdfsCmd = 'hadoop fs -put ' +config.get('doubleclickSearch', 'temp_folder_for_hist_files')+ '*_visit*.csv ' +config.get('doubleclickSearch', 'adl_end_point_his')+'visit/'+adl_folder_date_fmt+"/"
	
	print hdfsCmd
	output = commands.getstatusoutput(hdfsCmd)
	status = output[0]
	print status
	print output
	
	#------------
	#Archive ADL data copy starts here
	print 'Archive ADL folder creation starts'
	hdfsCmd = 'hadoop fs -mkdir '+config.get('doubleclickSearch', 'archive_adl_end_point_his')+'visit/'+adl_folder_date_fmt
	print hdfsCmd
	output = commands.getstatusoutput(hdfsCmd)
	status = output[0]
	print status
	print output

	print 'Archive ADL data copy starts'
	hdfsCmd = 'hadoop fs -cp '+config.get('doubleclickSearch', 'adl_end_point_his')+'visit/'+adl_folder_date_fmt+"/*"+' '+config.get('doubleclickSearch', 'archive_adl_end_point_his')+'visit/'+adl_folder_date_fmt+"/"
	print hdfsCmd
	output = commands.getstatusoutput(hdfsCmd)
	status = output[0]
	print status
	print output
	# Updating table partition
	print 'Removing previous partitions from raw table'
	cmd = "hive -e \"alter table cdp_raw_data.rw_ds_visit drop partition (file_dt > '0');\""
	print cmd
	commands.getstatusoutput(cmd)
	
	print 'Adding new partition to raw table'
	cmd = "hive -e \"alter table cdp_raw_data.rw_ds_visit add partition (file_dt="+adl_folder_date_fmt+") location 'adl://cdpuat.azuredatalakestore.net/cdp_raw_store/doubleclick_search/visit/"+adl_folder_date_fmt+"';\"";
	print cmd
	commands.getstatusoutput(cmd)
	
	#------------
	
	time.sleep(6)
	dateIter = dateIter + timedelta(days=1)	
print 'End of DS visit data pull script'