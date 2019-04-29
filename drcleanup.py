import csv
import sys
import commands

if (len(sys.argv) == 2):
	filesPath = sys.argv[1]
	cmd = 'rm -r '+filesPath+'cleansed/'
        output = commands.getstatusoutput(cmd)
        status = output[0]
        print status
        print output
	cmd = 'mkdir '+filesPath+'cleansed'
        output = commands.getstatusoutput(cmd)
        status = output[0]
        print status
        print output
	cmd = 'ls -l '+filesPath+' | grep \.csv | rev | cut -d" " -f1 | rev'
        output = commands.getstatusoutput(cmd)
        status = output[0]
        print status
        print output
	for file in output[1].splitlines():
		fileName = file.split('.')[:-1]
		fileNameDt = (''.join(fileName)).split('_')[-1]
		with open(filesPath+'/'+file, "r") as file_in:
			with open(filesPath+'cleansed/'+file, "w") as file_out:
				writer = csv.writer(file_out)
				for row in csv.reader(file_in):
					writer.writerow(row)
