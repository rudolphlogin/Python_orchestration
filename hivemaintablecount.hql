

INSERT OVERWRITE LOCAL DIRECTORY '/home/cdpappdev1/codebase/dataload/source/hivecount' SELECT COUNT(1) FROM ${hiveconf:load_maintablename} WHERE filedate ='${hiveconf:load_date}';



