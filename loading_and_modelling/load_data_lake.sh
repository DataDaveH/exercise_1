#!/bin/bash
# 
# prerequisite
# 
# all files have been downloaded, unzipped, and relevant csv files are local
# 
# remove headers and rename files for ease of reference
mkdir data_to_load
tail -n +2 "Hospital General Information.csv" > ./data_to_load/hospitals.csv
tail -n +2 "Timely and Effective Care - Hospital.csv" > ./data_to_load/effective_care.csv
tail -n +2 "Readmissions and Deaths - Hospital.csv" > ./data_to_load/readmissions.csv
tail -n +2 "Measure Dates.csv" > ./data_to_load/measures.csv
tail -n +2 "hvbp_hcahps_11_10_2016.csv" > ./data_to_load/survey_responses.csv

# now move all the files to HDFS
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put ./data_to_load/"hospitals.csv" /user/w205/hospital_compare
hdfs dfs -put ./data_to_load/"effective_care.csv" /user/w205/hospital_compare
hdfs dfs -put ./data_to_load/"readmissions.csv" /user/w205/hospital_compare
hdfs dfs -put ./data_to_load/"measures.csv" /user/w205/hospital_compare
hdfs dfs -put ./data_to_load/"survey_responses.csv" /user/w205/hospital_compare
