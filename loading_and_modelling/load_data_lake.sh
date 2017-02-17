#!/bin/bash
# 
# prerequisite
# 
# all files have been downloaded, unzipped, and relevant csv files are local

# remove headers and rename files for ease of reference
mkdir data_to_load
tail -n +2 "Hospital General Information.csv" > hospitals.csv
tail -n +2 "Timely and Effective Care - Hospital.csv" > effective_care.csv
tail -n +2 "Readmissions and Deaths - Hospital.csv" > readmissions.csv
tail -n +2 "Measure Dates.csv" > measures.csv
tail -n +2 "hvbp_hcahps_11_10_2016.csv" > survey_responses.csv




