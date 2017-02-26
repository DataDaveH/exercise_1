#!/bin/bash
#

# Clean up the directories and parquet tables created by the python scripts
# for resetting between tests

# hospital
hdfs dfs -rm /user/w205/hospital_compare/hospitalParquet/*
hdfs dfs -rmdir /user/w205/hospital_compare/hospitalParquet

# measures
hdfs dfs -rm /user/w205/hospital_compare/measuresParquet/*
hdfs dfs -rmdir /user/w205/hospital_compare/measuresParquet

# procedures
hdfs dfs -rm /user/w205/hospital_compare/proceduresParquet/*
hdfs dfs -rmdir /user/w205/hospital_compare/proceduresParquet

# surveyResults
hdfs dfs -rm /user/w205/hospital_compare/surveyRespParquet/*
hdfs dfs -rmdir /user/w205/hospital_compare/surveyRespParquet