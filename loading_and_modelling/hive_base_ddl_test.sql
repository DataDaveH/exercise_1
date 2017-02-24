DROP TABLE measures;
CREATE EXTERNAL TABLE measures (
Measure_Name STRING,
Measure_ID STRING,
Measure_Start_Quarter STRING,
Measure_Start_Date DATE,
Measure_End_Quarter STRING,
Measure_End_Date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/measures';
