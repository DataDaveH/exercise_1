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

DROP TABLE survey_responses;
CREATE EXTERNAL TABLE survey_responses (
Provider_Number INT,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Communication_with_Nurses_Floor STRING,
Communication_with_Nurses_Achievement_Threshold STRING,
Communication_with_Nurses_Benchmark STRING,
Communication_with_Nurses_Baseline_Rate STRING,
Communication_with_Nurses_Performance_Rate STRING,
Communication_with_Nurses_Achievement_Points STRING,
Communication_with_Nurses_Improvement_Points STRING,
Communication_with_Nurses_Dimension_Score STRING,
Communication_with_Doctors_Floor STRING,
Communication_with_Doctors_Achievement_Threshold STRING,
Communication_with_Doctors_Benchmark STRING,
Communication_with_Doctors_Baseline_Rate STRING,
Communication_with_Doctors_Performance_Rate STRING,
Communication_with_Doctors_Achievement_Points STRING,
Communication_with_Doctors_Improvement_Points STRING,
Communication_with_Doctors_Dimension_Score STRING,
Responsiveness_of_Hospital_Staff_Floor STRING,
Responsiveness_of_Hospital_Staff_Achievement_Threshold STRING,
Responsiveness_of_Hospital_Staff_Benchmark STRING,
Responsiveness_of_Hospital_Staff_Baseline_Rate STRING,
Responsiveness_of_Hospital_Staff_Performance_Rate STRING,
Responsiveness_of_Hospital_Staff_Achievement_Points STRING,
Responsiveness_of_Hospital_Staff_Improvement_Points STRING,
Responsiveness_of_Hospital_Staff_Dimension_Score STRING,
Pain_Management_Floor STRING,
Pain_Management_Achievement_Threshold STRING,
Pain_Management_Benchmark STRING,
Pain_Management_Baseline_Rate STRING,
Pain_Management_Performance_Rate STRING,
Pain_Management_Achievement_Points STRING,
Pain_Management_Improvement_Points STRING,
Pain_Management_Dimension_Score STRING,
Communication_about_Medicines_Floor STRING,
Communication_about_Medicines_Achievement_Threshold STRING,
Communication_about_Medicines_Benchmark STRING,
Communication_about_Medicines_Baseline_Rate STRING,
Communication_about_Medicines_Performance_Rate STRING,
Communication_about_Medicines_Achievement_Points STRING,
Communication_about_Medicines_Improvement_Points STRING,
Communication_about_Medicines_Dimension_Score STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Floor STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Achievement_Threshold STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Benchmark STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Baseline_Rate STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Performance_Rate STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Achievement_Points STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Improvement_Points STRING,
Cleanliness_and_Quietness_of_Hospital_Environment_Dimension_Score STRING,
Discharge_Information_Floor STRING,
Discharge_Information_Achievement_Threshold STRING,
Discharge_Information_Benchmark STRING,
Discharge_Information_Baseline_Rate STRING,
Discharge_Information_Performance_Rate STRING,
Discharge_Information_Achievement_Points STRING,
Discharge_Information_Improvement_Points STRING,
Discharge_Information_Dimension_Score STRING,
Overall_Rating_of_Hospital_Floor STRING,
Overall_Rating_of_Hospital_Achievement_Threshold STRING,
Overall_Rating_of_Hospital_Benchmark STRING,
Overall_Rating_of_Hospital_Baseline_Rate STRING,
Overall_Rating_of_Hospital_Performance_Rate STRING,
Overall_Rating_of_Hospital_Achievement_Points STRING,
Overall_Rating_of_Hospital_Improvement_Points STRING,
Overall_Rating_of_Hospital_Dimension_Score STRING,
HCAHPS_Base_Score STRING,
HCAHPS_Consistency_Score STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/survey_responses';

DROP TABLE effective_care;
CREATE EXTERNAL TABLE effective_care (
Provider_ID STRING,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Condition STRING,
Measure_ID STRING,
Measure_Name STRING,
Score INT,
Sample STRING,
Footnote STRING,
Measure_Start_Date DATE,
Measure_End_Date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';

DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals (
Provider_ID INT,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Hospital_Type STRING,
Hospital_Ownership STRING,
Emergency_Services STRING,
Meets_criteria_for_meaningful_use_of_EHRs STRING,
Hospital_overall_rating INT,
Hospital_overall_rating_footnote STRING,
Mortality_national_comparison STRING,
Mortality_national_comparison_footnote STRING,
Safety_of_care_national_comparison STRING,
Safety_of_care_national_comparison_footnote STRING,
Readmission_national_comparison STRING,
Readmission_national_comparison_footnote STRING,
Patient_experience_national_comparison STRING,
Patient_experience_national_comparison_footnote STRING,
Effectiveness_of_care_national_comparison STRING,
Effectiveness_of_care_national_comparison_footnote STRING,
Timeliness_of_care_national_comparison STRING,
Timeliness_of_care_national_comparison_footnote STRING,
Efficient_use_of_medical_imaging_national_comparison STRING,
Efficient_use_of_medical_imaging_national_comparison_footnote STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';

DROP TABLE readmissions;
CREATE EXTERNAL TABLE readmissions (
Provider_ID INT,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Measure_Name STRING,
Measure_ID INT,
Compared_to_National STRING,
Denominator INT,
Score FLOAT,
Lower_Estimate STRING,
Higher_Estimate STRING,
Footnote STRING,
Measure_Start_Date DATE,
Measure_End_Date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions';

