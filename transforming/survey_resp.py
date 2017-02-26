#
# survey_resp.py
# Extract useful data from survey_responses table and store in a parquet file
#
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext

sc = SparkContext("local", "Exercise1")
hiveCX = HiveContext(sc)

# bring the table into a data frame
dfSurveyResp = hiveCX.table("survey_responses")

# select out the relevent info into a new dataframe
dfSurveyRespNew = dfSurveyResp.select(
  col("provider_number").alias("providerID"),
  col("hcahps_base_score").cast(DecimalType()).alias("baseScore"),
  col("hcahps_consistency_score").cast(DecimalType()).alias("consistencyScore")).where( 
  col("baseScore").isNotNull() & col("consistencyScore").isNotNull())

# write the dataframe out as a parquet file
dfSurveyRespNew.write.parquet("/user/w205/hospital_compare/surveyRespParquet")

