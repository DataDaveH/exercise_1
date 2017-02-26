# 
# procedures.py
# Extract useful data from effective_care and readmissions tables and store in a parquet file
# 
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext

sc = SparkContext("local", "Exercise1")
hiveCX = HiveContext(sc)

# bring the table into a data frame
dfEffCare = hiveCX.table("effective_care")
dfReadmissions = hiveCX.table("readmissions")

# select out the relevent info into a new dataframe
dfEffCareNew = dfEffCare.withColumn( 
  "score", regexp_replace("score", "Low.*", "1")).withColumn(
  "score", regexp_replace("score", "Medium.*", "2")).withColumn(
  "score", regexp_replace("score", "High.*", "3")).withColumn(
  "score", regexp_replace("score", "Very High.*", "4")).select(
  col("provider_id").alias("providerID"),
  col("measure_id").alias("measureID"),
  col("score").cast(DecimalType()).alias("score")).where(
  col("score").isNotNull())

dfReadmissionsNew = dfReadmissions.select(
  col("provider_id").alias("providerID"),
  col("measure_id").alias("measureID"),
  col("score").cast(DecimalType()).alias("score")).where(
  col("score").isNotNull())

dfProcedures = dfEffCareNew.unionAll(dfReadmissionsNew)


# write the dataframe out as a parquet file
dfProcedures.write.parquet("/user/w205/hospital_compare/proceduresParquet")
