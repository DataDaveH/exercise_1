#
# hospitals.py
# Extract useful data from hospitals table and store in a parquet file
#
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext

sc = SparkContext("local", "Exercise1")
hiveCX = HiveContext(sc)

# bring the table into a data frame
dfHospital = hiveCX.table("hospitals")

# select out the relevent info into a new dataframe
dfHospitalNew = dfHospital.withColumn(
  "hospital_overall_rating", regexp_replace("hospital_overall_rating", "Not.*", "0")).select(
  col("provider_id").alias("id"),
  col("hospital_name").alias("name"),
  col("state").alias("state"),
  col("hospital_overall_rating").cast(DecimalType(1,0)).alias("rating"))

# write the dataframe out as a parquet file
dfHospitalNew.write.parquet("/user/w205/hospital_compare/hospitalParquet")




