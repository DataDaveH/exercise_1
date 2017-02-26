#
# measures.py
# Extract measures name-ID mappings and store in a parquet file
#
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext

sc = SparkContext("local", "Exercise1")
hiveCX = HiveContext(sc)

# bring the table into a data frame
dfMeasure = hiveCX.table("measures")

# select out the relevent info into a new dataframe
dfMeasuresNew = dfMeasure.select(
  col("measure_id").alias("id"),
  col("measure_name").alias("name"))

# write the dataframe out as a parquet file
dfMeasuresNew.write.parquet("/user/w205/hospital_compare/measuresParquet")
