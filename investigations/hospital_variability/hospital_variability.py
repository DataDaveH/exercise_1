#
# hospital_variability.py
# Investigate which procedures vary the most between hospitals
#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local", "Exercise1")
sqlContext = SQLContext(sc)

# read the dataframes in from the parguet file
dfHospitals =  sqlContext.read.parquet("/user/w205/hospital_compare/hospitalParquet")
dfProcedures =  sqlContext.read.parquet("/user/w205/hospital_compare/proceduresParquet")

