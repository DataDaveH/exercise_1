#
# hospital_variability.py
# Investigate which procedures vary the most between hospitals
#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from math import sqrt

sc = SparkContext("local", "Exercise1")
sqlContext = SQLContext(sc)

# read the dataframes in from the parguet file
dfMeasures =  sqlContext.read.parquet("/user/w205/hospital_compare/measuresParquet")
dfProcedures =  sqlContext.read.parquet("/user/w205/hospital_compare/proceduresParquet")

# compute variance by aggregating some score stats on measureID
# 0: providerID, 1: measureID, 2: score
rddVar = dfProcedures.rdd.map( lambda x: (x[1], x[2]))\
    .aggregateByKey((0.0,0.0,0.0,0.0,10000.0),\
    (lambda x, newVal: (x[0] + (float(newVal) ** 2), (x[1] + float(newVal)), (x[2] + 1),\
                        max(x[3], float(newVal)), min(x[4], float(newVal)))),\
    (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1], rdd1[2] + rdd2[2],\
                         max(rdd1[3], rdd2[3]), min(rdd1[4], rdd2[4]))))

# use standard deviation as a fraction of the range for variability
rddProcVar = rddVar.mapValues(\
             lambda x: (int(x[2]), round( sqrt((x[0] / x[2]) - ((x[1] / x[2]) ** 2)) / (x[3] - x[4]), 5)))

dfFinal = rddProcVar.join(dfMeasures.rdd)\
          .map( lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1])).toDF()\
          .select( F.col("_1").alias("MeasureID"),
                   F.col("_2").alias("Count"),
                   F.col("_3").alias("Variability"),
                   F.col("_4").alias("Description"))

dfFinal.sort("Variability", ascending = False).show(10, False)

