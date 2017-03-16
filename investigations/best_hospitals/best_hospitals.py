#
# best_hospitals.py
# Investigate which hospitals are the "best"
#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from math import sqrt
sc = SparkContext("local", "Exercise1")
sqlContext = SQLContext(sc)

# read the dataframe in from the parguet file
dfHospitals =  sqlContext.read.parquet("/user/w205/hospital_compare/hospitalParquet")
dfMeasures = sqlContext.read.parquet("/user/w205/hospital_compare/measuresParquet")
dfProcedures = sqlContext.read.parquet("/user/w205/hospital_compare/proceduresParquet")

# columns we want that are ranges ((x - min) / (max - min))
measuresRanges = ["EDV"]
dfRanges = dfProcedures.where(F.col("measureID").isin(measuresRanges))

mins = [dfRanges.where(F.col("measureID").like(m)).agg(F.min("score")).collect()[0][0] for m in measuresRanges]
maxs = [dfRanges.where(F.col("measureID").like(m)).agg(F.max("score")).collect()[0][0] for m in measuresRanges]
ranges = [maxs[i] - mins[i] for i in range(0,len(maxs))]

# compute range percents
rangeUDF = F.udf(lambda score: 100 * (score - mins[0]) / ranges[0], DecimalType(10,3))
dfQuality = dfRanges.withColumn("score", F.when(dfRanges.measureID.like(measuresRanges[0]), rangeUDF(dfRanges.score)))\
            .where(F.col("score").isNotNull())

for i in range(1,len(mins)):
    rangeUDF = F.udf(lambda score: 100 * (score - mins[i]) / ranges[i], DecimalType(10,3))
    dfQuality = dfQuality.unionAll( \
        dfRanges.withColumn("score", F.when(dfRanges.measureID.like(measuresRanges[i]), rangeUDF(dfRanges.score)))\
        .where(F.col("score").isNotNull()))

# compute reverse range (a higher number is worse)
measuresReverseRanges = ["VTE_6", "ED_1b", "ED_2b", "OP_18b", "OP_20", "OP_21", "OP_5"]
dfReverseRanges = dfProcedures.where(F.col("measureID").isin(measuresReverseRanges))

mins = [dfReverseRanges.where(F.col("measureID").like(m)).agg(F.min("score")).collect()[0][0] for m in measuresReverseRanges]
maxs = [dfReverseRanges.where(F.col("measureID").like(m)).agg(F.max("score")).collect()[0][0] for m in measuresReverseRanges ]
ranges = [maxs[i] - mins[i] for i in range(0,len(maxs))]

# compute reverse range percents ((max - x) / (max - min))
reverseRangeUDF = F.udf(lambda score: 100 * (maxs[0] - score) / ranges[0], DecimalType(10,3))
dfQuality = dfQuality.unionAll(dfReverseRanges.withColumn(
  "score", F.when(dfReverseRanges.measureID.like(measuresReverseRanges[0]), 
           reverseRangeUDF(dfReverseRanges.score))).where(F.col("score").isNotNull()))

for i in range(1,len(mins)):
    reverseRangeUDF = F.udf(lambda score: 100 * (maxs[i] - score) / ranges[i], DecimalType(10,3))
    dfQuality = dfQuality.unionAll( dfReverseRanges.withColumn(
         "score", F.when( dfReverseRanges.measureID.like(measuresReverseRanges[i]), reverseRangeUDF(dfReverseRanges.score)))\
        .where(F.col("score").isNotNull()))

# columns we want that are already percentages
measuresRates = ["OP_23", "OP_29", "OP_30", "OP_4", "VTE_5", "STK_4"]
dfQuality = dfQuality.unionAll(dfProcedures.where(F.col("measureID").isin(measuresRates)))

measuresQuality = measuresRates + measuresReverseRanges + measuresRanges
numMeasures = len(measuresQuality)

# now the penalties
# readmission measure
measuresRead = ["READM_30_HF"]
dfRead = dfProcedures.where(F.col("measureID").isin(measuresRead))

# measures for mortality
measuresMort = ["MORT_30_AMI", "MORT_30_CABG", "MORT_30_COPD", "MORT_30_HF", "MORT_30_PN", "MORT_30_STK"]
dfMort = dfProcedures.where(F.col("measureID").isin(measuresMort))

dfPenalty = dfMort.unionAll(dfRead)

# use quality and penalty scores to compute variance
rddQuality = dfQuality.rdd
rddPenalty = dfPenalty.rdd

# to build the score variance per provider, aggregate (sum of score^2, sum of score, count)
rddVar = rddQuality.map( lambda x: (x[0], x[2]))\
    .aggregateByKey((0.0,0.0,0.0),\
    (lambda x, newVal: ((x[0] + (float(newVal) ** 2)), (x[1] + float(newVal)), (x[2] + 1))),\
    (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1], rdd1[2] + rdd2[2])))

# then map by values to compute the variance = (sum(score^2) / count) - (sum(score) / count)^2
# which is the average sum of squares minus the mean squared
rddStdDev = rddVar.mapValues( lambda x: round( sqrt((x[0] / x[2]) - ((x[1] / x[2]) ** 2)), 5))
           
# compute average quality and penalty scores
# aggregate by adding values and increment count each time
rddAvgQ = rddQuality.map( lambda x: (x[0], x[2]))\
    .aggregateByKey((0.0,0.0),\
    (lambda x, newVal: ((x[0] + float(newVal)), (x[1] + 1))),\
    (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1])))

rddAvgQ = rddAvgQ.mapValues( lambda x: round((x[0] / (numMeasures)), 5))

# aggregate by adding values and increment count each time
rddAvgP = rddPenalty.map( lambda x: (x[0], x[2]))\
    .aggregateByKey((0.0,0.0),\
    (lambda x, newVal: ((x[0] + float(newVal)), (x[1] + 1))),\
    (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1])))

# we are penalizing a small amount based on the number of quality measures
rddAvgP = rddAvgP.mapValues( lambda x: round((x[0] / (x[1])), 5))

# break the columns apart after the joins
rddFinal = rddAvgQ.join( rddAvgP).join( rddStdDev).map( lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1]))

# build final dataframes
dfFinal = rddFinal.toDF( ["ProviderID", "QualityScore", "Penalty", "StandardDeviation"])\
          .withColumn("FinalScore", F.round(F.col("QualityScore") - F.col("Penalty"), 5))

dfShow = dfFinal.join( dfHospitals, dfHospitals.id == dfFinal.ProviderID)\
    .select("ProviderID", "name", "state", "rating", "QualityScore", "Penalty", "StandardDeviation", "FinalScore")\
    .sort("FinalScore", ascending = False)

# save this score table for question 4
dfShow.write.parquet("/user/w205/hospital_compare/hospitalQualParquet")

dfShowRank = dfShow.rdd.zipWithIndex().map(lambda x: (x[1] + 1, x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[0][5], x[0][6], x[0][7]))\
    .toDF().select(F.col("_1").alias("Rank"), F.col("_2").alias("ProviderID"), F.col("_3").alias("name"), F.col("_4").alias("state"),\
                       F.col("_5").cast("int").alias("rating"), F.col("_6").alias("QualityScore"), F.col("_7").alias("Penalty"),\
                       F.col("_8").alias("StandardDeviation"), F.col("_9").alias("FinalScore"))\
    .show(10, False)
