#
# best_hospitals.py
# Investigate which hospitals are the "best"
#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#from pyspark.sql.functions import *
import pyspark.sql.functions as F
sc = SparkContext("local", "Exercise1")
sqlContext = SQLContext(sc)

# read the dataframe in from the parguet file
dfHospitals =  sqlContext.read.parquet("/user/w205/hospital_compare/hospitalParquet")
dfMeasures = sqlContext.read.parquet("/user/w205/hospital_compare/measuresParquet")
dfProcedures = sqlContext.read.parquet("/user/w205/hospital_compare/proceduresParquet")

# columns we want that are ranges ((x - min) / (max - min))
measuresRanges = ["EDV", "ED_1b", "ED_2b", "OP_18b", "OP_20", "OP_21", "OP_5"]
dfRanges = dfProcedures.where(F.col("measureID").isin(measuresRanges))

mins = [dfRanges.where(F.col("measureID").like(m)).agg(F.min("score")).collect()[0][0] for m in measuresRanges]
maxs = [dfRanges.where(F.col("measureID").like(m)).agg(F.max("score")).collect()[0][0] for m in measuresRanges]
ranges = [maxs[i] - mins[i] for i in range(0,len(maxs))]

# compute range percents
rangeUDF = F.udf(lambda score: (score - mins[0]) / ranges[0], DecimalType(10,3))
dfQuality = dfRanges.withColumn("score", F.when(dfRanges.measureID.like(measuresRanges[0]), rangeUDF(dfRanges.score))).where(F.col("score").isNotNull())

for i in range(1,len(mins)):
    rangeUDF = F.udf(lambda score: (score - mins[i]) / ranges[i], DecimalType(10,3))
    dfQuality = dfQuality.unionAll(dfRanges.withColumn("score", F.when(dfRanges.measureID.like(measuresRanges[i]), rangeUDF(dfRanges.score))).where(F.col("score").isNotNull()))

# computer reverse range percents
measuresReverseRanges = ["VTE_6"]
dfReverseRanges = dfProcedures.where(F.col("measureID").isin(measuresReverseRanges))

mins = [dfReverseRanges.where(F.col("measureID").like(m)).agg(F.min("score")).collect()[0][0] for m in measuresReverseRanges]
maxs = [dfReverseRanges.where(F.col("measureID").like(m)).agg(F.max("score")).collect()[0][0] for m in measuresReverseRanges ]
ranges = [maxs[i] - mins[i] for i in range(0,len(maxs))]

# compute range percents
reverseRangeUDF = F.udf(lambda score: (maxs[0] - score) / ranges[0], DecimalType(10,3))
dfQuality = dfQuality.unionAll(dfReverseRanges.withColumn(
  "score", F.when(dfReverseRanges.measureID.like(measuresReverseRanges[0]), 
           reverseRangeUDF(dfReverseRanges.score))).where(F.col("score").isNotNull()))

# columns we want that are already percentages
measuresRates = ["OP_23", "OP_29", "OP_30", "OP_4", "VTE_5", "STK_4"]
dfQuality = dfQuality.unionAll(dfProcedures.where(F.col("measureID").isin(measuresRates)))

measuresQuality = measuresRates + measuresReverseRanges + measuresRanges

# now the penalties
measuresRead = ["READM_30_HF"]
dfRead = dfProcedures.where(F.col("measureID").isin(measuresRead))

# measures for mortality
measuresMort = ["MORT_30_AMI", "MORT_30_CABG", "MORT_30_COPD", "MORT_30_HF", "MORT_30_PN", "MORT_30_STK"]
dfMort = dfProcedures.where(F.col("measureID").isin(measuresMort))

dfPenalty = dfMort.unionAll(dfRead)

# build final dataframes
dfPenalty = dfPenalty.groupby("providerID").agg(F.avg("score").alias("score")).where(F.col("score").isNotNull())
dfQuality = dfQuality.select(F.col("providerID").alias("ID"), "score").groupby("ID").agg(F.avg("score").alias("quality_score")).where(F.col("quality_score").isNotNull())

dfFinal = dfPenalty.join(dfQuality, dfQuality.ID == dfPenalty.providerID)

div = len(measuresQuality)
dfShow = dfFinal.withColumn("FinalScore", dfFinal.quality_score - dfFinal.score / div).\
  select("providerID", F.col("FinalScore").cast(DecimalType(4,2)).alias("QualityScore"))

statsFinal = dfShow.describe()
stddev = statsFinal.select("summary", F.col("QualityScore").cast(DecimalType(6,3)).alias("QualityScore")).collect()[2][1]
dfShow.join(dfHospitals, dfHospitals.id == dfShow.providerID).select("providerID", "name", "state", "QualityScore").sort("QualityScore", ascending = False).show(10, False)
print("With Standard Deviation of Quality Score: " + str(stddev))

