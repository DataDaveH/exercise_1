#
# hospitals_and_patients.py
# Investigate the correlation between hospitals' scores and ratings
#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from math import sqrt

sc = SparkContext("local", "Exercise1")
sqlContext = SQLContext(sc)

# read the dataframes in from the parguet file
dfHospitals =  sqlContext.read.parquet("/user/w205/hospital_compare/hospitalParquet")
dfSurveyResp =  sqlContext.read.parquet("/user/w205/hospital_compare/surveyRespParquet")
dfHospitalQual =  sqlContext.read.parquet("/user/w205/hospital_compare/hospitalQualParquet")

dfFinal = dfSurveyResp.withColumn("score", dfSurveyResp.baseScore + dfSurveyResp.consistencyScore)\
            .select("providerID", "score").groupby("providerID").avg("score")\
            .sort("avg(score)", ascending = False)\
            .join(dfHospitals, F.col("id") == F.col("providerID"))

dfShow = dfFinal.select( F.col("providerID"), F.col("name"), F.col("rating"), F.col("state"),\
                F.col("avg(score)").alias("score"))

corrQuality = dfShow.join( dfHospitalQual, F.col("providerID") == F.col("ProviderID"))\
                    .stat.corr("FinalScore", "score")

corrRating = dfShow.stat.corr("rating", "score")

dfShow.show(10, False)

print("Rating/SurveyScore correlation: " + str(round(corrRating, 3)))
print("QualityScore/SurveyScore correlation: " + str(round(corrQuality, 3)))
