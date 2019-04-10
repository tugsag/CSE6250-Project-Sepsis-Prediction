from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Feature Extraction for Negative Cases").getOrCreate()

# Identify septic patients
diag = spark.read.csv("../data/DIAGNOSES_ICD.csv", header=True, inferSchema=True)
sep_check = udf(lambda item: 1 if item in ["99591", "99592", "78552"] else 0, IntegerType())
sepsis_diag = diag.select("SUBJECT_ID", "HADM_ID", sep_check(col('ICD9_CODE')).alias('SEPSIS'))
sepsis_diag_final = sepsis_diag.groupBy("SUBJECT_ID","HADM_ID").max("SEPSIS").withColumnRenamed("max(SEPSIS)", "SEPSIS")
# 58796 admissions; 5325 septic

icu = spark.read.csv("../data/ICUSTAYS.csv", header=True, inferSchema=True)
patient = spark.read.csv("../data/PATIENTS.csv", header=True, inferSchema=True).select("SUBJECT_ID", "GENDER", "DOB")
icu = icu.filter(icu.DBSOURCE == "metavision").filter(icu.LOS >= 6/24).select("SUBJECT_ID","HADM_ID","ICUSTAY_ID","INTIME","OUTTIME")
icu = icu.join(patient, ["SUBJECT_ID"])
icu_age = icu.withColumn('AGE', datediff(icu.INTIME, icu.DOB)/365)
icu_label = icu_age.join(sepsis_diag_final, ["SUBJECT_ID", "HADM_ID"])
icu_0 = icu_label.filter("SEPSIS = 0").drop("DOB")

chart = spark.read.csv("../data/CHARTEVENTS.csv", header=True, inferSchema=True).select("ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM")

# negative cases
feat_list = [220050, 220045, 223761, 220210, 220277, 227013, 220051]
negative_chart = icu_0.join(chart, ["ICUSTAY_ID"])
feat_chart_neg = negative_chart.filter(negative_chart.ITEMID.isin(feat_list))
feat_chart_neg_time = feat_chart_neg.select("ICUSTAY_ID", "CHARTTIME").groupBy("ICUSTAY_ID").agg(Fmin("CHARTTIME")).withColumnRenamed("min(CHARTTIME)", "FEATURE1_TIME")
feat_chart_neg_time = feat_chart_neg_time.withColumn("FEATURE2_TIME", feat_chart_neg_time.FEATURE1_TIME + expr('INTERVAL 1 HOURS'))
feat_chart_neg = feat_chart_neg.join(feat_chart_neg_time, ["ICUSTAY_ID"]).rdd
feat_chart_neg.persist()
feat1neg = feat_chart_neg.filter(lambda x: (x.FEATURE1_TIME.year, x.FEATURE1_TIME.month, x.FEATURE1_TIME.day, x.FEATURE1_TIME.hour) == (x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
feat2neg = feat_chart_neg.filter(lambda x: (x.FEATURE2_TIME.year, x.FEATURE2_TIME.month, x.FEATURE2_TIME.day, x.FEATURE2_TIME.hour) == (x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
print(feat1neg.count())
print(feat2neg.count())

spark.createDataFrame(feat1neg).coalesce(1).write.option("header", "true").csv("../data/NEG_FEAT1.csv")
spark.createDataFrame(feat2neg).coalesce(1).write.option("header", "true").csv("../data/NEG_FEAT2.csv")

spark.stop()
