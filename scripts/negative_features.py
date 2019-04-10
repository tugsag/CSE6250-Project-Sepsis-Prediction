from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Feature Extraction for Negative Cases").getOrCreate()

chart = spark.read.csv("../data/CHARTEVENTS.csv", header=True, inferSchema=True).select("ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM")
icu_0 = spark.read.csv("../data/ICU_0/*.csv", header=True, inferSchema=True)

# negative cases
feat_list = [223762, 224689, 220235, 220546, 220050, 220045, 223761, 220210, 220277, 227013, 220051, 228640, 224192, 224359, 226329, 225309]
negative_chart = icu_0.join(chart, ["ICUSTAY_ID"])
feat_chart_neg = negative_chart.filter(negative_chart.ITEMID.isin(feat_list))
feat_chart_neg_time = feat_chart_neg.select("ICUSTAY_ID", "CHARTTIME").groupBy("ICUSTAY_ID").agg(Fmin("CHARTTIME")).withColumnRenamed("min(CHARTTIME)", "FEATURE1_TIME")
feat_chart_neg_time = feat_chart_neg_time.withColumn("FEATURE2_TIME", feat_chart_neg_time.FEATURE1_TIME + expr('INTERVAL 1 HOURS'))
feat_chart_neg = feat_chart_neg.join(feat_chart_neg_time, ["ICUSTAY_ID"]).rdd
#feat_chart_neg.persist()
feat1neg = feat_chart_neg.filter(lambda x: (x.FEATURE1_TIME.year, x.FEATURE1_TIME.month, x.FEATURE1_TIME.day, x.FEATURE1_TIME.hour) == (x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
feat2neg = feat_chart_neg.filter(lambda x: (x.FEATURE2_TIME.year, x.FEATURE2_TIME.month, x.FEATURE2_TIME.day, x.FEATURE2_TIME.hour) == (x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
#print(feat1neg.count())
#print(feat2neg.count())

spark.createDataFrame(feat1neg).write.option("header", "true").csv("../data/NEG_FEAT1")
spark.createDataFrame(feat2neg).write.option("header", "true").csv("../data/NEG_FEAT2")

spark.stop()
