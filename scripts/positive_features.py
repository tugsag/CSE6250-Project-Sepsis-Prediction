from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Feature Extraction").getOrCreate()
timeFmt = "yyyy-MM-dd HH:mm:ss"

positive_df = spark.read.csv("../data/POSITIVE/*.csv", header=True, inferSchema=True)
chart = spark.read.csv("../data/CHARTEVENTS.csv", header=True, inferSchema=True).select("ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM")

positive_chart = positive_df.join(chart, ["ICUSTAY_ID"])
feat_list = [223762, 224689, 220235, 220546, 220050, 220045, 223761, 220210, 220277, 227013, 220051, 228640, 224192, 224359, 226329, 225309]
feat_chart = positive_chart.filter(positive_chart.ITEMID.isin(feat_list)).rdd
#feat_chart.persist()
feat1 = feat_chart.filter(lambda x: x.FEATURE1_TIME == datetime(x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
feat2 = feat_chart.filter(lambda x: x.FEATURE2_TIME == datetime(x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
#print(feat1.count())
#print(feat2.count())

spark.createDataFrame(feat1).write.option("header", "true").csv("../data/POS_FEAT1")
spark.createDataFrame(feat2).write.option("header", "true").csv("../data/POS_FEAT2")

spark.stop()
