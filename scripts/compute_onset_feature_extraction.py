from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Computing Onset and Feature Extraction").getOrCreate()
timeFmt = "yyyy-MM-dd HH:mm:ss"

def time_diff_in_hours(x, y):
    diff = x - y
    days = diff.days
    seconds = diff.seconds
    return days * 24 + seconds / 3600

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
icu_1 = icu_label.filter("SEPSIS = 1").drop("DOB")
icu_0 = icu_label.filter("SEPSIS = 0").drop("DOB")
# count: 23406; sepsis = 3199

# compute infection time
d_items = spark.read.csv("../data/D_ITEMS.csv", header=True, inferSchema=True)

antibiotics = d_items.filter(d_items.DBSOURCE == "metavision").filter(d_items.CATEGORY == "Antibiotics").select(d_items.ITEMID).collect()
antibiotics = [x.ITEMID for x in antibiotics]

cultures = d_items.filter(d_items.DBSOURCE == "metavision").filter(d_items.CATEGORY == "6-Cultures").select(d_items.ITEMID).collect()
cultures = [x.ITEMID for x in cultures]

input_mv = spark.read.csv("../data/INPUTEVENTS_MV.csv", header=True, inferSchema=True)
input_anti = input_mv.filter(input_mv.ITEMID.isin(antibiotics)).select("SUBJECT_ID", "HADM_ID", "ICUSTAY_ID", "STARTTIME").withColumn("ITEM", lit("ANTIBIOTICS"))
input_1 = input_anti.join(icu_1, ["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"])

proc_mv = spark.read.csv("../data/PROCEDUREEVENTS_MV.csv", header=True, inferSchema=True)
proc_cult = proc_mv.filter(proc_mv.ITEMID.isin(cultures)).select("SUBJECT_ID", "HADM_ID", "ICUSTAY_ID", "STARTTIME").withColumn("ITEM", lit("CULTURES"))
proc_1 = proc_cult.join(icu_1, ["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"])

anti_cult = input_1.union(proc_1)
anti_cult_rdd = anti_cult.select("ICUSTAY_ID", "STARTTIME", "ITEM", "INTIME").rdd.map(lambda x: (x.ICUSTAY_ID, [x.STARTTIME, x.ITEM, x.INTIME]))

def check_infection(x):
    x = list(x)
    x = sorted(x)
    flag = 0
    curr_item = x[0][1]
    curr_time = x[0][0]
    intime = x[0][2]
    for e in x[1:]:
        next_item = e[1]
        next_time = e[0]
        if next_item == curr_item:
            curr_time = next_time
            continue
        #diff = next_time - curr_time
        #diff_hours = diff.days * 24 + diff.seconds / 3600
        diff_hours = time_diff_in_hours(next_time, curr_time)
        if curr_item == "ANTIBIOTICS":
            if diff_hours <= 72:
                return (1, curr_time, intime)
        if curr_item == "CULTURES":
            if diff_hours <= 24:
                return (1, curr_time, intime)
        curr_item = next_item
        curr_time = next_time
    return (flag, curr_time, intime)

infection = anti_cult_rdd.groupByKey().mapValues(check_infection).filter(lambda x: x[1][0] == 1).map(lambda x: (x[0], x[1][1], x[1][2]))
infection_df = spark.createDataFrame(infection).toDF("ICUSTAY_ID", "INFECTION_TIME", "INTIME")
#count: 1892

chart = spark.read.csv("../data/CHARTEVENTS.csv", header=True, inferSchema=True).select("ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM")
#chart_1 = icu_1.join(chart, ["ICUSTAY_ID"])
chart_infection = chart.join(infection_df, ["ICUSTAY_ID"])
SIRS_list = [223761, 220045, 224689, 220235, 220546]
chart_SIRS = chart_infection.filter(chart_infection.ITEMID.isin(SIRS_list))
SIRS_rdd = chart_SIRS.rdd.filter(lambda x: (abs(time_diff_in_hours(x.INFECTION_TIME, x.CHARTTIME)) <= 24))
SIRS_rdd = SIRS_rdd.map(lambda x: (x.ICUSTAY_ID, [x.CHARTTIME, x.ITEMID, x.VALUENUM, x.INTIME]))
# sofa <- does NOT work..
#chart_sofa = chart_infection.filter("ITEMID = 227428")
#sofa_rdd = chart_sofa.rdd.filter(lambda x: (abs(time_diff_in_hours(x.INFECTION_TIME, x.CHARTTIME)) <= 24))
#sofa_rdd = sofa_rdd.map(lambda x: (x.ICUSTAY_ID, [x.CHARTTIME, x.VALUENUM, x.INTIME]))

def check_SIRS(x):
    x = list(x)
    x = sorted(x)
    hourly = {}
    for e in x:
        itemid = e[1]
        val = e[2]
        datehour = datetime(e[0].year, e[0].month, e[0].day, e[0].hour)
        if datehour not in hourly:
            hourly[datehour] = 0
        if itemid == 223761: # Temp F
            if (val < 96.8) | (val > 100.4):
                hourly[datehour] += 1
        elif itemid == 220045: # Heart rate
            if val > 90:
                hourly[datehour] += 1
        elif (itemid == 224689) | (itemid == 220235): # Repiratory rate | PaCO2
            if itemid == 224689:
                if val > 20:
                    hourly[datehour] += 1
            else:
                if val < 32:
                    hourly[datehour] += 1
        else: # WBC
            if (val * 1000 > 12000) or (val * 1000 < 4000):
                hourly[datehour] += 1
        if hourly[datehour] >= 2:
            return (1, datehour, e[3])
    return (0, e[0], e[3])

def check_sofa(x):
    x = list(x)
    x_time = sorted(x, key=lambda y: y[0])
    x_sofa = sorted(x, key=lambda y: y[1])
    min_sofa = x_sofa[0]
    init_sofa = x_time[0]
    for e in x_time[1:]:
        if e[1] - init_sofa[1] >= 2:
            return (1, e[0], e[2])
        if (e[1] - min_sofa[1] >= 2) & (e[0] > min_sofa[0]):
            return (1, e[0], e[2])
    return (0, init_sofa[0], init_sofa[2])

#positive = sofa_rdd.groupByKey().mapValues(check_sofa).filter(lambda x: x[1][0] == 1 & time_diff_in_hours(x[1][1], x[1][2]) >= 6).map(lambda x: (x[0], x[1][1], x[1][1] - timedelta(5), x[1][1] - timedelta(6)))
#print(positive.count())
#print(positive.take(5))
#positive_df = spark.createDataFrame(positive).toDF("ICUSTAY_ID", "ONSET_TIME", "FEATURE1_TIME", "FEATURE2_TIME")
positive = SIRS_rdd.groupByKey().mapValues(check_SIRS).filter(lambda x: (x[1][0] == 1) & (time_diff_in_hours(x[1][1], x[1][2]) >= 6)).map(lambda x: (x[0], x[1][1], x[1][1] - timedelta(hours=4), x[1][1] - timedelta(hours=5)))
positive_df = spark.createDataFrame(positive).toDF("ICUSTAY_ID", "ONSET_TIME", "FEATURE1_TIME", "FEATURE2_TIME")
positive_df.coalesce(1).write.option("header", "true").csv("../data/ONSET.csv")
print(positive_df.count()) #635

positive_chart = positive_df.join(chart, ["ICUSTAY_ID"])
feat_list = [220050, 220045, 223761, 220210, 220277, 227013, 220051]
feat_chart = positive_chart.filter(positive_chart.ITEMID.isin(feat_list)).rdd
feat_chart.persist()
feat1 = feat_chart.filter(lambda x: x.FEATURE1_TIME == datetime(x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
feat2 = feat_chart.filter(lambda x: x.FEATURE2_TIME == datetime(x.CHARTTIME.year, x.CHARTTIME.month, x.CHARTTIME.day, x.CHARTTIME.hour))
print(feat1.count())
print(feat2.count())

spark.createDataFrame(feat1).coalesce(1).write.option("header", "true").csv("../data/POS_FEAT1.csv")
spark.createDataFrame(feat2).coalesce(1).write.option("header", "true").csv("../data/POS_FEAT2.csv")

# negative cases
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
