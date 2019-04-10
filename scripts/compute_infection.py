from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Computing Infection").getOrCreate()
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
icu_1.write.option("header", "true").csv("../data/ICU_1")
icu_0.write.option("header", "true").csv("../data/ICU_0")

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
infection_df.write.option("header", "true").csv("../data/INFECTION")

spark.stop()
