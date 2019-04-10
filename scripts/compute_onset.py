from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, datediff, expr, min as Fmin
from pyspark.sql.types import *
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("Computing Onset").getOrCreate()
timeFmt = "yyyy-MM-dd HH:mm:ss"

def time_diff_in_hours(x, y):
    diff = x - y
    days = diff.days
    seconds = diff.seconds
    return days * 24 + seconds / 3600

infection_df = spark.read.csv("../data/INFECTION/*.csv", header=True, inferSchema=True)
icu_1 = spark.read.csv("../data/ICU_1/*.csv", header=True, inferSchema=True)
chart = spark.read.csv("../data/CHARTEVENTS.csv", header=True, inferSchema=True).select("ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM")
#chart_1 = icu_1.join(chart, ["ICUSTAY_ID"])
chart_infection = chart.join(infection_df, ["ICUSTAY_ID"])
SIRS_list = [223761, 223762, 220045, 224689, 220235, 220546]
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
    temp = {}
    hr = {}
    rr = {}
    wbc = {}
    for e in x:
        itemid = e[1]
        val = e[2]
        datehour = datetime(e[0].year, e[0].month, e[0].day, e[0].hour)
        if datehour not in hourly:
            hourly[datehour] = 0
            temp[datehour] = 0
            hr[datehour] = 0
            rr[datehour] = 0
            wbc[datehour] = 0
        if (itemid == 223761) | (itemid == 223762): # Temp F / C
            if temp[datehour] == 0:
                if itemid == 223761:
                    if (val < 96.8) | (val > 100.4):
                        hourly[datehour] += 1
                        temp[datehour] += 1
                else:
                    if (val < 36) | (val > 38):
                        hourly[datehour] += 1
                        temp[datehour] += 1
        elif itemid == 220045: # Heart rate
            if hr[datehour] == 0:
                if val > 90:
                    hourly[datehour] += 1
                    hr[datehour] += 1
        elif (itemid == 224689) | (itemid == 220235): # Repiratory rate | PaCO2
            if rr[datehour] == 0:
                if itemid == 224689:
                    if val > 20:
                        hourly[datehour] += 1
                        rr[datehour] += 1
                else:
                    if val < 32:
                        hourly[datehour] += 1
                        rr[datehour] += 1
        else: # WBC
            if wbc[datehour] == 0:
                if (val * 1000 > 12000) or (val * 1000 < 4000):
                    hourly[datehour] += 1
                    wbc[datehour] += 1
        if hourly[datehour] >= 2:
            return (1, datehour, e[3])
    return (0, e[0], e[3])

# def check_sofa(x):
#     x = list(x)
#     x_time = sorted(x, key=lambda y: y[0])
#     x_sofa = sorted(x, key=lambda y: y[1])
#     min_sofa = x_sofa[0]
#     init_sofa = x_time[0]
#     for e in x_time[1:]:
#         if e[1] - init_sofa[1] >= 2:
#             return (1, e[0], e[2])
#         if (e[1] - min_sofa[1] >= 2) & (e[0] > min_sofa[0]):
#             return (1, e[0], e[2])
#     return (0, init_sofa[0], init_sofa[2])

#positive = sofa_rdd.groupByKey().mapValues(check_sofa).filter(lambda x: x[1][0] == 1 & time_diff_in_hours(x[1][1], x[1][2]) >= 6).map(lambda x: (x[0], x[1][1], x[1][1] - timedelta(5), x[1][1] - timedelta(6)))
#print(positive.count())
#print(positive.take(5))
#positive_df = spark.createDataFrame(positive).toDF("ICUSTAY_ID", "ONSET_TIME", "FEATURE1_TIME", "FEATURE2_TIME")
positive = SIRS_rdd.groupByKey().mapValues(check_SIRS).filter(lambda x: (x[1][0] == 1) & (time_diff_in_hours(x[1][1], x[1][2]) >= 6)).map(lambda x: (x[0], x[1][1], x[1][1] - timedelta(hours=4), x[1][1] - timedelta(hours=5)))
positive_df = spark.createDataFrame(positive).toDF("ICUSTAY_ID", "ONSET_TIME", "FEATURE1_TIME", "FEATURE2_TIME")
positive_df.write.option("header", "true").csv("../data/POSITIVE")
#print(positive_df.count()) #635

spark.stop()
