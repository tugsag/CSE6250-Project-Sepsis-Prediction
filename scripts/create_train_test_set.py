import numpy as np
import pandas as pd
import glob
import datetime

icu = pd.read_csv("../data/ICUSTAYS.csv")[["ICUSTAY_ID", "SUBJECT_ID", "INTIME"]]
patient = pd.read_csv("../data/PATIENTS.csv")[["SUBJECT_ID", "GENDER", "DOB"]]
patient_icu = pd.merge(icu, patient, how='inner', on="SUBJECT_ID")
patient_icu["DOB"] = pd.to_datetime(patient_icu["DOB"])
patient_icu["INTIME"] = pd.to_datetime(patient_icu["INTIME"])
patient_icu["AGE"] = patient_icu.apply(lambda x: (x.INTIME - x.DOB).days / 365, axis=1)
age_gender = patient_icu[["ICUSTAY_ID", "AGE", "GENDER"]]

def read_pos(x):
    pospath = glob.glob("../data/POS_FEAT" + str(x) + "/*.csv")
    pos = pd.concat((pd.read_csv(f) for f in pospath))[["ICUSTAY_ID", "ITEMID", "VALUENUM"]]
    pos = pos.groupby(["ICUSTAY_ID", "ITEMID"]).mean().reset_index()
    pivoted = pos.pivot(index="ICUSTAY_ID", columns="ITEMID", values="VALUENUM")
    pivoted = pivoted.reset_index()[["ICUSTAY_ID",220045,220050,220210,220277]].dropna()
    #pivoted = pd.merge(pivoted, age_gender, how='inner', on="ICUSTAY_ID")
    return pivoted.rename({220045: "HR" + str(x), 220050: "ABP" + str(x), 220210:"RR" + str(x), 220277: "SpO2" + str(x)}, axis='columns')

pos1 = read_pos(1)
pos2 = read_pos(2)
posall = pd.merge(pos1, pos2, how="inner", on="ICUSTAY_ID")
posall["HRdiff"] = posall["HR1"] - posall["HR2"]
posall["ABPdiff"] = posall["ABP1"] - posall["ABP2"]
posall["RRdiff"] = posall["RR1"] - posall["RR2"]
posall["SpO2diff"] = posall["SpO21"] - posall["SpO22"]
posall = pd.merge(posall, age_gender, how="inner", on="ICUSTAY_ID")
posall["GENDER"] = np.where(posall['GENDER'] == 'F', 0, 1)
posall["SEPSIS"] = 1

def read_neg(x):
    negpath = glob.glob("../data/NEG_FEAT" + str(x) + "/*.csv")
    everything = pd.concat((pd.read_csv(f) for f in negpath))
    neg = everything[["ICUSTAY_ID", "ITEMID", "VALUENUM"]]
    age_gen = everything[["ICUSTAY_ID", "AGE", "GENDER"]].drop_duplicates()
    neg = neg.groupby(["ICUSTAY_ID", "ITEMID"]).mean().reset_index()
    pivoted = neg.pivot(index="ICUSTAY_ID", columns="ITEMID", values="VALUENUM")
    pivoted = pivoted.reset_index()[["ICUSTAY_ID",220045,220050,220210,220277]].dropna()
    pivoted = pivoted.rename({220045: "HR" + str(x), 220050: "ABP" + str(x), 220210:"RR" + str(x), 220277: "SpO2" + str(x)}, axis='columns')
    return pd.merge(pivoted, age_gen, how="inner", on="ICUSTAY_ID")

neg1 = read_neg(1)
neg2 = read_neg(2)
negall = pd.merge(neg1, neg2, how="inner", on=["ICUSTAY_ID", "AGE", "GENDER"])
negall["HRdiff"] = negall["HR1"] - negall["HR2"]
negall["ABPdiff"] = negall["ABP1"] - negall["ABP2"]
negall["RRdiff"] = negall["RR1"] - negall["RR2"]
negall["SpO2diff"] = negall["SpO21"] - negall["SpO22"]
negall["GENDER"] = np.where(negall['GENDER'] == 'F', 0, 1)
negall["SEPSIS"] = 0

df = pd.concat([posall, negall], sort=False)

from sklearn.model_selection import train_test_split

y = df["SEPSIS"]
X = df.drop(["ICUSTAY_ID", "SEPSIS"], axis=1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

X_train["SEPSIS"] = y_train
X_test["SEPSIS"] = y_test

X_train.to_csv("../data/TRAIN.csv", index=False)
X_test.to_csv("../data/TEST.csv", index=False)
