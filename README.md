# CSE6250-Project-Sepsis-Prediction
CSE6250 Big Data Health - Project - Sepsis Prediction

Zhijing Wu\
Lu Zheng

## Environment Setup (assuming base == Python 3.6.8)
Install pyspark:\
python -m venv env\
source ./env/bin/activate\
pip install pyspark
deactivate

## 1. Compute onset & perform feature extraction
source ./env/bin/activate\
python compute_onset_feature_extraction.py\
python negative_features.py
deactivate

## 2. Feature cleanup; preparation for modelling


