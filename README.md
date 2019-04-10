# CSE6250-Project-Sepsis-Prediction
CSE6250 Big Data Health - Project - Sepsis Prediction

Zhijing Wu\
Lu Zheng

## Environment Setup (assuming base == Python 3.6.8 with anaconda)
### Install pyspark inside venv
python -m venv env\
source ./env/bin/activate\
pip install pyspark\
deactivate

### Create conda environment
conda env create -f environment.yml 

## 1. Compute onset & feature extraction using PySpark
source ./env/bin/activate\
python script/compute_infection.py\
python script/compute_onset.py\
python script/positive_features.py\
python script/negative_features.py\
deactivate

## 2. Feature cleanup; preparation for modelling
source activate sepsis\

