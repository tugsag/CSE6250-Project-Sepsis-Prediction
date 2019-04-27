# CSE6250-Project-Sepsis-Prediction
CSE6250 Big Data Health - Project - Sepsis Prediction

Zhijing Wu\
Lu Zheng

## Video Presentation
Video presentation of this project can be accessed at:

## Environment Setup (assuming base with anaconda)
### Create environment
conda env create -f environment.yml\

## Test saved models (assuming at root)
jupyter notebook\
Go to "notebook" folder, open and run "Testing.ipynb" **(make sure kernel is set to "sepsis")**

## Complete workflow
(assuming ALL raw MIMIC data required are in data/ folder - see the description in that folder for a complete list of dataset needed)
### 1. Compute onset & feature extraction using PySpark 
(if PySpark is NOT installed properly during conda env create -f environment.yml, please install it using pip)\
source activate sepsis\
cd script\
python compute_infection.py\
python compute_onset.py\
python positive_features.py\
python negative_features.py

### 2. Feature cleanup, create test set
python create_train_test_set.py

### 3. Train, tune, validate
cd ..\
jupyter notebook\
Go to "notebook" folder, open and run "Train_Validate.ipynb" **(make sure kernel is set to "sepsis")**

### 4. Testing
open and run "Testing.ipynb" **(make sure kernel is set to "sepsis")**
