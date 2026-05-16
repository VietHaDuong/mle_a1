import os
from pyspark.sql import SparkSession
import utils.bronze_preprocess as bp

def create_session():
    spk = SparkSession.builder \
        .appName('Table_preprocessing') \
        .getOrCreate()
    return spk


bronze_dir = 'datamart/bronze'
if not os.path.exists(bronze_dir):
    os.makedirs(bronze_dir)

silver_dir = 'datamart/silver'
if not os.path.exists(silver_dir):
    os.makedirs(silver_dir)

gold_dir = 'datamart/gold'
if not os.path.exists(gold_dir):
    os.makedirs(gold_dir)

# PATH = "./data/lms_loan_daily.csv"

# print(os.path.basename(PATH))
# print(os.path.splitext(PATH))

# print(os.path.splitext(os.path.basename(PATH))[0])