import os
from pyspark.sql import SparkSession
import utils.bronze_preprocess as bp

# def create_session():
#     spk = SparkSession.builder \
#         .appName('Table_preprocessing') \
#         .getOrCreate()
#     return spk



# bronze_dir = 'datamart/bronze'
# if not os.path.exists(bronze_dir):
#     os.makedirs(bronze_dir)

# silver_dir = 'datamart/silver'
# if not os.path.exists(silver_dir):
#     os.makedirs(silver_dir)

# gold_dir = 'datamart/gold'
# if not os.path.exists(gold_dir):
#     os.makedirs(gold_dir)

PATH = "./data"

date = "2024-06-01"

for file in os.listdir(PATH):
    print(os.path.splitext(os.path.basename(file))[0]+'_'+date)