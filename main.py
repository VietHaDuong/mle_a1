import os
# from pyspark.sql import SparkSession

# def create_session():
#     spk = SparkSession.builder \
#         .appName('Table_preprocessing') \
#         .getOrCreate()
#     return spk

PATH = "./data/lms_loan_daily.csv"

print(os.path.basename(PATH))
print(os.path.splitext(PATH))

print(os.path.splitext(os.path.basename(PATH)))