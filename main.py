import os
import argparse
from pyspark.sql import SparkSession
import utils.bronze_preprocess as bp
import utils.silver_preprocess as sp
import utils.gold_preprocess as gp

def create_session():
    spk = SparkSession.builder \
        .appName('Table_preprocessing') \
        .getOrCreate()
    return spk

def make_bronze_table(spark, date):

    bronze_dir = 'datamart/bronze'
    if not os.path.exists(bronze_dir):
        os.makedirs(bronze_dir)

    PATH = "./data"

    for file in os.listdir(PATH):
        input_path = os.path.join(PATH, file)
        file_name = os.path.splitext(os.path.basename(file))[0]
        output_path = os.path.join(bronze_dir, file_name)
        prep = bp.process_bronze_table(spark, input_path, output_path, date)

def make_silver_table(spark, date):
    silver_dir = 'datamart/silver'
    if not os.path.exists(silver_dir):
        os.makedirs(silver_dir)

    sp.process_silver_tables(spark, 'datamart/bronze', silver_dir, date)

def make_gold_table(spark, date):
    gold_dir = 'datamart/gold'
    if not os.path.exists(gold_dir):
        os.makedirs(gold_dir)

    gp.process_gold_tables(spark, 'datamart/silver', gold_dir, date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default=None)
    args = parser.parse_args()

    spark = create_session()
    date = args.date
    make_bronze_table(spark, date)
    make_silver_table(spark, date)
    make_gold_table(spark, date)