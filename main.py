import os
from pyspark.sql import SparkSession
import utils.bronze_preprocess as bp
import utils.silver_preprocess as sp
import utils.gold_preprocess as gp

def create_session():
    spk = SparkSession.builder \
        .appName('Table_preprocessing') \
        .getOrCreate()
    return spk


def make_bronze_table():

    bronze_dir = 'datamart/bronze'
    if not os.path.exists(bronze_dir):
        os.makedirs(bronze_dir)

    PATH = "./data"

    for file in os.listdir(PATH):
        input_path = os.path.join(PATH, file)
        file_name = os.path.splitext(os.path.basename(file))[0]
        output_path = os.path.join(bronze_dir, file_name)
        prep = bp.process_bronze_table(create_session(), input_path, output_path)

def make_silver_table():
    silver_dir = 'datamart/silver'
    if not os.path.exists(silver_dir):
        os.makedirs(silver_dir)

    sp.process_silver_tables(create_session(), 'datamart/bronze', silver_dir)

def make_gold_table():
    gold_dir = 'datamart/gold'
    if not os.path.exists(gold_dir):
        os.makedirs(gold_dir)

    gp.process_gold_tables(create_session(), 'datamart/silver', gold_dir)

if __name__ == "__main__":
    make_bronze_table()
    make_silver_table()
    make_gold_table()