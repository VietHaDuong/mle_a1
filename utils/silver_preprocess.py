import os
from pyspark.sql import SparkSession

def process_silver_table(spark, input_path, output_path):
    
    df = spark.read.parquet(input_path)

    name = os.path.basename(input_path)

    df.write.parquet(f'{output_path}/{name}')