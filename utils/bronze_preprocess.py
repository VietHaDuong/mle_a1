import os
from pyspark.sql import SparkSession

def process_bronze_table(spark, input_path, output_path):
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    name = os.path.splitext(os.path.basename(input_path))[0]

    for row in df.select('snapshot_date').distinct().collect():
        date = row.snapshot_date
        this = df.filter(df.snapshot_date == date)
        this.write.parquet(f'{output_path}/{name}_{date}')

    
