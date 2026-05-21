import os
from pyspark.sql import SparkSession

def process_bronze_table(spark, input_path, output_path, date):
    
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    name = os.path.splitext(os.path.basename(input_path))[0]

    for row in df.select('snapshot_date').distinct().collect():
        snapshot = row.snapshot_date
        if date is not None and snapshot != date:
            continue
        this = df.filter(df.snapshot_date == snapshot)
        this.write.mode('overwrite').parquet(f'{output_path}/{name}_{snapshot}')