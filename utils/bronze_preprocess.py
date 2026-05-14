from pyspark.sql import SparkSession

def process_bronze_table(spark, input_path, output_path):
    
    path = input_path + '/*.csv'

    df = spark.read.csv(path, header=True, inferSchema=True)

    df.write.option('header', True).partitionBy('snapshot_date').mode('overwrite').csv(output_path)


