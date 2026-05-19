import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when , col, regexp_extract


def join_tables(spark, input_path, output_path):
    fa = spark.read.parquet(os.path.join(input_path, 'features_attributes'))
    fc = spark.read.parquet(os.path.join(input_path, 'feature_clickstream'))
    ff = spark.read.parquet(os.path.join(input_path, 'features_financials'))

    feature_table = fa.join(fc, on=['Customer_ID', 'snapshot_date'], how='inner').join(ff, on=['Customer_ID', 'snapshot_date'], how='inner')
    feature_table.write.parquet(output_path)

def label_store(spark, input_path, output_path):
    lms = spark.read.parquet(os.path.join(input_path, 'lms_loan_daily'))
    label_table = lms.withColumn('label', when(col('overdue_amt') > 0, 1).otherwise(0))
    label_table = label_table.select('Customer_ID', 'snapshot_date', 'label')
    label_table.write.parquet(output_path)

def process_gold_tables(spark, input_path, output_path):
    join_tables(spark, input_path, os.path.join(output_path, 'features'))
    label_store(spark, input_path, os.path.join(output_path, 'labels'))