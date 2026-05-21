import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when , col, regexp_extract


def join_tables(spark, input_path, output_path, date):
    fa = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(input_path, 'features_attributes'))
    fc = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(input_path, 'feature_clickstream'))
    ff = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(input_path, 'features_financials'))

    if date is not None:
        fa = fa.filter(fa.snapshot_date == date)
        fc = fc.filter(fc.snapshot_date == date)
        ff = ff.filter(ff.snapshot_date == date)

    feature_table = fa.join(fc, on=['Customer_ID', 'snapshot_date'], how='inner').\
        join(ff, on=['Customer_ID', 'snapshot_date'], how='inner')
    feature_table.write.mode('overwrite').parquet(output_path)

def label_store(spark, input_path, output_path, date):
    lms = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(input_path, 'lms_loan_daily'))
    if date is not None:
        lms = lms.filter(lms.snapshot_date == date)
    label_table = lms.withColumn('label', when(col('overdue_amt') > 0, 1).otherwise(0))
    label_table = label_table.select('Customer_ID', 'snapshot_date', 'label')
    label_table.write.mode('overwrite').parquet(output_path)

def process_gold_tables(spark, input_path, output_path, date):
    join_tables(spark, input_path, os.path.join(output_path, 'features'), date)
    label_store(spark, input_path, os.path.join(output_path, 'labels'), date)