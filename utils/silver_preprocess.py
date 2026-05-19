import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when , col

def clean_lms(spark, input_path, output_path):
    for date in os.listdir(input_path):
        if os.path.isdir(os.path.join(input_path, date)):
            this = spark.read.parquet(os.path.join(input_path, date))
            this = this.filter(this['tenure'] == this['installment_num'])
            this.write.parquet(os.path.join(output_path, date))

def clean_attributes(spark, input_path, output_path):
    for date in os.listdir(input_path):
        if os.path.isdir(os.path.join(input_path, date)):
            fa = spark.read.parquet(os.path.join(input_path, date))
            fa = fa.drop('SSN', 'Name')
            fa = fa.withColumn('Age', fa['Age'].cast('integer'))
            median_age = fa.approxQuantile('Age', [0.5], 0.01)[0]
            fa = fa.withColumn('Age', when((col('Age') < 18) | (col('Age') > 99), median_age).otherwise(col('Age')))
            fa = fa.replace('_______', 'Unknown', subset=['Occupation'])
            fa = fa.replace('', 'Unknown', subset=['Occupation'])
            fa.write.parquet(os.path.join(output_path, date))

def clean_financials(spark, input_path, output_path):
    columns_to_clean = {'Annual_Income': 'double',
    'Monthly_Inhand_Salary': 'double',
    'Num_Bank_Accounts': 'integer',
    'Num_Credit_Card': 'integer',
    'Interest_Rate': 'integer',
    'Num_of_Loan': 'integer',
    'Delay_from_due_date': 'integer',
    'Num_of_Delayed_Payment': 'integer',
    'Changed_Credit_Limit': 'double',
    'Amount_invested_monthly': 'double',
    'Monthly_Balance': 'double'}
    columns_to_replace = ['Num_Credit_Card', 'Num_of_Loan', 'Num_of_Delayed_Payment', 'Delay_from_due_date']
    for date in os.listdir(input_path):
        if os.path.isdir(os.path.join(input_path, date)):
            ff = spark.read.parquet(os.path.join(input_path, date))
            ff = ff.replace('_', 'Unknown', subset=['Credit_Mix'])
            ff = ff.replace('!@9#%8', 'Unknown', subset=['Payment_Behaviour'])
            for c, dtype in columns_to_clean.items():
                ff = ff.withColumn(c, regexp_replace(col(c), '[^0-9.]', '').cast(dtype))
            for c in columns_to_replace:
                ff = ff.withColumn(c, when(col(c) < 0, 0).otherwise(col(c)))
            ff = ff.withColumn('Num_Bank_Accounts', when(col('Num_Bank_Accounts') < 0, 1).otherwise(col('Num_Bank_Accounts')))
            ff = ff.drop('Type_of_Loan')
            ff.write.parquet(os.path.join(output_path, date))

def pass_through_fc(spark, input_path, output_path):
    for date in os.listdir(input_path):
        if os.path.isdir(os.path.join(input_path, date)):
            ff = spark.read.parquet(os.path.join(input_path, date))
            ff.write.parquet(os.path.join(output_path, date))

def process_silver_tables(spark, input_path, output_path):
    clean_lms(spark, os.path.join(input_path, 'lms_loan_daily'), os.path.join(output_path, 'lms'))
    clean_attributes(spark, os.path.join(input_path, 'features_attributes'), os.path.join(output_path, 'attributes'))
    clean_financials(spark, os.path.join(input_path, 'features_financials'), os.path.join(output_path, 'financials'))
    pass_through_fc(spark, os.path.join(input_path, 'feature_clickstream'), os.path.join(output_path, 'fc'))