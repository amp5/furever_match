import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
from connect_to_s3 import read_s3
from write_to_postgres import write_to_psql

i_df = read_s3('fureverdump', 'address.csv')
print(i_df)

mySchema = StructType([ StructField("first_name", StringType(), True)\
                       ,StructField("last_name", StringType(), True)\
                       ,StructField("street", StringType(), True)\
                       ,StructField("city", StringType(), True)\
                       ,StructField("state", StringType(), True)\
                       ,StructField("zipcode", StringType(), True)])

spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName('furevermatch').getOrCreate()
spk_df = spark.createDataFrame(i_df, schema=mySchema)
print(spk_df)

### need to create all the different kinds of spark dfs and then write them to psql
write_to_psql(spk_df, "address")
#write_to_psql(spk_df, "animal")
#write_to_psql(spk_df, "organization")
