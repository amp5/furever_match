import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
from write_to_postgres import write_to_psql
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext
import zlib
from ast import literal_eval
from collections import ChainMap
from collections import OrderedDict
import functools
from pyspark.sql.functions import col, when
from functools import reduce


spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
    .appName('furevermatch') \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(spark)


s3 = boto3.client('s3')
resource = boto3.resource('s3')
my_bucket = resource.Bucket('fureverdump')


raw_org = ['org_info_CA.csv',
           'org_info_NV.csv',
           'org_info_OR.csv',
           'org_info_TX.csv',
           'org_info_WA.csv']


obj = s3.get_object(Bucket='fureverdump', Key=raw_org[0])
df = pd.read_csv(obj['Body'])

read_dfs = []
for file in raw_org:
    obj = s3.get_object(Bucket='fureverdump', Key=file)
    df = pd.read_csv(obj['Body'])
    read_dfs.append(df)

org_df = pd.concat(read_dfs)
final_org = org_df[['id', 'name']]
f_o = final_org.drop_duplicates()

spkschema = StructType([StructField("organization_id", StringType(), True) \
                           , StructField("organization_name", StringType(), True) ])

spk_df = spark.createDataFrame(f_o, spkschema)
write_to_psql(spk_df, 'organization_info')
print("completed!")