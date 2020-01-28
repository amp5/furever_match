import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter


def read_s3(bucket, file_name):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket= bucket, Key= file_name)

    # this will change with real data
    initial_df = pd.read_csv(obj['Body'],
                             names=["first_name",
                                    "last_name",
                                    "street", "city",
                                    "state", "zipcode"])
    return initial_df

i_df = read_s3('fureverdump', 'address.csv')

#### reformat json files to flatten



##### create spark schemas


##### write to postgres

mySchema = StructType([ StructField("first_name", StringType(), True)\
                       ,StructField("last_name", StringType(), True)\
                       ,StructField("street", StringType(), True)\
                       ,StructField("city", StringType(), True)\
                       ,StructField("state", StringType(), True)\
                       ,StructField("zipcode", StringType(), True)])

spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName('furevermatch').getOrCreate()
spk_df = spark.createDataFrame(i_df, schema=mySchema)

print(type(spk_df))


def write_to_psql(df, table):
    """ input is spark dataframe and the postgres table df needs to be written to """

    # modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'
    mode = "append"
    url = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/postgres"
    properties = {"user": "postgres",
                  "password": "fureverdb",
                  "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url,
                  table=table,
                  mode=mode,
                  properties=properties)



write_to_psql(spk_df, "address")