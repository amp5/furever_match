import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter

bucket = 'fureverdump'
file_name = 'address.csv'

s3 = boto3.client('s3')

obj = s3.get_object(Bucket= bucket, Key= file_name)

initial_df = pd.read_csv(obj['Body'],
                         names=["first_name", "last_name", "street", "city", "state", "zipcode"])

print(initial_df)

mySchema = StructType([ StructField("first_name", StringType(), True)\
                       ,StructField("last_name", StringType(), True)\
                       ,StructField("street", StringType(), True)\
                       ,StructField("city", StringType(), True)\
                       ,StructField("state", StringType(), True)\
                       ,StructField("zipcode", StringType(), True)])

spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName('furevermatch').getOrCreate()
spk_df = spark.createDataFrame(initial_df, schema=mySchema)

print(type(spk_df))






my_writer = DataFrameWriter(spk_df)

url_connect = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/database-1?user=postgres&password='fureverdb'"
table = "address"
mode = "overwrite"

my_writer.jdbc(url_connect, table, mode)

####### usr/local/spark/jars

#[postgresql]
url = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/database-1"
Database = "postgres"
username= "postgres"
password = "fureverdb"
driver="/Users/alexandraplassaras/src/furever_match/src/postgres_setup/postgresql-42.2.9.jar"


#Create the Database properties
db_properties={}
config = configparser.ConfigParser()
config.read("db_properties.ini")
db_prop = config['postgres']
db_url = db_prop['url']
db_properties['username']=db_prop['username']
db_properties['password']=db_prop['properties']
db_properties['url']=
db_properties['driver']=db_prop['driver']


#Save the dataframe to the table.
spk_df.write.jdbc(url=db_url,table='postgress.address',mode='overwrite',properties=db_properties)


# spark = SparkSession \
#     .builder \
#     .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
#     .appName("furevermatch") \
#     .getOrCreate()






#address.createOrReplaceTempView("log_table")
#address.printSchema()

# dataframe = spark.read.format('jdbc').options(
#        url = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/database-1?user=postgres&password='fureverdb'",
#         database='database-1',
#         dbtable='address'
#     ).load()
#
# dataframe.show()