import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter


def write_to_psql(df, table):
    """ input is spark dataframe and the postgres table df needs to be written to """

    # modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'
    mode = "overwrite"
    url = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/postgres"
    properties = {"user": "postgres",
                  "password": "fureverdb",
                  "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url,
                  table=table,
                  mode=mode,
                  properties=properties)