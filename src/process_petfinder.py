import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
from write_to_postgres import write_to_psql
import connect_to_s3 as cs3

s3 = boto3.client('s3')
spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
    .appName('furevermatch') \
    .getOrCreate()

sc = spark.sparkContext

key = 'expected_output_0.json'
key_list = [key]

for key in key_list:
    obj = s3.get_object(Bucket='fureverdump', Key=key)
    text = obj["Body"].read().decode()

    med_info = cs3.spk_med(text)
    animal_info = cs3.spk_ani(text)
    description_info = cs3.spk_descr(text)
    status_info = cs3.spk_status(text)
    temperment_info = cs3.spk_temp(text)

    write_to_psql(med_info, 'animal_medical_info')
    write_to_psql(animal_info, 'animal_info')
    write_to_psql(description_info, 'animal_description')
    write_to_psql(status_info, 'animal_status')
    write_to_psql(temperment_info, 'animal_temperment')