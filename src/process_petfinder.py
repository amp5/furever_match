import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
from write_to_postgres import write_to_psql
import connect_to_s3 as cs3

s3 = boto3.client('s3')

already_processed = [
              '2020-02-11_CA.json',
              '2020-02-11_NV.json',
              '2020-02-11_OR.json',
              '2020-02-11_TX.json',
              '2020-02-11_WA.json',
              'expected_output.json']
real_raw = []
fake_raw = []
for key in s3.list_objects(Bucket='fureverdump')['Contents']:
    if key['Key'][0] == '2' and key['Key'] not in already_processed:
        real_raw.append(key['Key'])
    elif 'expected_output_' in  key['Key'] and key['Key'] not in already_processed:
        fake_raw.append(key['Key'])
#files_to_process = real_raw
#print(files_to_process)
#print(len(real_raw))



files_to_process = fake_raw[3:10]
#print(fake_raw[:100])
#print(files_to_process)



for key in files_to_process:
    obj = s3.get_object(Bucket='fureverdump', Key=key)
    text = obj["Body"].read().decode()


    med_info = cs3.spk_med(text)
    print('processed med -------------------------')
    animal_info = cs3.spk_ani(text)
    print('processed ani -------------------------')
    description_info = cs3.spk_descr(text)
    print('processed desc -------------------------')
    status_info = cs3.spk_status(text)
    print('processed stat -------------------------')
    temperment_info = cs3.spk_temp(text)
    print('processed temp -------------------------')

    write_to_psql(med_info, 'animal_medical_info')
    print('written to db! - med')
    print("now what?")
    write_to_psql(animal_info, 'animal_info')
    print('written to db! - info')
    write_to_psql(description_info, 'animal_description')
    print('written to db! - desc')
    write_to_psql(status_info, 'animal_status')
    print('written to db! - stat')
    write_to_psql(temperment_info, 'animal_temperment')

print(files_to_process)
print(len(files_to_process))