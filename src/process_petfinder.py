import boto3
import connect_to_s3 as cs3
import json
import os
import pandas as pd
from write_to_postgres import write_to_psql


s3 = boto3.client('s3')
s3r = boto3.resource('s3')

already_processed = [
              '2020-02-11_CA.json',
              '2020-02-11_NV.json',
              '2020-02-11_OR.json',
              '2020-02-11_TX.json',
              '2020-02-11_WA.json',
              '2020-02-11_K4B 1H9.json',
              '2020-02-11_M4M 2G3.json',
                '2020-01-28_97138.json',
                '2020-01-29_89019.json',
                '2020-01-29_97138.json',
                '2020-01-31_97138.json',
                '2020-02-01_97138.json',
                '2020-02-03_97138.json',
                '2020-02-04_97138.json',
                '2020-02-09_97138.json',
                '2020-02-11_89019.json',
                '2020-02-12_89019.json',
                '2020-02-13_89019.json',
                '2020-02-14_89019.json',
                '2020-02-11_89447.json',
                '2020-02-12_89447.json',
                '2020-02-13_89449.json',
                '2020-02-14_89447.json',
                '2020-02-12_89429.json',
                '2020-02-13_89429.json',
                '2020-02-14_89429.json',
                '2020-02-13_89447.json',
                '2020-02-14_89447.json',
                '2020-02-15_89447.json',
                '2020-02-15_89429.json',
                '2020-02-15_89015.json',
    'expected_output_10165.json',
    'expected_output_1010.json',
    'expected_output_10060.json',
    '2020-02-15_89044.json',
    '2020-02-15_89019.json',
    'expected_output_10222.json',
    'expected_output_10283.json',
    'expected_output_10003.json',
    'expected_output_10107.json',
    'expected_output_10327.json',
    'expected_output_10380.json',
    'expected_output_10437.json',
    'expected_output_10506.json',
              'expected_output.json']

# # # downloads files into data directory on EC2
# my_bucket = s3r.Bucket('fureverdump')
# for s3_object in my_bucket.objects.all():
#     # Need to split s3_object.key into path and file name, else it will give error file not found.
#     path, filename = os.path.split(s3_object.key)
#     if filename not in already_processed:
#         s3.download_file('fureverdump', s3_object.key, filename)
#         # my_bucket.download_file(s3_object.key, filename)
#
#
#
#
# # loops through files downloaded onto EC2
# # directory = os.fsencode('/home/ubuntu/data_files')
# # raw_files = []
# # for file in os.listdir(directory):
# #      filename = os.fsdecode(file)
# #      if filename.endswith(".json"):
# #          raw_files.append(filename)
# #          continuea
# #      else:
# #          continue


for key in s3.list_objects(Bucket='fureverdump')['Contents']:
    path = key['Key']
    print("working on file:" + path)
    if key['Key'][0] == '2' and key['Key'] not in already_processed and '2020-02-16' not in key['Key']:

        write_to_psql(cs3.spk_temp(path), 'animal_temperment')
        print("Data inserted to Postgres: Temperment")

        write_to_psql(cs3.spk_med(path), 'animal_medical_info')
        print("Data inserted to Postgres: Medical")

        write_to_psql(cs3.spk_descr(path), 'animal_description')
        print("Data inserted to Postgres: Description")

        write_to_psql(cs3.spk_status(path), 'animal_status')
        print("Data inserted to Postgres: Status")

        write_to_psql(cs3.spk_ani(path), 'animal_info')
        print("Data inserted to Postgres: Info")

    elif 'expected_output_' in  key['Key'] and key['Key'] not in already_processed:
        write_to_psql(cs3.spk_temp(path), 'animal_temperment')
        print("Data inserted to Postgres: Temperment")

        write_to_psql(cs3.spk_med(path), 'animal_medical_info')
        print("Data inserted to Postgres: Medical")

        write_to_psql(cs3.spk_descr(path), 'animal_description')
        print("Data inserted to Postgres: Description")

        write_to_psql(cs3.spk_status(path), 'animal_status')
        print("Data inserted to Postgres: Status")

        write_to_psql(cs3.spk_ani(path), 'animal_info')
        print("Data inserted to Postgres: Info")
    else:
        print("What didn't I insert?")
        print(key['Key'])
    print("Transfer Successful")








