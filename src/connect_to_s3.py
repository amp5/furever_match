import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
import json


def read_s3(bucket, file_name):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=file_name)

    # this will change with real data
    initial_df = pd.read_csv(obj['Body'],
                             names=["first_name",
                                    "last_name",
                                    "street", "city",
                                    "state", "zipcode"])
    return initial_df


i_df = read_s3('fureverdump', 'address.csv')

######### new version #########
key = 'expected_output_0.json'
# key = '2020-01-28_97525.json'
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='fureverdump', Key=key)



text = obj["Body"].read().decode()
json_text = json.loads(text)


def find_values(id, json_repr):
    results = []

    def _decode_dict(a_dict):
        try:
            results.append(a_dict[id])
        except KeyError:
            pass
        return a_dict

    json.loads(json_repr, object_hook=_decode_dict)  # Return value ignored.
    return results


def breakup_json(json_file, key_list, col_list):
    list_df = []
    for key in key_list:
        if key in col_list:
            dict_vs = {key: find_values(key, json_file)[0]}
            list_df.append(dict_vs)
        else:
            other_list = find_values(key, json_file)
    combined_list = {**list_df[0], **other_list[0]}
    pd_vs = pd.DataFrame([combined_list])
    return pd_vs


def val_json(json_file, key_name):
    dict_vs = {key_name: find_values(key_name, json_file)[0]}
    return dict_vs


def nested_json(json_file, key_name, parent_name):
    parent = find_values(parent_name, json_file)[0]
    child_val = parent[key_name]
    new_dict = {key_name: child_val}
    return new_dict


def get_key(val):
    for key, value in my_dict.items():
        if val == value:
            return key

    return "key doesn't exist"

###################### Spark Med DF ############################
def spk_med():
    id_d = val_json(text, 'id')
    housetrained_d = nested_json(text, 'house_trained', 'attributes')
    declawed_d = nested_json(text, 'declawed', 'attributes')
    spayed_neutered_d = nested_json(text, 'spayed_neutered', 'attributes')
    special_needs_d = nested_json(text, 'special_needs', 'attributes')
    shots_current_d = nested_json(text, 'shots_current', 'attributes')

    final_order = [id_d, declawed_d, housetrained_d, shots_current_d, special_needs_d, spayed_neutered_d]

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("declawed", BooleanType(), True) \
                           , StructField("house_trained", BooleanType(), True) \
                           , StructField("shots_current", BooleanType(), True) \
                           , StructField("special_needs", BooleanType(), True) \
                           , StructField("spayed_neutered", BooleanType(), True)])

    spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName(
        'furevermatch').getOrCreate()
    spk_df = spark.createDataFrame(final_order, schema=spkschema)
    return spk_df


med_spk = spk_med()
print(med_spk)

###################### Spark description DF ############################
def spk_descr():
    id_d = val_json(text, 'id')
    descriptiion_d = val_json(text, 'description')
    final_order = [id_d, descriptiion_d]

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("description", StringType(), True) ])
    spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName('furevermatch').getOrCreate()
    spk_df = spark.createDataFrame(final_order, schema=spkschema)
    return spk_df

descr_spk = spk_descr()
print(descr_spk)





###################### Spark temperment DF ############################
def spk_temp():
    id_d = val_json(text, 'id')
    kids_d = nested_json(text, 'children', 'environment')
    dogs_d = nested_json(text, 'dogs', 'environment')
    cats_d = nested_json(text, 'cats', 'environment')
    final_order = [id_d, kids_d, dogs_d, cats_d]

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("children", BooleanType(), True) \
                           , StructField("dogs", BooleanType(), True) \
                           , StructField("cats", BooleanType(), True)])

    spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName(
        'furevermatch').getOrCreate()
    spk_df = spark.createDataFrame(final_order, schema=spkschema)
    return spk_df

temp_spk = spk_temp()
print(temp_spk)

###################### Spark status DF ############################
def spk_status():
    id_d = val_json(text, 'id')
    status_d = val_json(text, 'status')
    status_chg_d = val_json(text, 'status_changed_at')
    final_order = [id_d, status_d, status_chg_d]

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("status", StringType(), True) \
                            , StructField("status_changed_at", StringType(), True)])
    spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName('furevermatch').getOrCreate()
    spk_df = spark.createDataFrame(final_order, schema=spkschema)
    return spk_df

status_spk = spk_status()
print(status_spk)

# status_spk###################### Spark media DF ############################
# def spk_media():
#     id_d = val_json(text, 'id')
#     photos = val_json(text, 'photos')['photos']
#     photo1 = photos[0]['full']
#     photo2 = photos[1]['full']
#     photo3 = photos[2]['full']
#     final_order = [id_d, photo1, photo2, photo3]
#
#     spkschema = StructType([StructField("id", IntegerType(), True) \
#                            , StructField("photo1", StringType(), True) \
#                            , StructField("photo2", StringType(), True) \
#                            , StructField("photo3", StringType(), True)])
#
#     spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName(
#         'furevermatch').getOrCreate()
#     spk_df = spark.createDataFrame(final_order, schema=spkschema)
#     return spk_df
#
# media_spk = spk_media()
# print(media_spk)



###################### Spark animal DF ############################
def spk_ani():
    id_d = val_json(text, 'id')
    org_d = val_json(text, 'organization_id')
    name_d = val_json(text, 'name')
    size_d = val_json(text, 'size')
    age_d = val_json(text, 'age')
    gender_d = val_json(text, 'gender')
    breed_pri_d = nested_json(text, 'primary', 'breeds')
    breed_sec_d = nested_json(text, 'secondary', 'breeds')
    breed_mix_d = nested_json(text, 'mixed', 'breeds')
    breed_unkn_d = nested_json(text, 'unknown', 'breeds')
    color_pri_d = nested_json(text, 'primary', 'colors')
    color_sec_d = nested_json(text, 'secondary', 'colors')
    color_mix_d = nested_json(text, 'tertiary', 'colors')
    coat_d = val_json(text, 'coat')
    date_added = val_json(text, 'published_at')

    final_order = [id_d,
                   org_d,
                   name_d,
                   size_d,
                   age_d,
                   gender_d,
                   breed_pri_d,
                   breed_sec_d,
                   breed_mix_d,
                   breed_unkn_d,
                   color_pri_d,
                   color_sec_d,
                   color_mix_d,
                   coat_d,
                   date_added]

    spkschema = StructType([StructField("id", IntegerType(), True) \
                            , StructField("org", StringType(), True) \
                            , StructField("name", StringType(), True) \
                            , StructField("size", StringType(), True) \
                            , StructField("age", StringType(), True) \
                            , StructField("gender", StringType(), True) \
                            , StructField("breed_pri", StringType(), True) \
                            , StructField("breed_sec", StringType(), True)
                            , StructField("breed_mix", StringType(), True) \
                            , StructField("breed_unkn", StringType(), True) \
                            , StructField("color_pri", StringType(), True)
                            , StructField("color_sec", StringType(), True) \
                            , StructField("color_mix", StringType(), True) \
                            , StructField("coat", StringType(), True) \
                            , StructField("date_added", StringType(), True)])

    spark = SparkSession.builder.config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar").appName(
        'furevermatch').getOrCreate()
    spk_df = spark.createDataFrame(final_order, schema=spkschema)
    return spk_df


ani_spk = spk_ani()
print(ani_spk)

###################### Spark tag DF ############################
###################### Spark organization DF ############################



# create functions for removing out parts of the json file
# then those functs will return a spark df that I can then load onto postgres
##### write to postgres