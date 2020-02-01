import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
import json
from write_to_postgres import write_to_psql
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext
import zlib
from ast import literal_eval
#yes
from collections import ChainMap
from pyspark.sql import Row
from collections import OrderedDict
from pyspark.sql.types import *
import functools
from pyspark.sql.functions import col, when
from functools import reduce


key = 'expected_output_0.json'
s3 = boto3.client('s3')

obj = s3.get_object(Bucket='fureverdump', Key=key)
text = obj["Body"].read().decode()

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
    .appName('furevermatch') \
    .getOrCreate()

sc = spark.sparkContext

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
    return_list = []
    all_vals = find_values(key_name, json_file)
    for val in all_vals:
        pair = {key_name : val}
        return_list.append(pair)
    return return_list

def nested_json(json_file, key_name, parent_name):
    parents = find_values(parent_name, json_file)
    children_lst = []
    for i in range(len(parents)):
        if parent_name == 'breeds' or parent_name == 'colors':
            parent = find_values(parent_name, json_file)[i]
            child_val = parent[key_name]
            new_name = parent_name + '_' + key_name
            pair = {new_name: child_val}
        else:
            parent = find_values(parent_name, json_file)[i]
            child_val = parent[key_name]
            pair = {key_name : child_val}
        children_lst.append(pair)
    return children_lst


def get_key(val):
    for key, value in my_dict.items():
        if val == value:
            return key
    return "key doesn't exist"


def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))

def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)


def creating_new_rows(raw_results, schema):
    cat_rows = []
    for cat in raw_results:
        final_dict = dict(ChainMap(*cat))
        newRow = spark.createDataFrame([final_dict], schema)
        cat_rows.append(newRow)
    return cat_rows

###################### Spark Med DF ############################
def spk_med():
    id_d = val_json(text, 'id')
    housetrained_d = nested_json(text, 'house_trained', 'attributes')
    declawed_d = nested_json(text, 'declawed', 'attributes')
    spayed_neutered_d = nested_json(text, 'spayed_neutered', 'attributes')
    special_needs_d = nested_json(text, 'special_needs', 'attributes')
    shots_current_d = nested_json(text, 'shots_current', 'attributes')
    final_order = [id_d, declawed_d, housetrained_d, shots_current_d, special_needs_d, spayed_neutered_d]

    animals = []
    num_cats_in_file = len(final_order[0])
    for i in range(num_cats_in_file):
        animal = []
        animal.append(final_order[0][i])
        animal.append(final_order[1][i])
        animal.append(final_order[2][i])
        animal.append(final_order[3][i])
        animal.append(final_order[4][i])
        animal.append(final_order[5][i])
        animals.append(animal)

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("declawed", BooleanType(), True) \
                           , StructField("house_trained", BooleanType(), True) \
                           , StructField("shots_current", BooleanType(), True) \
                           , StructField("special_needs", BooleanType(), True) \
                           , StructField("spayed_neutered", BooleanType(), True)])

    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


###################### Spark description DF ############################
def spk_descr():
    id_d = val_json(text, 'id')
    descriptiion_d = val_json(text, 'description')
    final_order = [id_d, descriptiion_d]

    animals = []
    num_cats_in_file = len(final_order[0])
    for i in range(num_cats_in_file):
        animal = []
        animal.append(final_order[0][i])
        animal.append(final_order[1][i])
        animals.append(animal)

    spkschema = StructType([StructField("id", IntegerType(), True) \
                               , StructField("description", StringType(), True)])

    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


###################### Spark temperment DF ############################
def spk_temp():
    id_d = val_json(text, 'id')
    kids_d = nested_json(text, 'children', 'environment')
    dogs_d = nested_json(text, 'dogs', 'environment')
    cats_d = nested_json(text, 'cats', 'environment')
    final_order = [id_d, kids_d, dogs_d, cats_d]
    animals = []
    num_cats_in_file = len(final_order[0])
    for i in range(num_cats_in_file):
        animal = []
        animal.append(final_order[0][i])
        animal.append(final_order[1][i])
        animal.append(final_order[2][i])
        animal.append(final_order[3][i])
        animals.append(animal)

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("children", BooleanType(), True) \
                           , StructField("dogs", BooleanType(), True) \
                           , StructField("cats", BooleanType(), True)])
    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


###################### Spark status DF ############################
def spk_status():
    id_d = val_json(text, 'id')
    status_d = val_json(text, 'status')
    status_chg_d = val_json(text, 'status_changed_at')
    final_order = [id_d, status_d, status_chg_d]

    animals = []
    num_cats_in_file = len(final_order[0])
    for i in range(num_cats_in_file):
        animal = []
        animal.append(final_order[0][i])
        animal.append(final_order[1][i])
        animal.append(final_order[2][i])
        animals.append(animal)

    spkschema = StructType([StructField("id", IntegerType(), True) \
                           , StructField("status", StringType(), True) \
                            , StructField("status_changed_at", StringType(), True)])

    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


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


###################### SOMETHING IS WRONG WITH THIS!
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
                            , StructField("organization_id", StringType(), True) \
                            , StructField("name", StringType(), True) \
                            , StructField("size", StringType(), True) \
                            , StructField("age", StringType(), True) \
                            , StructField("gender", StringType(), True) \
                            , StructField("breeds_primary", StringType(), True) \
                            , StructField("breeds_secondary", StringType(), True)
                            , StructField("breeds_mixed", StringType(), True) \
                            , StructField("breeds_unknown", StringType(), True) \
                            , StructField("colors_primary", StringType(), True)
                            , StructField("colors_secondary", StringType(), True) \
                            , StructField("colors_mixed", StringType(), True) \
                            , StructField("coat", StringType(), True) \
                            , StructField("published_at", StringType(), True)])

    animals = []
    num_cats_in_file = len(final_order[0])
    for i in range(num_cats_in_file):
        animal = []
        animal.append(final_order[0][i])
        animal.append(final_order[1][i])
        animal.append(final_order[2][i])
        animal.append(final_order[3][i])
        animal.append(final_order[4][i])
        animal.append(final_order[5][i])
        animal.append(final_order[6][i])
        animal.append(final_order[7][i])
        animal.append(final_order[8][i])
        animal.append(final_order[9][i])
        animal.append(final_order[10][i])
        animal.append(final_order[11][i])
        animal.append(final_order[12][i])
        animal.append(final_order[13][i])
        animal.append(final_order[14][i])
        animals.append(animal)

    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


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

###################### Spark tag DF ############################
###################### Spark organization DF ############################



write_to_psql(spk_med(), 'animal_medical_info')
write_to_psql(spk_ani(), 'animal_info')
write_to_psql(spk_descr(), 'animal_description')
write_to_psql(spk_status(), 'animal_status')
write_to_psql(spk_temp(), 'animal_temperment')