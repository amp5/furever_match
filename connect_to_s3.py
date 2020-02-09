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
        print(pair)
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
        print(pair)
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
def spk_med(data):
    id_d = val_json(data, 'id')
    housetrained_d = nested_json(data, 'house_trained', 'attributes')
    declawed_d = nested_json(data, 'declawed', 'attributes')
    spayed_neutered_d = nested_json(data, 'spayed_neutered', 'attributes')
    special_needs_d = nested_json(data, 'special_needs', 'attributes')
    shots_current_d = nested_json(data, 'shots_current', 'attributes')
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
def spk_descr(data):
    id_d = val_json(data, 'id')
    descriptiion_d = val_json(data, 'description')
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
def spk_temp(data):
    id_d = val_json(data, 'id')
    kids_d = nested_json(data, 'children', 'environment')
    dogs_d = nested_json(data, 'dogs', 'environment')
    cats_d = nested_json(data, 'cats', 'environment')
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
def spk_status(data):
    id_d = val_json(data, 'id')
    status_d = val_json(data, 'status')
    status_chg_d = val_json(data, 'status_changed_at')
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

###################### Spark animal DF ############################
def spk_ani(data):
    id_d = val_json(data, 'id')
    org_d = val_json(data, 'organization_id')
    name_d = val_json(data, 'name')
    size_d = val_json(data, 'size')
    age_d = val_json(data, 'age')
    gender_d = val_json(data, 'gender')
    breed_pri_d = nested_json(data, 'primary', 'breeds')
    breed_sec_d = nested_json(data, 'secondary', 'breeds')
    breed_mix_d = nested_json(data, 'mixed', 'breeds')
    breed_unkn_d = nested_json(data, 'unknown', 'breeds')
    color_pri_d = nested_json(data, 'primary', 'colors')
    color_sec_d = nested_json(data, 'secondary', 'colors')
    color_mix_d = nested_json(data, 'tertiary', 'colors')
    coat_d = val_json(data, 'coat')
    date_added = val_json(data, 'published_at')

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
    print("do I have final ani schema?")

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
    print("have my ani animals been appended?")

    spk_df = spark.createDataFrame(sc.emptyRDD(), spkschema)
    unioned_df = unionAll(creating_new_rows(animals, spkschema))
    return unioned_df


def write_to_psql(df, table):
    """ input is spark dataframe and the postgres table df needs to be written to """

    # modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'
    print("do I stop here?")
    mode = "overwrite"
    url = "jdbc:postgresql://database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com:5432/postgres"
    properties = {"user": "postgres",
                  "password": "fureverdb",
                  "driver": "org.postgresql.Driver"}
    print("or at least make it to before writing")
    df.write.jdbc(url=url,
                  table=table,
                  mode=mode,
                  properties=properties)



def run_query(query):
    df_select = spark.sql(query)
