import boto3
import json
from pandas import concat
import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, Row, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, split, expr, substring, regexp_replace, trim, round
from pyspark.sql import functions as F
from write_to_postgres import write_to_psql


spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
    .appName('furevermatch') \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(spark)


df = spark.read.csv("Lost__found__adoptable_pets.csv", header=True)


list_o_cols = ["Animal_ID",
                 "Data_Source",
                 "Record_Type",
                 "Current_Location",
                 'Animal_Name',
                 'animal_type',
                 'Age',
                 'Animal_Gender',
                 'Animal_Breed',
                 'Animal_Color',
                 'Date',
                 'Date_Type',
                 'Zip',
                 'Memo',
                 'Temperament']

filtered_df = df.drop("impound_no",
             "Link",
             "Obfuscated_Address",
             "City",
             "State",
             "jurisdiction",
             "obfuscated_latitude",
             "obfuscated_longitude",
             "Image",
             "image_alt_text",
             "Data_Source",
             "Current_Location",
                      "Date_Type",
                      "Date"
             )

adoptable = filtered_df.filter(filtered_df["Record_Type"] == 'ADOPTABLE')
adoptable_cats = adoptable.filter(adoptable["animal_type"] == "Cat")


adoptable_cats = adoptable_cats.drop("Record_Type", "animal_type")

renamed = adoptable_cats.select(adoptable_cats["Animal_ID"].alias("local_id"),
                            adoptable_cats["Animal_Name"].alias("name"),
                            adoptable_cats["Age"].alias("age"),
                            adoptable_cats["Animal_Gender"].alias("gender_combo"),
                            adoptable_cats["Animal_Breed"].alias("breed"),
                            adoptable_cats["Animal_Color"].alias("color"),
                            adoptable_cats["Zip"].alias("zip"),
                            adoptable_cats["Memo"].alias("memo"),
                            adoptable_cats["Temperament"].alias("temperament")

                             )

gender_list = renamed.withColumn("gender_combo", split("gender_combo", "\s+"))
cleaned = gender_list.select(["*"] +[expr('gender_combo[' + str(x) + ']') for x in range(0, 2)])
cleaned = cleaned.selectExpr("local_id",
                             "name",
                             "age",
                             "gender_combo[0] as fixed_status",
                             "gender_combo[1] as gender",
                             "breed",
                             "color",
                             "zip",
                             "memo",
                             "temperament"
                             )



age_list = cleaned.withColumn("age", split("age", "[S]\s+"))
cleaned = age_list.select(["*"] +[expr('age[' + str(x) + ']') for x in range(0, 2)])



cleaned = cleaned.withColumn('age_y', when(col('age[0]').like("%MONTH%"), regexp_replace(col('age[0]'), "[1-9] [MONTHS]", "").substr(1, 2)).otherwise(cleaned["age[0]"].substr(1, 2)))
cleaned = cleaned.withColumn('age_y', when(col('age_y').like("%ON%"), regexp_replace(col('age_y'), "[A-Z]", None)).otherwise(cleaned.age_y))
cleaned = cleaned.withColumn('age_y', trim(col('age_y')))
cleaned = cleaned.withColumn('age_m', when(col('age[0]').like("%MONTH%"), cleaned["age[0]"].substr(-8, 1)).otherwise(cleaned["age[1]"].substr(1, 1)))



cleaned = cleaned.withColumn("age_y",when(cleaned.age_y.isNull(), 0).otherwise(cleaned.age_y))
cleaned = cleaned.withColumn("age_m",when(cleaned.age_m.isNull(), 0).otherwise(cleaned.age_m))

cast = cleaned.withColumn("age_y", cleaned["age_y"].cast(IntegerType()))
cast = cleaned.withColumn("age_m", cleaned["age_m"].cast(IntegerType()))

cast = cast.withColumn("age_m", round((col("age_m") / 12), 2))
aged = cast.withColumn("age", col("age_y") + col("age_m"))

aged = aged.withColumn("age_group", when(col("age") < .5, "Baby").when(col("age") < 1, "Young").when(col("age") < 6, "Adult").when(col("age") > 6, "Senior").otherwise("Unknown"))


finished_cleaning = aged.drop("age[0]",
             "age[1]",
             "age_y",
             "age_m")


write_to_psql(finished_cleaning, 'local_info')
print("Data inserted to Postgres: Local")