import boto3
import json
import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, Row, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/postgresql-42.2.9.jar") \
    .appName('furevermatch') \
    .getOrCreate()

sc = spark.sparkContext

sqlContext = SQLContext(spark)


###################### Spark Med DF ############################
def spk_med(data):
    spk_df = spark.read.json(data)

    # Selects data from JSON to normalize
    filtered = spk_df.select(spk_df.animals.id.alias("id"), \
                          spk_df.animals.attributes.house_trained.alias("house_trained"), \
                          spk_df.animals.attributes.declawed.alias("declawed"), \
                          spk_df.animals.attributes.spayed_neutered.alias("spayed_neutered"), \
                             spk_df.animals.attributes.special_needs.alias("special_needs"), \
                             spk_df.animals.attributes.shots_current.alias("shots_current"))

    # Result is cleaned dataframe
    cleaned = filtered.withColumn("filtered", F.explode(F.arrays_zip("id",
                                                                     "house_trained",
                                                                     "declawed",
                                                                     "spayed_neutered",
                                                                     "special_needs",
                                                                     "shots_current"))) \
        .select("filtered.id",
                "filtered.house_trained",
                "filtered.declawed",
                "filtered.spayed_neutered",
                "filtered.special_needs",
                "filtered.shots_current")

    # Casting values to correct datatype
    cleaned_cast = cleaned.select(
        cleaned.id.cast(IntegerType()),
        cleaned.house_trained.cast(BooleanType()),
        cleaned.declawed.cast(BooleanType()),
        cleaned.spayed_neutered.cast(BooleanType()),
        cleaned.special_needs.cast(BooleanType()),
        cleaned.shots_current.cast(BooleanType())
    )

    return cleaned_cast



###################### Spark description DF ############################
def spk_descr(data):

    spk_df = spark.read.json(data)

    # Selects data from JSON to normalize
    filtered = spk_df.select(spk_df.animals.id.alias("id"), \
                          spk_df.animals.description.alias("description"))

    # Result is cleaned dataframe
    cleaned = filtered.withColumn("filtered", F.explode(F.arrays_zip("id", \
                                                                     "description"))) \
        .select("filtered.id", \
                "filtered.description")

    # Casting values to correct datatype
    cleaned_cast = cleaned.select(
        cleaned.id.cast(IntegerType()),
        cleaned.description.cast(StringType())
    )

    return cleaned_cast



###################### Spark temperment DF ############################
def spk_temp(data):
    spk_df = spark.read.json(data)

    # Selects data from JSON to normalize
    filtered = spk_df.select(spk_df.animals.id.alias("id"), \
                          spk_df.animals.environment.cats.alias("cats"), \
                          spk_df.animals.environment.dogs.alias("dogs"), \
                          spk_df.animals.environment.children.alias("children"))

    # Result is cleaned dataframe
    cleaned = filtered.withColumn("filtered", F.explode(F.arrays_zip("id", \
                                                                     "cats", \
                                                                     "dogs", \
                                                                     "children"))) \
        .select("filtered.id", \
                "filtered.cats", \
                "filtered.dogs", \
                "filtered.children")

    # Casting values to correct datatyoe
    cleaned_cast = cleaned.select(
        cleaned.id.cast(IntegerType()),
        cleaned.children.cast(BooleanType()),
        cleaned.dogs.cast(BooleanType()),
        cleaned.cats.cast(BooleanType()))

    return cleaned_cast


###################### Spark status DF ############################
def spk_status(data):
    spk_df = spark.read.json(data)

    # Selects data from JSON to normalize
    filtered = spk_df.select(spk_df.animals.id.alias("id"), \
                          spk_df.animals.status.alias("status"), \
                          spk_df.animals.status_changed_at.alias("status_changed_at"))


    # Result is cleaned dataframe
    cleaned = filtered.withColumn("filtered", F.explode(F.arrays_zip("id", \
                                                                     "status", \
                                                                     "status_changed_at"))) \
        .select("filtered.id", \
                "filtered.status", \
                "filtered.status_changed_at")


    # Casting values to correct datatype
    cleaned_cast = cleaned.select(
        cleaned.id.cast(IntegerType()),
        cleaned.status.cast(StringType()),
        cleaned.status_changed_at.cast(TimestampType())
    )

    return cleaned_cast


###################### Spark animal DF ############################
def spk_ani(data):
    spk_df = spark.read.json(data)

    # Selects data from JSON to normalize
    filtered = spk_df.select(spk_df.animals.id.alias("id"),
                             spk_df.animals.organization_id.alias("organization_id"),
                             spk_df.animals["name"].alias("name"),
                             spk_df.animals.size.alias("size"),
                             spk_df.animals.age.alias("age"),
                             spk_df.animals.gender.alias("gender"),
                             spk_df.animals.breeds.primary.alias("breeds_primary"),
                             spk_df.animals.breeds.secondary.alias("breeds_secondary"),
                             spk_df.animals.breeds.mixed.alias("breeds_mixed"),
                             spk_df.animals.breeds.unknown.alias("breeds_unknown"),
                             spk_df.animals.colors.primary.alias("colors_primary"),
                             spk_df.animals.colors.secondary.alias("colors_secondary"),
                             spk_df.animals.colors.tertiary.alias("colors_mixed"),
                             spk_df.animals.coat.alias("coat"),
                             spk_df.animals.published_at.alias("published_at")
                             )

    # Result is cleaned dataframe
    cleaned = filtered.withColumn("filtered", F.explode(F.arrays_zip("id",
                                                                     "organization_id",
                                                                     "name",
                                                                     "size",
                                                                     "age",
                                                                     "gender",
                                                                     "breeds_primary",
                                                                     "breeds_secondary",
                                                                     "breeds_mixed",
                                                                     "breeds_unknown",
                                                                     "colors_primary",
                                                                     "colors_secondary",
                                                                     "colors_mixed",
                                                                     "coat",
                                                                     "published_at"))) \
        .select("filtered.id",
                "filtered.organization_id",
                "filtered.name",
                "filtered.size",
                "filtered.age",
                "filtered.gender",
                "filtered.breeds_primary",
                "filtered.breeds_secondary",
                "filtered.breeds_mixed",
                "filtered.breeds_unknown",
                "filtered.colors_primary",
                "filtered.colors_secondary",
                "filtered.colors_mixed",
                "filtered.coat",
                "filtered.published_at"
                )


    # Casting values to correct datatype
    cleaned_cast = cleaned.select(
        cleaned.id.cast(IntegerType()),
        cleaned.organization_id.cast(StringType()),
        cleaned.name.cast(StringType()),
        cleaned.size.cast(StringType()),
        cleaned.age.cast(IntegerType()),
        cleaned.gender.cast(StringType()),
        cleaned.breeds_primary.cast(StringType()),
        cleaned.breeds_secondary.cast(StringType()),
        cleaned.breeds_mixed.cast(StringType()),
        cleaned.breeds_unknown.cast(StringType()),
        cleaned.colors_primary.cast(StringType()),
        cleaned.colors_secondary.cast(StringType()),
        cleaned.colors_mixed.cast(StringType()),
        cleaned.coat.cast(StringType()),
        cleaned.published_at.cast(TimestampType())

    )

    return cleaned_cast



############################################################################
#spark.stop()
