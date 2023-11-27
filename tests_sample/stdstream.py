from pyspark import sql
import csv
import io
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, regexp_replace
from pyspark.sql.functions import from_json
import json, math, datetime
import psycopg2
from operator import add
from pyspark.sql.functions import col, from_json, length
import time
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LinearRegressionModel
import ast

KAFKA_TOPIC = "newstudentdata"
KAFKA_SERVER = "localhost:9092"


def main():

    # Initialize spark session 
    spark = SparkSession.builder.appName("TRAFFIC").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").getOrCreate()
    sc = spark.sparkContext


    # Define schema
    json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
])
        #Read streaming data from kafka server
    kafkaStream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest")\
        .option("multiline", True)\
  	.option("delimeter",",") \
        .load()  

    value_df = kafkaStream\
		.selectExpr("CAST(value AS STRING) as value")
#    value_df = value_df.withColumn("cleaned_value", regexp_replace(col("value"), '\\\\"', '"'))
    transformed_df = value_df.select(from_json(col("value"),json_schema, {"mode" : "FAILFAST"}).alias("data")).select("data.*")

    query = transformed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

    return


if __name__ == '__main__':

    main()
