from kafka import KafkaProducer
import os
import json
from datetime import datetime
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *


def handler(message):
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
    producer.send('trafficdata', key=json.dumps(str(datetime.now())).encode('utf-8'), value=json.dumps(message).encode('utf-8'))
    producer.flush()


def main():

    spark = SparkSession.builder.appName("TRAFFIC").config("spark.executor.cores", "6").config("spark.executor.memory", "6g").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    raw_data = sc.textFile("./streaming-pipelines/data/")
    header = raw_data.first()
    records = raw_data.filter(lambda line: line != header).map(lambda x: x.split(","))
    records.cache()

    records.foreach(lambda line: handler(line))

    return

if __name__ == '__main__':
    main()
