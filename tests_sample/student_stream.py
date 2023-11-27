import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime


KAFKA_TOPIC = "newstudentdata"
KAFKA_SERVER = "localhost:9092"

if __name__ == "__main__":
    
    # Initializing Spark session
    spark = SparkSession\
        .builder\
        .appName("TRAFFIC")\
        .getOrCreate()

    # Path to traffic data
    text_file_path = "./streaming-pipelines/data_sample/"
    # Define Schema of the data required by spark for reading the stream 
    json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)])
    # Read data using spark streaming in the defined schema
    lines = spark\
        .readStream\
        .schema(json_schema)\
	.option("delimiter", ",") \
	.option("header", "False")\
	.option("multiLine", "True")\
        .csv(text_file_path)
    lines = lines.withColumn("timestamp", current_timestamp())
   
    # Expression that converts data in key and value format required to avoid rejection from kafka 
    lines = lines\
		.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")

   # Writing dataframe to console in append mode
#    query = lines\
#		.writeStream\
#		.outputMode("append")\
#		.format("console")\
#		.start()\
#		.awaitTermination()
    # Setting parameters for Spark to write stream to Kafka
   
    kafka_sink_query = lines \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("topic", KAFKA_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start() \
        .awaitTermination()
