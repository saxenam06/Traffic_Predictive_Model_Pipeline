import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime


KAFKA_TOPIC = "trafficdata"
KAFKA_SERVER = "localhost:9092"

if __name__ == "__main__":
    
    # Initializing Spark session
    spark = SparkSession\
        .builder\
        .appName("TRAFFIC")\
        .getOrCreate()

    # Path to traffic data
    text_file_path = "./streaming-pipelines/data/"

    # Define Schema of the data required by spark for reading the stream 
    json_schema = StructType([
    StructField("date", StringType(), False),
    StructField("day_of_data", StringType(), False),
    StructField("day_of_week", StringType(), False),
    StructField("direction_of_travel", StringType(), False),
    StructField("direction_of_travel_name", StringType(), False),
    StructField("fips_state_code", StringType(), False),
    StructField("functional_classification", StringType(), False),
    StructField("functional_classification_name", StringType(), False),
    StructField("lane_of_travel", StringType(), False),
    StructField("month_of_data", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("restrictions", StringType(), False),
    StructField("station_id", StringType(), False),
    StructField("traffic_volume_counted_after_0000_to_0100", StringType(), False),
    StructField("traffic_volume_counted_after_0100_to_0200", StringType(), False),
    StructField("traffic_volume_counted_after_0200_to_0300", StringType(), False),
    StructField("traffic_volume_counted_after_0300_to_0400", StringType(), False),
    StructField("traffic_volume_counted_after_0400_to_0500", StringType(), False),
    StructField("traffic_volume_counted_after_0500_to_0600", StringType(), False),
    StructField("traffic_volume_counted_after_0600_to_0700", StringType(), False),
    StructField("traffic_volume_counted_after_0700_to_0800", StringType(), False),
    StructField("traffic_volume_counted_after_0800_to_0900", StringType(), False),
    StructField("traffic_volume_counted_after_0900_to_1000", StringType(), False),
    StructField("traffic_volume_counted_after_1000_to_1100", StringType(), False),
    StructField("traffic_volume_counted_after_1100_to_1200", StringType(), False),
    StructField("traffic_volume_counted_after_1200_to_1300", StringType(), False),
    StructField("traffic_volume_counted_after_1300_to_1400", StringType(), False),
    StructField("traffic_volume_counted_after_1400_to_1500", StringType(), False),
    StructField("traffic_volume_counted_after_1500_to_1600", StringType(), False),
    StructField("traffic_volume_counted_after_1600_to_1700", StringType(), False),
    StructField("traffic_volume_counted_after_1700_to_1800", StringType(), False),
    StructField("traffic_volume_counted_after_1800_to_1900", StringType(), False),
    StructField("traffic_volume_counted_after_1900_to_2000", StringType(), False),
    StructField("traffic_volume_counted_after_2000_to_2100", StringType(), False),
    StructField("traffic_volume_counted_after_2100_to_2200", StringType(), False),
    StructField("traffic_volume_counted_after_2200_to_2300", StringType(), False),
    StructField("traffic_volume_counted_after_2300_to_2400", StringType(), False),
    StructField("year_of_data", StringType(), False)
])

    # Read data using spark streaming in the defined schema
    dataStream = spark\
        .readStream\
        .schema(json_schema)\
	.option("delimiter", ",") \
	.option("header", "True")\
	.option("multiLine", "True")\
        .csv(text_file_path)
    dataStream = dataStream.withColumn("timestamp", current_timestamp())
   
    # Expression that converts data in key and value format required to avoid rejection from kafka 
    dataStream = dataStream\
		.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")

   # Writing dataframe to console in append mode
#    query = lines\
#		.writeStream\
#		.outputMode("append")\
#		.format("console")\
#		.start()\
#		.awaitTermination()
    # Setting parameters for Spark to write stream to Kafka

    kafkaStream = dataStream \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("topic", KAFKA_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start() \
        .awaitTermination()
