from pyspark import sql
import csv
import io
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, regexp_replace, udf
from pyspark.sql.functions import from_json
import json, math, datetime
import psycopg2
from operator import add
from pyspark.sql.functions import col, from_json, length
#from cassandra.cluster import Cluster
#from cassandra.query import BatchStatement
#from cassandra import ConsistencyLevel
import time
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LinearRegressionModel
import ast
import numpy as np

KAFKA_TOPIC = "trafficdata"
KAFKA_SERVER = "localhost:9092"

def extract_features(record, hour, category_len, mappings):
    cat_vec = np.zeros(category_len)
    i = 0
    step = 0
    for field in record[1:11]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    num_vec1 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[13 : hour + 13]])  # log transformed
    num_vec2 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[hour + 14 : 37]])  # skip current time
    return np.concatenate((cat_vec, num_vec1, num_vec2))

def extract_label(record, hour):
    return np.log(float(record[hour].strip("\"")) + 0.01)  # log transformation

def update(batch_df, models, mapping):
    connection = psycopg2.connect(host = 'localhost', database = 'postgres', user = 'postgres', password = 'my-password')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS public.realtimetraffic (sid text, location text, latitude double precision, longitude double precision,\
        direction text, lanes integer, roadtype text, highway text, current integer, historical double precision, predicted double precision, level text, PRIMARY KEY (sid));')

    print(f"connection_established")
    category_len = 156
    
    for record in batch_df.rdd.toLocalIterator():

        hour = time.localtime(time.time()).tm_hour
        p = LabeledPoint(extract_label(record, hour + 13), extract_features(record, hour, category_len, mapping))
        predicted = (np.exp(models[hour].predict(p.features)))

        direction = str(record[4]).strip("\"")
        fips_state_code = str(record[5]).strip("\"")
        station_id = str(record[12]).strip("\"")
        sid = fips_state_code + "_" + station_id + "_" + direction
        roadtype = str(record[7]).strip("\"")
        volume = int(str(record[hour + 13]).strip("\""))
        np.random.seed(0)
        simulatedVolume = int(max(np.random.normal(volume, volume * 0.5, 1)[0],0))
        cursor.execute('SELECT * FROM traffic WHERE id = %s;', (sid,))
        rows = cursor.fetchall()
        for row in rows:
            location = row[1]
            latitude = row[2]
            longitude = row[3]
            highway = row[6]
            lanes =  row[4]
            historical = row[7 + time.localtime(time.time()).tm_hour]
            level = "High" if (volume > historical * 2) else ("Low" if (volume < historical * 0.5) else "Medium")
            #cursor.execute('INSERT INTO realtimetraffic ("sid", "location", "latitude", "longitude", "direction", "lanes", "roadtype", "highway",\
            # "current", "historical", "predicted", "level", "geom") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
            # ST_SetSRID(ST_MakePoint(%s, %s), 4326)) ON CONFLICT (id) DO UPDATE SET current = %s;', (sid, location, latitude, longitude, direction,\
            #    lanes, roadtype, highway, simulatedVolume, historical, predicted, level, latitude, longitude, simulatedVolume,))
           
            # without converting to geom using postgis
            cursor.execute('INSERT INTO public.realtimetraffic ("sid", "location", "latitude", "longitude", "direction", "lanes", "roadtype", "highway", \
                "current", "historical", "predicted", "level") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ("sid") DO UPDATE SET current = %s;', \
                (sid, location, latitude, longitude, direction, lanes, roadtype, highway, simulatedVolume, historical, predicted, level, simulatedVolume))

    connection.commit()
    connection.close()

def process_batch(batch_df):
    print(batch_df.select(col("date")).collect()[1])
    print(batch_df.count())
    print(batch_df.rdd.collect()[1][0])
    for row in batch_df.rdd.toLocalIterator():
        print(row[0])

def main():

    # Initialize spark session 
    spark = SparkSession.builder.appName("TRAFFIC").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").getOrCreate()
    sc = spark.sparkContext

    # Load mapping
    mapping = sc.textFile("./streaming-pipelines/ML_model/mappings.txt").collect()[0]
    mapping = ast.literal_eval(str(mapping))
    
    # Load the JSON file with the custom object hook
    for i in range(len(mapping)):
         mapping[i] = {key.strip('"'): value for key, value in mapping[i].items()}

    # Load models
    models=[]
    for hour in range(0, 24):
        model = LinearRegressionModel.load(sc, "./streaming-pipelines/ML_model/linear_model_log_"+str(hour))
        models.append(model)
  
    category_len = 156

#    sqlContext = sql.SQLContext(sc)

    # Define schema
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

    # Select the json data in the streams
    value_df = kafkaStream\
		.selectExpr("CAST(value AS STRING) as value")
    dstream = value_df.select(from_json(col("value"),json_schema).alias("data")).select("data.*")
    
    # For display on console
#    query = dstream.writeStream \
#    .outputMode("update") \
#    .format("console") \
#    .option("truncate", False) \
#    .start() \
#    .awaitTermination()

    # Perform inference
    query = dstream.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda batch_df, epoch_id: update(batch_df, models, mapping)) \
    .start()\
    .awaitTermination()

    return


if __name__ == '__main__':

    main()
