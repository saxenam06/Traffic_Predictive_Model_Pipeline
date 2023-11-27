# streaming-pipelines
A repository built on gcp for streaming pipelines using kafka, spark and more.

## Instructions to run: 
### Batch Pipeline for ML Training and writing to PostgreSQL
Make sure spark is setup on your machine. To setup Spark you can follow instructions here [Spark Setup](https://github.com/saxenam06/Ros_Python_Kafka_Spark#install-spark)
Also you must use your own username and password for PostgreSQL in pgadmin. 

1. Read data and Write Tranformed data to PostgreSQL.
   Read data, extract features as categories, train and save ML models. 
   ```
   sudo ./streaming-pipelines/spark/run_spark_topostgresql_and_mltrain.sh
   ```
### Streaming Pipeline for ML inference and writing results to PostgreSQL
1. 
