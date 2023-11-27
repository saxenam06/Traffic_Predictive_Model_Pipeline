## Instructions to run: 
### Batch Pipeline for ML Training and writing to PostgreSQL
Make sure spark is setup on your machine. To setup Spark you can follow instructions here [Spark Setup](https://github.com/saxenam06/Ros_Python_Kafka_Spark#install-spark)
To integrate with Postgresql follow some steps mentioned here [Postgresql Setup](https://github.com/saxenam06/Ros_Python_Kafka_Spark/blob/main/README.md#integration-with-postgresql)
Also you must use your own username and password for PostgreSQL in pgadmin.
1. In Instance 1, make sure your postgres docker image is started.
2. Read data and Write Tranformed data to PostgreSQL.

   Read data, extract features as categories, train and save ML models. 
   ```
   sudo ./streaming-pipelines/spark/run_spark_topostgresql_and_mltrain.sh
   ```
4. You can see now a table 'traffic' created in PostgreSQL and mappings.txt together ML models saved to ./streaming-pipelines/ML_model
   ![image](https://github.com/dugar-tarun/streaming-pipelines/assets/83720464/0826405f-884e-4ec2-83a9-02ad0cd01672)
 
### Streaming Pipeline for ML inference and writing results to PostgreSQL
Make sure spark, kafka and postgreql are setup on your machine. To setup Spark, Kafka and PostgreSQL you can follow instructions here [Spark Setup](https://github.com/saxenam06/Ros_Python_Kafka_Spark#install-spark), [Kafka Setup](https://github.com/saxenam06/Ros_Python_Kafka_Spark/blob/main/README.md#install-kafka) and [PostgresqlSetup](https://github.com/saxenam06/Ros_Python_Kafka_Spark/blob/main/README.md#integration-with-postgresql). 
Also you must use your own username and password for PostgreSQL in pgadmin.  
1. Open two instances of your VM.
2. In Instance 1, first start zookeeper and kafka servers. Create topic with name 'trafficdata'.
   ```
   sudo ./streaming-pipelines/kafka/run_start_kafka.sh
   ```
3. In Instance 1, read data and stream to kafka topic  'trafficdata'
   ```
   sudo ./streaming-pipelines/kafka/run_spark_kafka_producer.sh
   ```
4. In Instance 2, make sure your postgres docker image is started.
5. In Instance, subscribe to streaming data being published on topic 'trafficdata', load the ML models and categorigal mappings earlier created, perform ML predictions and write the predicted results in PostgreSQL.
   ```
   sudo ./streaming-pipelines/sparkstreaming/run_streaming.sh
   ```
6. You can see now a table 'realtimetraffic' created in PostgreSQL with your predicted results.
      ![image](https://github.com/dugar-tarun/streaming-pipelines/assets/83720464/49fc2dc8-519f-4e01-9ab5-6d870121b49d)
