echo "Starting Spark Kafka Producer"
sudo /etc/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 ./streaming-pipelines/kafka/trafficdata_Streamto_kafka.py
