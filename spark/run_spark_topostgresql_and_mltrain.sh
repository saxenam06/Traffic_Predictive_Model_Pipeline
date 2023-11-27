echo "Starting Spark to Postgresq"
sudo /etc/spark/bin/spark-submit --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 --packages org.postgresql:postgresql:42.6.0 ./streaming-pipelines/spark/spark.py
echo "Starting Spark ML Training"
sudo /etc/spark/bin/spark-submit --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 ./streaming-pipelines/spark/sparkml.py

