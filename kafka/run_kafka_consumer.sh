export KAFKA_HOME=/home/mani_dataops/youtubekafka/kafka_2.12-3.5.0
echo "Starting Consumer"
sudo ${KAFKA_HOME}/bin/kafka-console-consumer.sh   --topic trafficdata   --bootstrap-server localhost:9092   --from-beginning    --property "print.key=true"   --property "print.value=true"

