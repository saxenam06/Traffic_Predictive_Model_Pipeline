export KAFKA_HOME=/home/mani_dataops/youtubekafka/kafka_2.12-3.5.0
sudo ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties > ${KAFKA_HOME}/logs/zookeeper.log 2>&1 &
sleep 20
echo "Started Zookeeper"
sudo ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties > ${KAFKA_HOME}/logs/broker1.log 2>&1 &
sleep 20
echo "Started Kafka Server"
sudo ${KAFKA_HOME}/bin/connect-distributed.sh ${KAFKA_HOME}/config/connect-distributed.properties > ${KAFKA_HOME}/logs/connect.log 2>&1 &
sleep 20
echo "Started Distributed server"
sudo ${KAFKA_HOME}/bin/kafka-topics.sh   --create   --topic trafficdata   --bootstrap-server localhost:9092   --partitions 4   --replication-factor 3   --config "cleanup.policy=compact"    --config "retention.ms=604800000"   --config "segment.bytes=1073741824"
sleep 20
echo "Created Topic"
