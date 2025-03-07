
sudo bin/zookeeper-server-start.sh config/zookeeper.properties

sudo bin/kafka-server-start.sh config/server.properties

sudo bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092 --zookeeper localhost:218

sudo bin/kafka-console-producer.sh --topic quickstart-events --broker-list localhost:9092

sudo ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

cd /usr/local/kafka

bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092

python3 client.py

python3 client_stream.py

python3 mongo_client.py
