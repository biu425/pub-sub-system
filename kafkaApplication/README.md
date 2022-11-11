## If you do not have Kafka and Zookeeper downloaded
- go to https://kafka.apache.org/quickstart to get the quick start

## First, Start Zookeeper
- `bin/zookeeper-server-start.sh config/zookeeper.properties`

## Then, Start Kafka Server
- `bin/kafka-server-start.sh config/server.properties`

## Create Kafka Topic
- `bin/kafka-topics.sh --create --bootstrap-server localhost:<YourPort1> --replication-factor 1 --partitions 1 --topic <YourTopic>`

## Consume Kafka Topic
- `bin/kafka-console-consumer.sh --topic <Your Topic> --from-beginning --bootstrap-server localhost:<YourPort2>`