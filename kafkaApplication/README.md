## If you do not have Kafka and Zookeeper downloaded
- go to https://kafka.apache.org/quickstart to get the quick start

## Start Kafka:
### First, Start Zookeeper
- `bin/zookeeper-server-start.sh config/zookeeper.properties`

### Then, Start Kafka Server
- `bin/kafka-server-start.sh config/server.properties`

## Endpoints:
### Create Topic
- `http://localhost:<YourPort>/publisher/newTopic/<topicName>`

### Post message to a topic
- `http://localhost:<YourPort>/publisher/post/<topicName>/<message>`

### Create new subscriber
- `http://localhost:<YourPort>/subscriber/newSubscriber/{subscriber_name}`

### Get all available topics
- `http://localhost:<YourPort>/subscriber/getTopics`

### Subscribe to a new topic (NOT DONE YET)
- `http://localhost:<YourPort>/subscriber/subscribe/{subscriber_name}/{topicName}`

### Poll unread messages from all subscription (NOT DONE YET)
- `http://localhost:<YourPort>/subscriber/{subscriber_name}/poll`
