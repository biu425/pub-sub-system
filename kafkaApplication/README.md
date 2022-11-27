## If you do not have Kafka and Zookeeper downloaded
- go to https://kafka.apache.org/quickstart to get the quick start

##Run docker image:
- `docker -compose  up`

## Endpoints:
### Create Topic
- `http://localhost:<YourPort>/publisher/newTopic/<topicName>`

### Post message to a topic
- `http://localhost:<YourPort>/publisher/post/<topicName>/<message>`

### Create new subscriber
- `http://localhost:<YourPort>/subscriber/newSubscriber/{subscriber_name}`

### Get all available topics
- `http://localhost:<YourPort>/subscriber/getTopics`

### Subscribe to a new topic 
- `http://localhost:<YourPort>/subscriber/subscribe/{subscriber_name}/{topicName}`

### Poll unread messages from all subscription 
- `http://localhost:<YourPort>/subscriber/{subscriber_name}/poll`

### Unsubscribe a topic
- `http://localhost:<YourPort>/subscriber/unsubscribe/{subscriber_name}/{topicName}`
