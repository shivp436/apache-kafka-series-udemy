# learn about --group parameter
# see how partitions read are divided amongst multiple cli consumers

# create a topic with 5 partitions
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic third-topic --partitions 5

# start 1 consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application

# --group first-application: this will start consumer offset, so the nest consumer from this group will read where the first one stopped

# start producing to topic
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --producer-property acks=all \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
  --property "parse.key=true" \
  --property "key.separator=:"

# initially all messages will be read by 1 consumer (all partitions into 1 consumer)

# let's start another consumer in this group

# start 2 consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=' | ' \
  --group first-application

# Now: each consumer will be assigned partitions from the topic and they will only read from those partitions
# here: console-consumer 1: partition 3,4 && console-consumer 2: partition 0,1,2

# check whole group in console
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group first-application

: << 'COMMENT'
└─❯ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group first-application

Consumer group 'first-application' has no active members.

GROUP             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
first-application third-topic     0          3               3               0               -               -               -
first-application third-topic     2          3               3               0               -               -               -
first-application third-topic     1          3               3               0               -               -               -
first-application third-topic     4          4               4               0               -               -               -
first-application third-topic     3          3               3               0               -               -               -
COMMENT 

# in the above comment, can see that the offset is 3, 3, 3, 4, 3 for respective partitions 
# total messages sent were 16, with 3 in each, except 4th partition with 4 messages. 
# And each of the partitions were consumed and set offset till the latest message, with lag 0
# CURRENT-OFFSET: the last offset this consumer-group has committed/processed
# LOG-END-OFFSET: the last offset available in the topic partition (most recent message)
# LAG: LOG-END-OFFSET - CURRENT-OFFSET = unprocessed messages
