# consume from first-topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic
# this will only listen messages sent after consumer is started

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first-topic
# > hello world
# > hiii

# to listen from beginning of the messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --from-beginning

## Topics with multiple partitions
# messages are ordered only within a partition
# There can be no guarantee of order in messages across partitions

# Create a topic with partitions first
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic second-topic --partitions 3

# producing to the topic
# with keys: hashing (by default)
# without keys: roundRobin (by default)
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic second-topic \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner

# consuming from the topic with advanced properties
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic second-topic \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.headers=true \
  --property key.separator=" | "

# ### Output
# CreateTime:1760193227651 | Partition:2 | Offset:0 | NO_HEADERS | null | hello 0
# CreateTime:1760193235362 | Partition:2 | Offset:1 | NO_HEADERS | null | hello 3
# CreateTime:1760193230914 | Partition:1 | Offset:0 | NO_HEADERS | null | hello 1
# CreateTime:1760193238458 | Partition:1 | Offset:1 | NO_HEADERS | null | hello 4
# CreateTime:1760193232603 | Partition:0 | Offset:0 | NO_HEADERS | null | hello 2
# CreateTime:1760193239926 | Partition:0 | Offset:1 | NO_HEADERS | null | hello 5
# ###
