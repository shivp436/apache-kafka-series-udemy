# List all topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create new topic
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic first-topic

# With Properties
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic first-topic-adv --partitions 3 --replication-factor 1
# replication-factor cannot be more than number of brokers: 1 for localhost

# Describe topic
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic first-topic

# Delete topic
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic first-topic
