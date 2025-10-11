# Produce to a topic from console
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first-topic

# Produce to a topic from a file
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first-topic <example_data.txt

# With Key, Value Parsing
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first-topic --producer-property acks=all --property "parse.key=true" --property "key.separator=:" <example_data.txt

# Producing to a non-existent topic
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first-topic-non-existent --producer-property acks=all --property "parse.key=true" --property "key.separator=:" <example_data.txt
# topic is created with default partitions and replication-factor provided in config/server.properties
