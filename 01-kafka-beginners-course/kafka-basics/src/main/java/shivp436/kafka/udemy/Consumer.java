package shivp436.kafka.udemy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer");

        String groupId = "demo-java-group";
        String topic = "java-topic";

        // Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest"); // read from beginning in case no offset is committed already
        // if offset is commited already, it will read from the last committed offset

        // Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true) {
            log.info("Polling"); // Polling is listening
            // will keep consuming for ever

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + " Value: " + record.value());
                log.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }

        // close

    }
}
