package shivp436.kafka.udemy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello from Producer");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java-topic", "Hi Again, from a Java Program");

        // send data
        producer.send(producerRecord);

        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and clean the producer
        producer.close();

        /*
         **send()** - "Send this whenever you get around to it" (lazy, async)
         **flush()** - "Stop whatever you're doing and send EVERYTHING right now!" (urgent, blocking)
         **close()** - "We're done here, send whatever's left and shut down" (flush + cleanup)
         */

    }
}
