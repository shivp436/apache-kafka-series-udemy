package shivp436.kafka.udemy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("I'm a Producer with Callback :)");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        /*
        Sticky Partitioner in Kafka:
        By default, Partitioner will batch records and send them into same partition, till the batch size reaches 16kb
        This is efficient in production environments

        If Batching is disabled, and no key is given, it will follow roundrobin partition
        If batching disabled and key given then hashing for partition

        With key: always hash partitioning
        No key + Batching: Sticky Partitioning
        No Key + No Batching: Round Robin
         */

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                // Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java-topic", "Hi from Java, with Callback");

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n"
                            );
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and clean the producer
        producer.close();
    }
}
