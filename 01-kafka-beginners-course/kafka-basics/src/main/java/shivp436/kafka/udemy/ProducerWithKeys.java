package shivp436.kafka.udemy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Producer with Keys :)");

        // Producer Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int j=0; j<3; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "java-topic";
                String key = "key-" + i;
                String value = "value-" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Key: " + key + " Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
            }
        }

        producer.flush();
        producer.close();
    }
}
