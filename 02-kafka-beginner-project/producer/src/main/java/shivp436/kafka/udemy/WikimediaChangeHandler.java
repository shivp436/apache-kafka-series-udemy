package shivp436.kafka.udemy;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // Connection opened - nothing to do
        log.info("Stream connection opened");
    }

    @Override
    public void onClosed() {
        // Connection closed
        log.info("Stream connection closed");
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageEvent.getData());
        log.info("Received event: {}", messageEvent.getData());

        // Send async with callback
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending message to Kafka", exception);
            } else {
                log.debug("Message sent successfully to partition {} with offset {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void onComment(String comment) {
        // Comments in the stream - usually ignored
        log.debug("Received comment: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in reading stream", t);
    }
}