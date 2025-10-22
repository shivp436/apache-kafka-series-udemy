package shivp436.kafka.udemy;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "localhost:9092";
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        // Safe producer configs (optional but recommended)
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer configs
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Create Event Handler
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        // Build event source
        BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(
                eventHandler,
                new EventSource.Builder(URI.create(url))
        ).build();

        // Start the producer in another thread
        backgroundEventSource.start();

        System.out.println("Starting to read from Wikimedia stream...");

        // Produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);

        // Close resources
        System.out.println("Closing resources...");
        backgroundEventSource.close();
        producer.close();
    }
}
