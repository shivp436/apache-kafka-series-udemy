"""
Kafka Producer for streaming data from Binance or Wikimedia
"""
import sys
import logging
import argparse
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from stream_handler import StreamEventHandler

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('stream-producer')

# Reduce kafka library verbosity
logging.getLogger('kafka').setLevel(logging.WARNING)


class Producer:
    """Kafka Producer wrapper with optimal configurations"""
    
    def __init__(self, bootstrap_servers):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                
                # Serializers
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                
                # Reliability - idempotent producer
                acks='all',
                retries=2147483647,  # Max retries
                
                # Performance
                batch_size=32 * 1024,  # 32 KB
                linger_ms=20,
                compression_type='snappy',
                
                # Timeouts
                request_timeout_ms=30000,
                max_block_ms=60000,
                
                # Metadata
                client_id='stream-producer'
            )
            logger.info("✅ Kafka Producer initialized successfully")
        except KafkaError as e:
            logger.error(f"❌ Failed to create Kafka Producer: {e}")
            raise

    def send(self, topic, message):
        """Send message to Kafka topic"""
        try:
            future = self.producer.send(topic, message)
            # Don't block on every send, just check for errors periodically
            return future
        except Exception as e:
            logger.error(f"❌ Error sending message: {e}")
            raise

    def flush(self):
        """Flush pending messages"""
        try:
            self.producer.flush(timeout=10)
        except Exception as e:
            logger.warning(f"⚠️ Error flushing producer: {e}")

    def close(self):
        """Close producer gracefully"""
        try:
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)
            logger.info("✅ Producer closed successfully")
        except Exception as e:
            logger.warning(f"⚠️ Error closing producer: {e}")


def get_url_topic(source):
    """Return (url, topic) based on the data source"""
    sources = {
        'binance': (
            'wss://stream.binance.com:9443/ws/btcusdt@trade',
            'binance-trades'
        ),
        'wikimedia': (
            'https://stream.wikimedia.org/v2/stream/recentchange',
            'wikimedia-recentchange'
        )
    }
    return sources[source]


def main():
    parser = argparse.ArgumentParser(
        description="Stream data from Binance or Wikimedia to Kafka"
    )
    parser.add_argument(
        "--source",
        default="binance",
        choices=["binance", "wikimedia"],
        help="Data source (binance or wikimedia)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration in seconds to run the stream"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap server address"
    )
    args = parser.parse_args()

    handler = None
    try:
        url, topic = get_url_topic(args.source)
        producer = Producer(args.bootstrap_servers)
        handler = StreamEventHandler(producer, topic, url, args.source)
        handler.start(args.duration)
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user. Stopping gracefully...")
        if handler:
            handler.stop()
            
    except Exception as e:
        logger.exception(f"⚠️ Unexpected error: {e}")
        if handler:
            handler.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
