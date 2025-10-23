import logging
import argparse
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from opensearch_client import OpenSearchClient

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('opensearch-consumer')
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('opensearchpy').setLevel(logging.WARNING)
logging.getLogger('opensearch').setLevel(logging.WARNING)

class ConsumerClient:
    def __init__(self, topic):
        try:
            self.topic = topic
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers = 'localhost:9092',
                group_id = 'os_consumer_group_v1',
                enable_auto_commit = True,
                auto_offset_reset= 'earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            )
            logger.info('Kafka Consumer Created')
            self.client = OpenSearchClient()
            self.client.create_index(topic)
        except KafkaError as e:
            logger.error('Could not create KafkaConsumer: ', e)
            raise
        except Exception as e:
            logger.error('Could not create OS Client or Index: ', e)
            raise

    def print_messages(self):
        for msg in self.consumer:
            print(f' Message: {msg.partition} - {msg.offset}')

    def push_msg_to_opensearch(self):
        for msg in self.consumer:
            try:
                document = msg.value
                enhanced_document = {
                    'data': document,
                    'kafka_metadata': {
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'timestamp': msg.timestamp,
                        'key': msg.key.decode('utf-8') if msg.key else None
                    }
                }
                print(f' Message: {msg.partition} - {msg.offset}')
                try:
                    doc_id = msg.value['meta']['id']
                except Exception as e:
                    doc_id = f"{msg.partition}-{msg.offset}"
                self.client.index_document_exactly_once(
                    index = self.topic,
                    document = enhanced_document,
                    doc_id = doc_id
                )
            except Exception as e:
                logger.error(f"Couldn't parse document {msg.partition}-{msg.offset} : {e}")

    def close(self):
        self.client.close()
        self.consumer.close()
        logger.info('Consumer closed successfully')

def main():
    parser = argparse.ArgumentParser(
        description="Consume data from Kafka and index to OpenSearch"
    )
    parser.add_argument(
        "--source",
        default="binance",
        choices=["binance", "wikimedia"],
        help="Data source (determines Kafka topic)"
    )
    args = parser.parse_args()
    topics = {
        'binance': 'binance-trades',
        'wikimedia': 'wikimedia-recentchange'
    }
    topic = topics[args.source]

    try:
        consumer_client = ConsumerClient(topic)
        consumer_client.push_msg_to_opensearch()
        consumer_client.close()
    except KeyboardInterrupt:
        logger.info('Interrupted by Keyboard: Closing all')
        consumer_client.close()

if __name__ == '__main__':
    main()
