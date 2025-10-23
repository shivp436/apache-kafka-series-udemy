"""
Kafka Consumer that streams data to OpenSearch
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from opensearch_client_v2 import OpenSearchClient
from config import (
    KafkaConfig,
    OpenSearchConfig,
    ConsumerConfig,
    get_stream_source,
    get_index_mapping
)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('opensearch-consumer')
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('opensearchpy').setLevel(logging.WARNING)


class KafkaToOpenSearchConsumer:
    """Kafka Consumer that indexes messages to OpenSearch"""
    
    def __init__(
        self,
        topic,
        opensearch_client,
        kafka_config=None,
        consumer_config=None
    ):
        """
        Initialize Kafka consumer with OpenSearch indexing
        
        Args:
            topic: Kafka topic to consume from
            opensearch_client: OpenSearchClient instance
            kafka_config: KafkaConfig instance
            consumer_config: ConsumerConfig instance
        """
        self.topic = topic
        self.os_client = opensearch_client
        
        # Use provided configs or defaults
        kafka_cfg = kafka_config or KafkaConfig()
        consumer_cfg = consumer_config or ConsumerConfig()
        
        self.batch_size = consumer_cfg.batch_size
        self.batch_timeout = consumer_cfg.batch_timeout
        self.use_bulk = consumer_cfg.use_bulk_indexing
        
        self.total_processed = 0
        self.total_indexed = 0
        self.total_failed = 0
        
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_cfg.bootstrap_servers,
                group_id=kafka_cfg.consumer_group_id,
                auto_offset_reset=kafka_cfg.auto_offset_reset,
                enable_auto_commit=kafka_cfg.enable_auto_commit,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                session_timeout_ms=kafka_cfg.session_timeout_ms,
                heartbeat_interval_ms=kafka_cfg.heartbeat_interval_ms,
                max_poll_records=kafka_cfg.max_poll_records,
                max_poll_interval_ms=kafka_cfg.max_poll_interval_ms
            )
            logger.info(f"✅ Kafka Consumer initialized for topic '{topic}'")
            
        except KafkaError as e:
            logger.error(f"❌ Failed to create Kafka Consumer: {e}")
            raise

    def _extract_id(self, message):
        """
        Extract a unique ID from the message for OpenSearch document ID
        
        Args:
            message: Kafka message value
            
        Returns:
            str: Document ID or None
        """
        # Try common ID fields
        id_fields = ['id', 'meta.id', 'trade_id', 'event_id', '_id']
        
        for field in id_fields:
            if '.' in field:
                # Nested field like 'meta.id'
                parts = field.split('.')
                value = message
                try:
                    for part in parts:
                        value = value.get(part)
                    if value:
                        return str(value)
                except (AttributeError, TypeError):
                    continue
            else:
                # Top-level field
                if field in message:
                    return str(message[field])
        
        # No ID found, let OpenSearch auto-generate
        return None

    def _prepare_document(self, message, kafka_metadata):
        """
        Prepare document for indexing with metadata
        
        Args:
            message: Kafka message value
            kafka_metadata: Kafka message metadata (partition, offset, timestamp)
            
        Returns:
            dict: Document ready for indexing
        """
        document = message.copy() if isinstance(message, dict) else {'data': message}
        
        # Add Kafka metadata for traceability
        document['_kafka_metadata'] = {
            'topic': kafka_metadata['topic'],
            'partition': kafka_metadata['partition'],
            'offset': kafka_metadata['offset'],
            'timestamp': kafka_metadata['timestamp'],
            'indexed_at': int(time.time() * 1000)  # Current timestamp in ms
        }
        
        return document

    def consume_and_index(self, max_messages=None):
        """
        Consume messages from Kafka and index to OpenSearch in batches
        
        Args:
            max_messages: Maximum number of messages to process (None = unlimited)
        """
        logger.info(f"🚀 Starting to consume from topic '{self.topic}' and index to OpenSearch")
        
        batch = []
        last_commit_time = time.time()
        messages_since_commit = 0
        
        try:
            for message in self.consumer:
                try:
                    # Extract message data
                    kafka_metadata = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp
                    }
                    
                    # Prepare document
                    document = self._prepare_document(message.value, kafka_metadata)
                    doc_id = self._extract_id(message.value)
                    
                    # Add to batch with ID
                    batch.append({
                        'document': document,
                        'id': doc_id
                    })
                    
                    self.total_processed += 1
                    messages_since_commit += 1
                    
                    # Check if batch is ready to index
                    should_flush = (
                        len(batch) >= self.batch_size or
                        time.time() - last_commit_time >= self.batch_timeout
                    )
                    
                    if should_flush:
                        self._flush_batch(batch)
                        batch = []
                        
                        # Commit offsets after successful indexing
                        self.consumer.commit()
                        messages_since_commit = 0
                        last_commit_time = time.time()
                        
                        logger.info(
                            f"📊 Progress: {self.total_processed} processed, "
                            f"{self.total_indexed} indexed, {self.total_failed} failed"
                        )
                    
                    # Check if we've reached max messages
                    if max_messages and self.total_processed >= max_messages:
                        logger.info(f"✅ Reached max messages limit: {max_messages}")
                        break
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ Invalid JSON in message: {e}")
                    self.total_failed += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    self.total_failed += 1
            
            # Flush any remaining messages
            if batch:
                self._flush_batch(batch)
                self.consumer.commit()
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Interrupted by user")
            if batch:
                logger.info("💾 Flushing remaining batch...")
                self._flush_batch(batch)
                self.consumer.commit()
        
        finally:
            self._print_summary()

    def _flush_batch(self, batch):
        """
        Index a batch of documents to OpenSearch
        
        Args:
            batch: List of documents with IDs
        """
        if not batch:
            return
        
        try:
            if self.use_bulk:
                # Use bulk indexing for better performance
                documents = [item['document'] for item in batch]
                success, failed = self.os_client.bulk_index(self.topic, documents)
            else:
                # Index documents individually
                success = 0
                failed = 0
                
                for item in batch:
                    result = self.os_client.index_document(
                        index_name=self.topic,
                        document=item['document'],
                        doc_id=item['id']
                    )
                    
                    if result:
                        success += 1
                    else:
                        failed += 1
            
            self.total_indexed += success
            self.total_failed += failed
            
            logger.debug(f"✅ Batch indexed: {success} succeeded, {failed} failed")
            
        except Exception as e:
            logger.error(f"❌ Failed to flush batch: {e}")
            self.total_failed += len(batch)

    def _print_summary(self):
        """Print final statistics"""
        logger.info("=" * 60)
        logger.info("📊 Final Statistics:")
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Total Indexed:   {self.total_indexed}")
        logger.info(f"  Total Failed:    {self.total_failed}")
        logger.info("=" * 60)

    def close(self):
        """Close consumer and commit offsets"""
        try:
            self.consumer.commit()
            self.consumer.close()
            logger.info("✅ Consumer closed successfully")
        except Exception as e:
            logger.warning(f"⚠️ Error closing consumer: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Consume data from Kafka and index to OpenSearch"
    )
    parser.add_argument(
        "--source",
        default="binance",
        choices=["binance", "wikimedia"],
        help="Data source (determines Kafka topic)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap server address"
    )
    parser.add_argument(
        "--opensearch-host",
        default="localhost",
        help="OpenSearch host address"
    )
    parser.add_argument(
        "--opensearch-port",
        type=int,
        default=9200,
        help="OpenSearch port"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages to batch before indexing"
    )
    parser.add_argument(
        "--batch-timeout",
        type=float,
        default=5.0,
        help="Max seconds to wait before flushing batch"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum messages to process (default: unlimited)"
    )
    parser.add_argument(
        "--use-bulk",
        action="store_true",
        default=True,
        help="Use bulk indexing for better performance"
    )
    parser.add_argument(
        "--consumer-group",
        default="opensearch-consumer-group",
        help="Kafka consumer group ID"
    )
    args = parser.parse_args()

    consumer = None
    os_client = None
    
    try:
        # Get stream source configuration
        stream_source = get_stream_source(args.source)
        topic = stream_source.topic
        
        logger.info(f"🎯 Source: {args.source}, Topic: {topic}")
        
        # Create configurations
        kafka_config = KafkaConfig(
            bootstrap_servers=args.bootstrap_servers,
            consumer_group_id=args.consumer_group
        )
        
        opensearch_config = OpenSearchConfig(
            host=args.opensearch_host,
            port=args.opensearch_port
        )
        
        consumer_config = ConsumerConfig(
            batch_size=args.batch_size,
            batch_timeout=args.batch_timeout,
            max_messages=args.max_messages,
            use_bulk_indexing=args.use_bulk
        )
        
        # Initialize OpenSearch client
        os_client = OpenSearchClient(
            host=opensearch_config.host,
            port=opensearch_config.port,
            timeout=opensearch_config.timeout
        )
        
        # Create index with proper mappings
        index_mapping = get_index_mapping(args.source)
        index_settings = {
            'index': {
                'number_of_shards': opensearch_config.number_of_shards,
                'number_of_replicas': opensearch_config.number_of_replicas,
                'refresh_interval': opensearch_config.refresh_interval
            }
        }
        
        os_client.create_index(
            index_name=topic,
            settings=index_settings,
            mappings=index_mapping if index_mapping else None
        )
        
        # Initialize consumer
        consumer = KafkaToOpenSearchConsumer(
            topic=topic,
            opensearch_client=os_client,
            kafka_config=kafka_config,
            consumer_config=consumer_config
        )
        
        # Start consuming and indexing
        consumer.consume_and_index(max_messages=consumer_config.max_messages)
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user. Stopping gracefully...")
        
    except Exception as e:
        logger.exception(f"⚠️ Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        if consumer:
            consumer.close()
        if os_client:
            os_client.close()


if __name__ == "__main__":
    main()
