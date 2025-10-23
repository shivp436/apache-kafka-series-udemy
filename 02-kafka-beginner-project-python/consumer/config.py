"""
Configuration module for Kafka and OpenSearch settings
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    bootstrap_servers: str = '127.0.0.1:9092'
    consumer_group_id: str = 'opensearch-consumer-group'
    auto_offset_reset: str = 'earliest'
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    
    # Producer settings
    producer_acks: str = 'all'
    producer_retries: int = 2147483647
    enable_idempotence: bool = True
    compression_type: str = 'snappy'
    batch_size: int = 32 * 1024
    linger_ms: int = 20
    max_in_flight_requests: int = 5
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092'),
            consumer_group_id=os.getenv('KAFKA_CONSUMER_GROUP', 'opensearch-consumer-group')
        )


@dataclass
class OpenSearchConfig:
    """OpenSearch configuration settings"""
    host: str = 'localhost'
    port: int = 9200
    use_ssl: bool = False
    verify_certs: bool = False
    timeout: int = 30
    max_retries: int = 3
    retry_on_timeout: bool = True
    
    # Index settings
    number_of_shards: int = 1
    number_of_replicas: int = 0
    refresh_interval: str = '5s'
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv('OPENSEARCH_HOST', 'localhost'),
            port=int(os.getenv('OPENSEARCH_PORT', '9200')),
            use_ssl=os.getenv('OPENSEARCH_USE_SSL', 'false').lower() == 'true'
        )


@dataclass
class ConsumerConfig:
    """Consumer processing configuration"""
    batch_size: int = 100
    batch_timeout: float = 5.0
    max_messages: Optional[int] = None
    use_bulk_indexing: bool = True
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            batch_size=int(os.getenv('CONSUMER_BATCH_SIZE', '100')),
            batch_timeout=float(os.getenv('CONSUMER_BATCH_TIMEOUT', '5.0')),
            use_bulk_indexing=os.getenv('USE_BULK_INDEXING', 'true').lower() == 'true'
        )


@dataclass
class StreamSource:
    """Stream source configuration"""
    name: str
    url: str
    topic: str
    
    
# Predefined stream sources
STREAM_SOURCES = {
    'binance': StreamSource(
        name='binance',
        url='wss://stream.binance.com:9443/ws/btcusdt@trade',
        topic='binance-trades'
    ),
    'wikimedia': StreamSource(
        name='wikimedia',
        url='https://stream.wikimedia.org/v2/stream/recentchange',
        topic='wikimedia-recentchange'
    )
}


def get_stream_source(source_name: str) -> StreamSource:
    """
    Get stream source configuration
    
    Args:
        source_name: Name of the source (binance, wikimedia)
        
    Returns:
        StreamSource: Configuration for the source
        
    Raises:
        ValueError: If source name is not found
    """
    if source_name not in STREAM_SOURCES:
        raise ValueError(f"Unknown source: {source_name}. Available: {list(STREAM_SOURCES.keys())}")
    return STREAM_SOURCES[source_name]


# Index mappings for different sources
BINANCE_MAPPING = {
    'properties': {
        'e': {'type': 'keyword'},  # Event type
        's': {'type': 'keyword'},  # Symbol
        't': {'type': 'long'},     # Trade ID
        'p': {'type': 'float'},    # Price
        'q': {'type': 'float'},    # Quantity
        'T': {'type': 'date'},     # Trade time
        'm': {'type': 'boolean'},  # Is buyer maker
        '_kafka_metadata': {
            'properties': {
                'topic': {'type': 'keyword'},
                'partition': {'type': 'integer'},
                'offset': {'type': 'long'},
                'timestamp': {'type': 'date'},
                'indexed_at': {'type': 'date'}
            }
        }
    }
}

WIKIMEDIA_MAPPING = {
    'properties': {
        'id': {'type': 'long'},
        'type': {'type': 'keyword'},
        'namespace': {'type': 'integer'},
        'title': {'type': 'text'},
        'comment': {'type': 'text'},
        'timestamp': {'type': 'date'},
        'user': {'type': 'keyword'},
        'bot': {'type': 'boolean'},
        'server_name': {'type': 'keyword'},
        '_kafka_metadata': {
            'properties': {
                'topic': {'type': 'keyword'},
                'partition': {'type': 'integer'},
                'offset': {'type': 'long'},
                'timestamp': {'type': 'date'},
                'indexed_at': {'type': 'date'}
            }
        }
    }
}

SOURCE_MAPPINGS = {
    'binance': BINANCE_MAPPING,
    'wikimedia': WIKIMEDIA_MAPPING
}


def get_index_mapping(source_name: str) -> dict:
    """Get index mapping for a source"""
    return SOURCE_MAPPINGS.get(source_name, {})
