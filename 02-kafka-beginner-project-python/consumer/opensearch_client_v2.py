"""
OpenSearch client wrapper for indexing and querying operations
"""
from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
import logging

logger = logging.getLogger('opensearch-client')


class OpenSearchClient:
    """Wrapper class for OpenSearch operations"""
    
    def __init__(self, host='localhost', port=9200, timeout=30):
        """
        Initialize OpenSearch client
        
        Args:
            host: OpenSearch host address
            port: OpenSearch port number
            timeout: Connection timeout in seconds
        """
        try:
            self.client = OpenSearch(
                hosts=[{'host': host, 'port': port}],
                http_compress=True,
                use_ssl=False,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                timeout=timeout,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Test connection
            if self.client.ping():
                logger.info(f"✅ Connected to OpenSearch at {host}:{port}")
            else:
                raise ConnectionError("Cannot ping OpenSearch")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenSearch client: {e}")
            raise

    def index_exists(self, index_name):
        """
        Check if an index exists
        
        Args:
            index_name: Name of the index
            
        Returns:
            bool: True if index exists, False otherwise
        """
        try:
            exists = self.client.indices.exists(index=index_name)
            if exists:
                logger.debug(f"ℹ️ Index '{index_name}' exists")
            return exists
        except OpenSearchException as e:
            logger.error(f"❌ Error checking index existence: {e}")
            return False

    def create_index(self, index_name, settings=None, mappings=None):
        """
        Create an index with optional settings and mappings
        
        Args:
            index_name: Name of the index to create
            settings: Index settings (shards, replicas, etc.)
            mappings: Index mappings (field types, analyzers, etc.)
            
        Returns:
            bool: True if created successfully, False otherwise
        """
        if self.index_exists(index_name):
            logger.info(f"ℹ️ Index '{index_name}' already exists")
            return True
        
        try:
            body = {}
            
            if settings:
                body['settings'] = settings
            else:
                # Default settings for single-node setup
                body['settings'] = {
                    'index': {
                        'number_of_shards': 1,
                        'number_of_replicas': 0,
                        'refresh_interval': '5s'
                    }
                }
            
            if mappings:
                body['mappings'] = mappings
            
            self.client.indices.create(index=index_name, body=body)
            logger.info(f"✅ Index '{index_name}' created successfully")
            return True
            
        except OpenSearchException as e:
            logger.error(f"❌ Failed to create index '{index_name}': {e}")
            return False

    def index_document(self, index_name, document, doc_id=None):
        """
        Index a single document
        
        Args:
            index_name: Name of the index
            document: Document to index (dict)
            doc_id: Optional document ID
            
        Returns:
            dict: Response from OpenSearch or None on failure
        """
        try:
            response = self.client.index(
                index=index_name,
                body=document,
                id=doc_id,
                refresh=False  # Don't refresh immediately for better performance
            )
            return response
            
        except OpenSearchException as e:
            logger.error(f"❌ Failed to index document: {e}")
            return None

    def bulk_index(self, index_name, documents):
        """
        Bulk index multiple documents for better performance
        
        Args:
            index_name: Name of the index
            documents: List of documents to index
            
        Returns:
            tuple: (success_count, failed_count)
        """
        from opensearchpy.helpers import bulk
        
        try:
            # Prepare bulk actions
            actions = [
                {
                    '_index': index_name,
                    '_source': doc
                }
                for doc in documents
            ]
            
            success, failed = bulk(
                self.client,
                actions,
                raise_on_error=False,
                refresh=False
            )
            
            if failed:
                logger.warning(f"⚠️ Bulk index: {success} succeeded, {len(failed)} failed")
            else:
                logger.debug(f"✅ Bulk indexed {success} documents")
                
            return success, len(failed) if failed else 0
            
        except Exception as e:
            logger.error(f"❌ Bulk index failed: {e}")
            return 0, len(documents)

    def search(self, index_name, query=None, size=10, from_=0):
        """
        Search documents in an index
        
        Args:
            index_name: Name of the index
            query: OpenSearch query DSL (default: match_all)
            size: Number of results to return
            from_: Starting offset for pagination
            
        Returns:
            dict: Search response
        """
        try:
            if query is None:
                query = {"match_all": {}}
            
            response = self.client.search(
                index=index_name,
                body={
                    "query": query,
                    "size": size,
                    "from": from_
                }
            )
            return response
            
        except OpenSearchException as e:
            logger.error(f"❌ Search failed: {e}")
            return None

    def get_documents(self, index_name, query=None, size=10):
        """
        Get documents from an index (simplified search)
        
        Args:
            index_name: Name of the index
            query: OpenSearch query DSL
            size: Number of documents to return
            
        Returns:
            list: List of document sources
        """
        response = self.search(index_name, query, size)
        if response:
            return [hit['_source'] for hit in response['hits']['hits']]
        return []

    def count_documents(self, index_name):
        """
        Get total document count in an index
        
        Args:
            index_name: Name of the index
            
        Returns:
            int: Document count or -1 on error
        """
        try:
            response = self.client.count(index=index_name)
            count = response['count']
            logger.debug(f"ℹ️ Index '{index_name}' has {count} documents")
            return count
        except OpenSearchException as e:
            logger.error(f"❌ Failed to count documents: {e}")
            return -1

    def delete_index(self, index_name):
        """
        Delete an index
        
        Args:
            index_name: Name of the index to delete
            
        Returns:
            bool: True if deleted successfully
        """
        try:
            if not self.index_exists(index_name):
                logger.warning(f"⚠️ Index '{index_name}' does not exist")
                return False
                
            self.client.indices.delete(index=index_name)
            logger.info(f"✅ Index '{index_name}' deleted")
            return True
            
        except OpenSearchException as e:
            logger.error(f"❌ Failed to delete index: {e}")
            return False

    def refresh_index(self, index_name):
        """
        Manually refresh an index to make recent changes visible
        
        Args:
            index_name: Name of the index
        """
        try:
            self.client.indices.refresh(index=index_name)
            logger.debug(f"✅ Index '{index_name}' refreshed")
        except OpenSearchException as e:
            logger.error(f"❌ Failed to refresh index: {e}")

    def close(self):
        """Close the OpenSearch client connection"""
        try:
            self.client.close()
            logger.info("✅ OpenSearch client closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing OpenSearch client: {e}")
