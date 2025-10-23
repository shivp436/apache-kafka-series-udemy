from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
import logging

logger = logging.getLogger('opensearch-client')

class OpenSearchClient:
    def __init__(self):
        host = 'localhost'
        port = 9200
        timeout = 30

        self.client = OpenSearch(
            hosts = [{'host': host, 'port': port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=timeout,
            max_retries=3,
            retry_on_timeout=True
        )
        if self.client.ping():
            logger.info(f"✅ Connected to OpenSearch at {host}:{port}")
        else:
            raise ConnectionError("Cannot ping OpenSearch")

    def index_exists(self, index):
        if self.client.indices.exists(index=index):
            logger.info(f"Index {index} already exists")
            return True
        return False

    def create_index(self, index):
        if not self.index_exists(index):
            self.client.indices.create(index=index)
            logger.info(f"Index {index} created")

    def index_document(self, index, document, doc_id=None):
        try:
            response = self.client.index(
                index=index,
                body=document,
                id=doc_id
            )
            logger.info(f'Document Indexed: {doc_id} - {response}')
            return response
        except OpenSearchException as e:
            logger.warning(f'Document not indexed {e}')
            return None
    
    def index_document_exactly_once(self, index, document, doc_id=None):
        try:
            if not self.client.exists(index=index, id=doc_id):
                response = self.client.index(
                    index=index,
                    body=document,
                    id=doc_id
                )
                logger.info(f'Document Indexed: {doc_id}')
                return response
            else:
                logger.info(f'Document already exists, skipping {doc_id}')
                return None
        except OpenSearchException as e:
            logger.warning(f'Document not indexed {doc_id} : {e}')
            return None

    def count_documents(self, index):
        if self.index_exists(index):
            response = self.client.count(index=index)
            count = response['count']
            logger.info(f"ℹ️ Index '{index_name}' has {count} documents")
            return count
        else:
            logger.info(f'Index {index} does not exist')
            return None
    
    def delete_index(self, index):
        if self.index_exists(index):
            self.client.indices.delete(index=index)
            logger.info(f'Index {index} deleted')
        else:
            logger.info(f'Index {index} does not exist')

    def close(self):
        if self.client.ping():
            self.client.close()
            logger.info('OpenSearch Client Closed')
        else:
            logger.info('OpenSearch Client Does not Exist')
