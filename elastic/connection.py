from elasticsearch import Elasticsearch

ES_CLIENT = Elasticsearch('http://localhost:9200', request_timeout=100)  # 127.0.0.1:5602  192.168.50.8:9200
INGEST_BULK_SIZE = 1000
BUCKET_SIZE = 1000
SEARCH_WINDOW_SIZE = 5000
