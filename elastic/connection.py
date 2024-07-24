from elasticsearch import Elasticsearch, helpers
from collections import deque
import time

ES_CLIENT = Elasticsearch('192.168.50.8:9200', request_timeout=100)  # 127.0.0.1:5602  192.168.50.8:9200 localhost:9200
INGEST_BULK_SIZE = 1000
BUCKET_SIZE = 1000
SEARCH_WINDOW_SIZE = 1000


class ESIndex:
    CLIENT = ES_CLIENT

    def __init__(self, name, settings, mappings):
        self.name = name
        self.settings = settings
        self.mappings = mappings

    def create(self):
        if self.CLIENT.indices.exists(index=self.name):
            print(f'{self.name} existed!')
        else:
            self.CLIENT.indices.create(index=self.name, mappings=self.mappings,
                                       settings=self.settings)
            print(f'{self.name} created')

    def generate_docs(self, documents):
        pass

    def bulk_insert_documents(self, documents, do_parallel=True):
        print('Insert_Documents started ...')
        start_t = time.time()
        generate_docs_method = self.generate_docs
        if do_parallel:
            deque(helpers.parallel_bulk(self.CLIENT, generate_docs_method(documents), thread_count=8, chunk_size=500,
                                        request_timeout=3000), maxlen=0)
        else:
            helpers.bulk(self.CLIENT, generate_docs_method(documents), chunk_size=500, request_timeout=120)
        self.CLIENT.indices.flush(index=self.name)
        self.CLIENT.indices.refresh(index=self.name)
        # Check the results:
        result = self.CLIENT.count(index=self.name)
        end_t = time.time()
        print(f"{result['count']} documents indexed.")
        print('Ingestion completed (' + str(end_t - start_t) + ').')

    def delete_index(self):
        if self.CLIENT.indices.exists(index=self.name):
            self.CLIENT.indices.delete(index=self.name, ignore=[400, 404])
            print(f'{self.name} deleted')
        else:
            print(f'{self.name} not found')

    def update_mapping(self):
        print('update mapping started ...')
        start_t = time.time()
        self.CLIENT.indices.put_mapping(index=[self.name], body=self.mappings)
        self.CLIENT.indices.flush([self.name])
        self.CLIENT.indices.refresh([self.name])
        # Check the results:
        result = self.CLIENT.count(index=self.name)
        end_t = time.time()
        print(f"{result['count']} documents indexed (time: {end_t - start_t}).")


class IndexObjectWithId(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, paragraphs):
        for paragraph in paragraphs:
            paragraph_id = paragraph["_id"]
            del paragraph["_id"]
            new_paragraph = {
                "_index": self.name,
                "_id": paragraph_id,
                "_source": paragraph,
            }
            yield new_paragraph


class IndexObjectWithoutId(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, paragraphs):
        for paragraph in paragraphs:
            new_paragraph = {
                "_index": self.name,
                "_source": paragraph,
            }
            yield new_paragraph
