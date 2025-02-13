from elastic.connection import *
from docx import Document, enum
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import RGBColor
import re

es_client = Elasticsearch('http://127.0.0.1:5602', request_timeout=100)


levels = ["قانون"]

index_name = "hooshyar_document_index"
res_query = {
    "query": {
        "bool": {
            "filter": [
                {
                    "term": {
                        "type": "قانون"
                    }
                },
                {
                    "term": {
                        "last_status": "معتبر"
                    }
                }
            ]
        }
    }
}

index_paragraph_size = es_client.count(body={
    "query": res_query["query"]
}, index=index_name)['count']

if index_paragraph_size > SEARCH_WINDOW_SIZE:
    es_client.indices.put_settings(index=index_name,
                                   body={"index": {
                                       "max_result_window": index_paragraph_size
                                   }})

response = es_client.search(
    index=index_name,
    _source_includes=["name", 'content'],
    body=res_query,
    size=index_paragraph_size,
)

if index_paragraph_size > SEARCH_WINDOW_SIZE:
    es_client.indices.put_settings(index=index_name,
                                   body={"index": {
                                       "max_result_window": SEARCH_WINDOW_SIZE
                                   }})

def preprocess_filename(filename):
    non_valid_chars = "/\\:*?\"<>|'؛،؟"
    cleaned_filename = ''.join(c if c not in non_valid_chars else ' ' for c in filename)
    return cleaned_filename[:100]


results = response['hits']['hits']
i = 0
for row in results:
    print(i, index_paragraph_size)
    i += 1
    _source = row['_source']
    document_name = preprocess_filename(_source['name'])
    content = _source['content']

    f = open(f"output2/results/{document_name[:100]}.txt", "w", encoding="utf-8")
    f.write(content)