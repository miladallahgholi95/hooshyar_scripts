from googletrans import Translator

def translate_text(text, dest_lang='fa'):
    translator = Translator()
    translated = translator.translate(text, dest=dest_lang)
    return translated.text

from elasticsearch import Elasticsearch, helpers
ES_CLIENT = Elasticsearch('http://127.0.0.1:5602', request_timeout=100) # 127.0.0.1:5602  192.168.50.8:9200 localhost:9200

result = []
i = 1
last_id = "0"
while True:
    index_name = "hooshyar2_en_paragraph"
    res_query = {
        "match_all": {}
    }
    response = ES_CLIENT.search(
        index=index_name,
        query=res_query,
        size=5000,
        search_after=[last_id] if last_id != "0" else None,
        sort=[{"_id": {"order": "asc"}}]
    )
    hits_data = response['hits']['hits']
    hits_count = len(hits_data)


    if hits_count == 0:
        break

    last_id = hits_data[-1]['_id']

    for hit in hits_data:
        _id = hit['_id']
        content = hit['_source']["content"]