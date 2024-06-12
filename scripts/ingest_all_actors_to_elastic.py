import math
from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE
from elastic.MAPPINGS import DOCUMENT_MAPPING, ACTORS_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING
from input_configs import *

# es configs
CURRENT_VECTOR_ID = 1


def get_all_actors_forms():
    res_query = {
        "match_all": {}
    }
    response = ESIndex.CLIENT.search(
        index=ACTORS_MAPPING.NAME,
        query=res_query,
        size=5000
    )
    actor_list = response['hits']['hits']
    actors_form_dict = {}
    actors_id_dict = {}
    for row in actor_list:
        actors_form_dict[row["_source"]["name"]] = row["_source"]["forms"]
        actors_id_dict[row["_source"]["name"]] = row["_id"]
    return actors_form_dict, actors_id_dict


def extract_document_actors_data(source_id, actors_forms_dictionary, patch_obj):
    res_query = {
        "bool":
            {
                "filter": [],
                "should": [],
                "minimum_should_match": 1
            }
    }
    if patch_obj is None:
        res_query["bool"]["filter"].append({
            "term": {
                "source_id": source_id
            }
        })
    else:
        res_query["bool"]["filter"].append({
            "terms": {
                "_id": patch_obj
            }
        })
    document_actor_dict = {}
    cntr = 1
    for actor, actor_forms in actors_forms_dictionary.items():
        print(cntr, "------------------------")
        cntr += 1
        res_query["bool"]["should"] = []
        for actor_form in actor_forms:
            res_query["bool"]["should"].append({"match_phrase": {"content": actor_form}})

        last_id = "0"
        while True:
            index_name = DOCUMENT_MAPPING.NAME
            response = ESIndex.CLIENT.search(
                _source=False,
                index=index_name,
                query=res_query,
                size=SEARCH_WINDOW_SIZE,
                search_after=[last_id] if last_id != "0" else None,
                sort=[{"_id": {"order": "asc"}}],
                highlight={
                    "order": "score",
                    "fields": {
                        "content": {
                            "type": "fvh",
                            "pre_tags": [
                                "<em>"
                            ],
                            "post_tags": [
                                "</em>"
                            ],
                            "number_of_fragments": 0,
                            "phrase_limit": 100000
                        }
                    }
                }
            )
            hits_data = response['hits']['hits']
            hits_count = len(hits_data)
            if hits_count == 0:
                break
            for hit in hits_data:
                document_id = hit["_id"]
                last_id = document_id
                highlight_actors_forms = hit["highlight"]["content"][0]
                if document_id not in document_actor_dict:
                    document_actor_dict[document_id] = {}
                document_actor_dict[document_id][actor] = highlight_actors_forms.count("<em>")
    return document_actor_dict


def get_ingest_data(source_id, patch_obj, document_actors_data, actors_id_dict):

    if patch_obj is None:
        res_query = {
            "term": {
                "source_id": source_id
            }
        }
    else:
        res_query = {
            "terms": {
                "_id": patch_obj
            }
        }

    last_id = "0"
    result = []
    while True:
        index_name = DOCUMENT_MAPPING.NAME
        response = ESIndex.CLIENT.search(
            index=index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}],
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)
        if hits_count == 0:
            break
        for hit in hits_data:
            document_id = hit["_id"]
            last_id = document_id
            _source = hit["_source"]
            _source["_id"] = document_id
            _source["actors"] = []
            if document_id in document_actors_data:
                for actor, count in document_actors_data[document_id].items():
                    actor_id = actors_id_dict[actor]
                    _source["actors"].append({"id": actor_id, "name": actor, "frequency": count})
            result.append(_source)
    return result


class DocumentIndex(ESIndex):
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

def apply(patch_obj=None):

    actors_forms_dictionary, actors_id_dict = get_all_actors_forms()
    document_actors_data = extract_document_actors_data(SOURCE_ID, actors_forms_dictionary, patch_obj)
    document_ingest_data = get_ingest_data(SOURCE_ID, patch_obj, document_actors_data, actors_id_dict)
    index_name = DOCUMENT_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = DOCUMENT_MAPPING.MAPPING
    data = document_ingest_data
    batch_size = 10000
    slice_count = math.ceil(data.__len__() / batch_size)
    for i in range(slice_count):
        print(f"[ [{i}] / [{slice_count}]")
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, data.__len__())
        sub_list = data[start_idx:end_idx]
        new_index = DocumentIndex(index_name, setting, mapping)
        new_index.create()
        new_index.bulk_insert_documents(sub_list)

    print("========================================\n\n", "Done\n\n\n========================================\n\n\n")

