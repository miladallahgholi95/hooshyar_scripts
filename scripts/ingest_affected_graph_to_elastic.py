import time
from elastic.connection import ESIndex, ES_CLIENT, SEARCH_WINDOW_SIZE, IndexObjectWithId
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING

NODES_COLOR_DICT = {
    "selected_doc": "#D4C400",
    "affected_by": "#CCE9F4",
    "affecting": "#89CF8F",
    "rules_related": "#CCE9F4",
    "regulation_related": "#89CF8F"
}

GRAPH_TYPE_FIELDS = {
    "affected_graph_data": ["affected_by", "affecting"],
    "related_graph_data": ["rules_related", "regulation_related"]
}


def get_docs_by_ids(index_name, doc_id_list, source_fields):
    field_query = {
        "terms": {
            "_id": doc_id_list
        }
    }

    response = ES_CLIENT.search(index=index_name,
                                request_timeout=120,
                                _source_includes=source_fields,
                                query=field_query,
                                size=SEARCH_WINDOW_SIZE,
                                )
    return response['hits']['hits']


def create_graph_data(doc, graph_type):
    selected_doc_id = doc["_id"]
    selected_doc_name = doc["_source"]["name"]

    nodes_data = []
    edges_data = []

    nodes_data.append({
        "id": selected_doc_id,
        "name": selected_doc_name,
        "node_type": "selected_doc",

    })
    graph_fields = GRAPH_TYPE_FIELDS[graph_type]

    for field in graph_fields:
        try:
            res_doc_id_list = doc["_source"][field]

            if res_doc_id_list:
                result_doc_list = get_docs_by_ids(DOCUMENT_MAPPING.NAME, res_doc_id_list, ["name"])

                for res_doc in result_doc_list:
                    nodes_data.append({
                        "id": res_doc["_id"],
                        "name": res_doc["_source"]["name"],
                        "node_type": field + "_doc",

                    })
                    if field == "affecting":
                        edges_data.append({
                            "id": res_doc["_id"] + "_" + selected_doc_id,
                            "source": res_doc["_id"],
                            "source_name": res_doc["_source"]["name"],
                            "source_node_type": field + "_doc",
                            "target": selected_doc_id,
                            "target_name": selected_doc_name,
                            "target_node_type": "selected_doc",
                            "weight": "1",
                            "edge_type": "from_" + field + "_doc" + "_to_selected_doc"
                        })
                    else:
                        edges_data.append({
                            "id": selected_doc_id + "_" + res_doc["_id"],
                            "source": selected_doc_id,
                            "source_name": selected_doc_name,
                            "source_node_type": "selected_doc",
                            "target": res_doc["_id"],
                            "target_name": res_doc["_source"]["name"],
                            "target_node_type": field + "_doc",
                            "weight": "1",
                            "edge_type": "from_selected_doc_to_" + field + "_doc"
                        })
        except:
            pass
    return nodes_data, edges_data


def extract_document_type_data(index_name, index_mapping, index_setting, source_id, patch_obj=None, ):
    if patch_obj is None:
        res_query = {
            "term": {
                "source_id":  source_id
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
    i = 0
    while True:
        index_name = index_name
        response = ESIndex.CLIENT.search(
            index=index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)

        if hits_count == 0:
            break

        i += SEARCH_WINDOW_SIZE
        print("====> ", i)

        last_id = hits_data[-1]["_id"]

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            for graph_type in GRAPH_TYPE_FIELDS:
                nodes_data, edges_data = create_graph_data(hit, graph_type)
                _source[graph_type] = {
                    "nodes_data": nodes_data,
                    "edges_data": edges_data,
                }
            result.append(_source)

        if result.__len__() % 10000 == 0:
            new_index = IndexObjectWithId(index_name, index_setting, index_mapping)
            new_index.bulk_insert_documents(result)
            result = []

    if result.__len__() > 0:
        new_index = IndexObjectWithId(index_name, index_setting, index_mapping)
        new_index.bulk_insert_documents(result)

def apply(patch_obj=None):
    start_time = time.time()

    # ------------------------------------------------------------------
    extract_document_type_data(DOCUMENT_MAPPING.NAME,
                               DOCUMENT_MAPPING.MAPPING,
                               DOCUMENT_SETTING.SETTING,
                               SOURCE_ID,
                               patch_obj)
    # ------------------------------------------------------------------

    end_time = time.time()

    print("graph_data added  in {} seconds".format(end_time - start_time))
