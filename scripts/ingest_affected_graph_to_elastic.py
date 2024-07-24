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


def add_graphs_data(doc_list):
    docs_ctr = 0

    all_docs_count = len(doc_list)

    for doc in doc_list:
        docs_ctr += 1
        print(f"{round(docs_ctr / all_docs_count, 2) * 100}% ...")

        for graph_type in GRAPH_TYPE_FIELDS:
            nodes_data, edges_data = create_graph_data(doc, graph_type)

            doc["_source"][graph_type] = {
                "nodes_data": nodes_data,
                "edges_data": edges_data,
            }

    return doc_list


def get_document_list(country, patch_obj):
    if patch_obj is None:
        res_query = {
            "term":
                {
                    "source_id": country
                }
        }
    else:
        res_query = {
            "terms":
                {
                    "_id": patch_obj
                }
        }

    # get all document size
    index_document_size = ES_CLIENT.count(body={
        "query": res_query
    }, index=DOCUMENT_MAPPING.NAME)['count']

    # index_document_size = 100

    # change result window size
    if index_document_size > SEARCH_WINDOW_SIZE:
        ES_CLIENT.indices.put_settings(index=DOCUMENT_MAPPING.NAME,
                                       body={"index": {
                                           "max_result_window": index_document_size
                                       }})

    response = ES_CLIENT.search(index=DOCUMENT_MAPPING.NAME,
                                request_timeout=120,
                                query=res_query,
                                size=index_document_size
                                )

    # change result window size to default
    if index_document_size > SEARCH_WINDOW_SIZE:
        ES_CLIENT.indices.put_settings(index=DOCUMENT_MAPPING.NAME,
                                    body={"index": {
                                        "max_result_window": SEARCH_WINDOW_SIZE
                                    }})

    return response['hits']['hits']


def apply(patch_obj=None):
    start_time = time.time()

    # ------------------------------------------------------------------
    document_list = get_document_list(SOURCE_ID, patch_obj)
    document_list_with_graph_data = add_graphs_data(document_list)

    # -------------------- ingest documents --------------------------

    import math
    batch_size = 100000
    slice_count = math.ceil(document_list_with_graph_data.__len__() / batch_size)
    for i in range(slice_count):
        print(f"Insert in Paragraph Index {i}/{slice_count}")
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, document_list_with_graph_data.__len__())
        sub_list = document_list_with_graph_data[start_idx:end_idx]
        new_index = IndexObjectWithId(
                        DOCUMENT_MAPPING.NAME,
                        DOCUMENT_SETTING.SETTING,
                        DOCUMENT_MAPPING.MAPPING
                    )
        new_index.create()
        new_index.bulk_insert_documents(sub_list)


    end_time = time.time()

    print("graph_data added  in {} seconds".format(end_time - start_time))
