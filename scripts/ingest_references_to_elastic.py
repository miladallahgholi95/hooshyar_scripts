from elastic.connection import ESIndex, ES_CLIENT, SEARCH_WINDOW_SIZE, IndexObjectWithId
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING, REFERENCES_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING
import re

CURRENT_VECTOR_ID = 1

def extract_document_list(source_index, source_id, last_id="0"):
    res_query = {
        "bool": {
            "must": [
                {
                    "term": {
                        "source_id": source_id
                    }
                }
            ],
            "must_not": [
                {
                    "term": {
                        "type": "سایر"
                    }
                }
            ]

        }
    }

    result = []
    while True:
        index_name = source_index
        response = ES_CLIENT.search(
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

        last_id = hits_data[-1]["_id"]

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            result.append(_source)

    return result


def extract_document_references(source_index, document_name, date_time, last_id="0"):
    year_number = date_time["year"]
    month_number = date_time["month"]["number"]
    day_number = date_time["day"]["number"]

    res_query = {
        "bool":
            {
                "must": [
                    {
                        "match_phrase": {
                            "content": document_name
                        }
                    },
                    {
                        "bool":
                        {
                            "should": [
                                {
                                    "range": {
                                        "document_datetime.year": {
                                            "gt": year_number
                                        }
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "term": {
                                                    "document_datetime.year": year_number
                                                }
                                            },
                                            {
                                                "range": {
                                                    "document_datetime.month.number": {
                                                        "gt": month_number
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "term": {
                                                    "document_datetime.year": year_number
                                                }
                                            },
                                            {
                                                "term": {
                                                    "document_datetime.month.number": month_number
                                                }
                                            },
                                            {
                                                "range": {
                                                    "document_datetime.day.number": {
                                                        "gt": day_number
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ],
                "must_not": [
                    {
                        "term": {
                            "document_name.keyword": document_name
                        }
                    }
                ]
            }
    }
    highlight_query = {
        "fields": {
            "content": {}
        }
    }

    result = []
    while True:
        index_name = source_index
        response = ESIndex.CLIENT.search(
            index=index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            highlight=highlight_query,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)
        print(hits_count)
        print(res_query)

        if hits_count == 0:
            break

        last_id = hits_data[-1]["_id"]

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            _source["highlight_content"] = hit["highlight"]["content"][0]
            result.append(_source)

    return result

def get_content_index(input_text):

    start_char = r"<em>"
    end_char = r"</em>"

    start_matches = re.finditer(start_char, input_text)
    start_indices = [match.start() for match in start_matches]

    end_matches = re.finditer(end_char, input_text)
    end_indices = [match.start()-3 for match in end_matches]

    start_delete_index = []
    end_delete_index = []

    for i in range(1, len(start_indices)):
        start_indices[i] -= i * 9
        end_indices[i] -= i * 9

        diff = start_indices[i] - end_indices[i - 1]
        if diff <= 5:
            start_delete_index.append(i)
            end_delete_index.append(i - 1)

    start_indices_final = [start_indices[i] for i in range(len(start_indices)) if i not in start_delete_index]
    end_indices_final = [end_indices[i] - 1 for i in range(len(end_indices)) if i not in end_delete_index]

    locations = list(zip(start_indices_final, end_indices_final))

    return locations

def get_new_ref_list(base_list, check_list):
    result = []
    for interval1 in check_list:
        status = False
        for interval2 in base_list:
            if interval1[0] <= interval2[1] and interval1[1] >= interval2[0]:
                status = True
                break
        if not status:
            result.append(interval1)
    return result


def apply():

    index_name = DOCUMENT_MAPPING.NAME

    document_list = extract_document_list(index_name, SOURCE_ID)

    document_list = sorted(document_list, key=lambda x: len(x["name"]), reverse=True)

    result_data = []

    doc_ref_count_dict = {}
    doc_ref_para_dict = {}
    seen_paragraphs = {}

    cntr = 0
    for _doc in document_list:
        cntr += 1
        print(f"extract ref [{cntr}/ {document_list.__len__()}]")

        target_document_id = _doc["_id"]
        target_document_name = _doc["name"]
        target_document_source_id = _doc["source_id"]
        target_document_source_name = _doc["source_name"]
        target_document_datetime = _doc["datetime"]
        target_document_category = _doc["category"]
        target_document_main_subject = _doc["main_subject"]
        target_document_type = _doc["type"]
        target_document_level = _doc["level"]

        target_node_data = {"id": target_document_id,
                            "name": target_document_name,
                            "node_type": "document"}
        references_list = extract_document_references(index_name, target_document_name, target_document_datetime)
        for ref_paragraph in references_list:
            ref_paragraph_id = ref_paragraph["_id"]
            source_document_id = ref_paragraph["document_id"]
            source_document_name = ref_paragraph["document_name"]
            source_document_source_id = ref_paragraph["document_source_id"]
            source_document_source_name = ref_paragraph["document_source_name"]
            source_document_datetime = ref_paragraph["document_datetime"]
            source_document_category = ref_paragraph["document_category"]
            source_document_main_subject = ref_paragraph["document_main_subject"]
            source_document_type = ref_paragraph["document_type"]
            source_document_level = ref_paragraph["document_level"]
            highlight_content = ref_paragraph["highlight_content"]

            highlight_index = get_content_index(highlight_content)

            if ref_paragraph_id in seen_paragraphs:
                highlight_index = get_new_ref_list(seen_paragraphs[ref_paragraph_id], highlight_index)
                seen_paragraphs[ref_paragraph_id] += highlight_index
            else:
                seen_paragraphs[ref_paragraph_id] = highlight_index

            if highlight_index.__len__() == 0:
                continue

            source_node_data = {"id": source_document_id,
                                "name": source_document_name,
                                "node_type": "document"}

            ref_id = source_document_id + "__" + target_document_id

            edge_data = {
                "id": ref_id,
                "source": source_document_id,
                "source_name": source_document_name,
                "source_node_type": "document",
                "target": target_document_id,
                "target_name": target_document_name,
                "target_node_type": "document",
                "weight": 1,
            }

            if ref_id not in doc_ref_para_dict:
                doc_ref_para_dict[ref_id] = {}

            doc_ref_para_dict[ref_id][ref_paragraph_id] = highlight_index

            if ref_id in doc_ref_count_dict:
                doc_ref_count_dict[ref_id] += 1
            else:
                result_dict = {
                    "_id": ref_id,

                    "source_document_id": source_document_id,
                    "source_document_name": source_document_name,
                    "source_document_source_id": source_document_source_id,
                    "source_document_source_name": source_document_source_name,
                    "source_document_datetime": source_document_datetime,
                    "source_document_category": source_document_category,
                    "source_document_main_subject": source_document_main_subject,
                    "source_document_type": source_document_type,
                    "source_document_level": source_document_level,

                    "target_document_id": target_document_id,
                    "target_document_name": target_document_name,
                    "target_document_source_id": target_document_source_id,
                    "target_document_source_name": target_document_source_name,
                    "target_document_datetime": target_document_datetime,
                    "target_document_category": target_document_category,
                    "target_document_main_subject": target_document_main_subject,
                    "target_document_type": target_document_type,
                    "target_document_level": target_document_level,

                    "text_indices_list": {},

                    "node_data": [source_node_data, target_node_data],
                    "edge_data": edge_data
                }
                result_data.append(result_dict)
                doc_ref_count_dict[ref_id] = 1

    for i in range(result_data.__len__()):
        _id = result_data[i]["_id"]
        weight = doc_ref_count_dict[_id]
        text_index_list = doc_ref_para_dict[_id]
        para_data = []
        for para_id, indices in text_index_list.items():
            res = {
                "paragraph_id": para_id,
                "indices": str(indices)
            }
            para_data.append(res)

        result_data[i]["text_indices_list"] = para_data
        result_data[i]["edge_data"]["weight"] = weight

    index_name = REFERENCES_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = REFERENCES_MAPPING.MAPPING
    new_index = IndexObjectWithId(index_name, setting, mapping)
    new_index.create()
    new_index.bulk_insert_documents(result_data)

    print("========================================\n\n\n\n\n", "Done========================================\n\n\n\n\n")
