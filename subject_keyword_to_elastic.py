from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE, IndexObjectWithId
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING
import heapq
from operator import itemgetter

document_index_name = "hooshyar3_document_index_v2"
document_index_settings = DOCUMENT_SETTING.SETTING
document_index_mappings = {
    "properties": {
        "actors": {
            "type": "nested",
            "properties": {
                "frequency": {
                    "type": "short"
                },
                "id": {
                    "type": "short"
                },
                "name": {
                    "type": "keyword"
                }
            }
        },
        "affect_detail": {
            "type": "nested",
            "properties": {
                "clause": {
                    "type": "text"
                },
                "document_date": {
                    "properties": {
                        "day": {
                            "properties": {
                                "name": {
                                    "type": "keyword"
                                },
                                "number": {
                                    "type": "short"
                                }
                            }
                        },
                        "month": {
                            "properties": {
                                "name": {
                                    "type": "keyword"
                                },
                                "number": {
                                    "type": "short"
                                }
                            }
                        },
                        "year": {
                            "type": "short"
                        }
                    }
                },
                "document_id": {
                    "type": "keyword"
                },
                "document_name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "term_vector": "with_positions_offsets",
                    "analyzer": "persian_custom_analyzer"
                },
                "paragraph_text": {
                    "type": "text",
                    "term_vector": "with_positions_offsets",
                    "analyzer": "persian_custom_analyzer"
                },
                "status": {
                    "type": "keyword"
                },
                "status_type": {
                    "type": "keyword"
                }
            }
        },
        "affected_by": {
            "type": "keyword"
        },
        "affected_graph_data": {
            "properties": {
                "edges_data": {
                    "type": "flattened"
                },
                "nodes_data": {
                    "type": "flattened"
                }
            }
        },
        "affecting": {
            "type": "keyword"
        },
        "category": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            },
            "analyzer": "persian_custom_analyzer"
        },
        "content": {
            "type": "text",
            "term_vector": "with_positions_offsets",
            "analyzer": "persian_custom_analyzer"
        },
        "datetime": {
            "properties": {
                "day": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "number": {
                            "type": "short"
                        }
                    }
                },
                "hour": {
                    "type": "short"
                },
                "minute": {
                    "type": "short"
                },
                "month": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "number": {
                            "type": "short"
                        }
                    }
                },
                "year": {
                    "type": "short"
                }
            }
        },
        "download_datetime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
        },
        "extra_data": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "keyword"
                }
            }
        },
        "file_path": {
            "type": "keyword"
        },
        "keyword_main_subject": {
            "type": "keyword"
        },
        "keyword_subjects": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "float"
                }
            }
        },
        "keyword_subjects_words": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "keyword"
                }
            }
        },
        "last_status": {
            "type": "keyword"
        },
        "level": {
            "type": "keyword"
        },
        "main_subject": {
            "type": "keyword"
        },
        "name": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            },
            "term_vector": "with_positions_offsets",
            "analyzer": "persian_custom_analyzer"
        },
        "regulation_related": {
            "type": "keyword"
        },
        "related_graph_data": {
            "properties": {
                "edges_data": {
                    "type": "flattened"
                },
                "nodes_data": {
                    "type": "flattened"
                }
            }
        },
        "revoked_type_name": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "rules_related": {
            "type": "keyword"
        },
        "source_id": {
            "type": "short"
        },
        "source_name": {
            "type": "keyword"
        },
        "source_url": {
            "type": "keyword"
        },
        "subjects": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "float"
                }
            }
        },
        "tika_meta_data": {
            "properties": {
                "character_count": {
                    "type": "integer"
                },
                "character_count_with_spaces": {
                    "type": "integer"
                },
                "content_length": {
                    "type": "integer"
                },
                "create_datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
                },
                "format": {
                    "type": "keyword"
                },
                "modified_datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
                },
                "number_of_pages": {
                    "type": "integer"
                },
                "paragraph_count": {
                    "type": "short"
                },
                "word_count": {
                    "type": "integer"
                }
            }
        },
        "type": {
            "type": "keyword"
        }
    }
}

# insert paragraphs to index
paragraph_index_name = "hooshyar3_paragraph_index_v2"
paragraph_index_settings = PARAGRAPH_SETTING.SETTING
paragraph_index_mappings = {
    "properties": {
        "content": {
            "type": "text",
            "term_vector": "with_positions_offsets",
            "analyzer": "persian_custom_analyzer"
        },
        "document_category": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            },
            "analyzer": "persian_custom_analyzer"
        },
        "document_datetime": {
            "properties": {
                "day": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "number": {
                            "type": "short"
                        }
                    }
                },
                "hour": {
                    "type": "short"
                },
                "minute": {
                    "type": "short"
                },
                "month": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "number": {
                            "type": "short"
                        }
                    }
                },
                "year": {
                    "type": "short"
                }
            }
        },
        "document_id": {
            "type": "keyword"
        },
        "document_keyword_main_subject": {
            "type": "keyword"
        },
        "document_last_status": {
            "type": "keyword"
        },
        "document_level": {
            "type": "keyword"
        },
        "document_main_subject": {
            "type": "keyword"
        },
        "document_name": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            },
            "term_vector": "with_positions_offsets",
            "analyzer": "persian_custom_analyzer"
        },
        "document_source_id": {
            "type": "short"
        },
        "document_source_name": {
            "type": "keyword"
        },
        "document_type": {
            "type": "keyword"
        },
        "entities": {
            "type": "nested",
            "properties": {
                "dates": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "facilities": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "locations": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "money": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "name": {
                    "type": "keyword"
                },
                "organizations": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "percents": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "persons": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "products": {
                    "properties": {
                        "end": {
                            "type": "long"
                        },
                        "start": {
                            "type": "long"
                        },
                        "word": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "value": {
                    "type": "nested"
                }
            }
        },
        "keyword_main_subject": {
            "type": "keyword"
        },
        "keyword_subjects": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "float"
                }
            }
        },
        "keyword_subjects_words": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "keyword"
                }
            }
        },
        "main_subject": {
            "type": "keyword"
        },
        "paragraph_number": {
            "type": "integer"
        },
        "sentiment": {
            "type": "keyword"
        },
        "subjects": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "value": {
                    "type": "float"
                }
            }
        }
    }
}

# es configs
CURRENT_VECTOR_ID = 1

def extract_paragraph_data(keyword, country, patch_obj):
    last_id = "0"

    res_query = {
        "bool": {
            "must": [
                {
                    "term":
                        {
                            "document_source_id": country
                        }
                } if patch_obj is None else
                {
                    "terms":
                        {
                            "document_id": patch_obj
                        }
                },
                {
                    "match_phrase": {
                        "content": keyword
                    }
                }
            ]
        }
    }
    result = []
    while True:
        response = ESIndex.CLIENT.search(
            index=paragraph_index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)

        if hits_count == 0:
            break

        for hit in hits_data:
            _id = hit["_id"]
            last_id = _id
            result.append(_id)

    return result



def normalize_dictionary(dictionary):
    sum_value = sum([value for key, value in dictionary.items()])
    factor = 1/sum_value if sum_value > 0 else 0
    result_dict = {}
    for key, value in dictionary.items():
        result_dict[key] = round(dictionary[key] * factor, 3)

    return result_dict

def get_subject_para_keyword_data(country, patch_obj=None):
    subject_file = open("./other_files/subject_v1.txt", encoding="utf8").read().split("\n")
    subject_dictionary = {}
    last_subject = None

    for line in subject_file:
        if "*" in line:
            last_subject = line.replace("*", "").strip()
            subject_dictionary[last_subject] = []
        else:
            subject_dictionary[last_subject].append(line.strip())

    para_subject_keyword_dictionary = {}

    cntr = 0
    for subject, key_list in subject_dictionary.items():

        for keyword in key_list:
            cntr += 1
            print(f"extract paragraph keywords {cntr}")

            para_list = extract_paragraph_data(keyword, country, patch_obj)

            for para_id in para_list:
                if para_id not in para_subject_keyword_dictionary:
                    para_subject_keyword_dictionary[para_id] = {}

                if subject not in para_subject_keyword_dictionary[para_id]:
                    para_subject_keyword_dictionary[para_id][subject] = []

                para_subject_keyword_dictionary[para_id][subject].append(keyword)

    subject_list = subject_dictionary.keys()

    for para_id, subject_dict in para_subject_keyword_dictionary.items():
        for subject in subject_list:
            if subject not in subject_dict:
                para_subject_keyword_dictionary[para_id][subject] = []

    paragraph_score_dict = {}
    cntr = 0
    for para_id, value in para_subject_keyword_dictionary.items():
        cntr += 1
        print(f"normailize score {cntr}/{len(para_subject_keyword_dictionary.keys())}")

        para_subject_score_dict = {}
        for subject in subject_list:
            para_subject_score_dict[subject] = 0
            if subject in value:
                para_subject_score_dict[subject] = value[subject].__len__()

        paragraph_score_dict[para_id] = normalize_dictionary(para_subject_score_dict)

    return paragraph_score_dict, para_subject_keyword_dictionary


def extract_paragraph_data_for_add(country, paragraph_score_dict, para_subject_keyword_dictionary, last_id="0", patch_obj=None):
    result = []
    document_score_dict = {}
    document_subject_keyword_dictionary = {}
    if patch_obj is None:
        res_query = {
            "term":
            {
                "document_source_id": country
            }
        }
    else:
        res_query = {
            "terms":
            {
                "document_id": patch_obj
            }
        }
    cntr = 0
    while True:
        response = ESIndex.CLIENT.search(
            index=paragraph_index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)

        if hits_count == 0:
            break

        cntr += SEARCH_WINDOW_SIZE
        print("Extract Paragraph Data for Add", cntr)

        for hit in hits_data:
            _id = hit["_id"]
            last_id = _id
            _source = hit['_source']
            _source["_id"] = _id
            document_id = _source["document_id"]

            _source["keyword_subjects"] = {}
            _source["keyword_main_subject"] = "سایر"
            _source["keyword_subjects_words"] = {}

            if document_id not in document_score_dict:
                document_score_dict[document_id] = []
                document_subject_keyword_dictionary[document_id] = []

            if _id in para_subject_keyword_dictionary:
                _source["keyword_subjects"] = dict_to_json(paragraph_score_dict[_id])
                _source["keyword_subjects_words"] = dict_to_json_list(para_subject_keyword_dictionary[_id])
                _source["keyword_main_subject"] = list(dict(heapq.nlargest(1, paragraph_score_dict[_id].items(), key=itemgetter(1))).keys())[0]

                document_score_dict[document_id].append(paragraph_score_dict[_id])
                document_subject_keyword_dictionary[document_id].append(para_subject_keyword_dictionary[_id])

            result.append(_source)

    return result, document_score_dict, document_subject_keyword_dictionary


def concat_dictionary(dict_list):
    result_dict = {}
    for dict_data in dict_list:
        for key, value in dict_data.items():
            if key not in result_dict:
                result_dict[key] = value
            else:
                result_dict[key] += value

    return result_dict


def dict_to_json_list(input_data_dict):
    result = []
    for key, value in input_data_dict.items():
        res = {
            "name": key,
            "value": list(set(value))
        }
        result.append(res)
    return result


def dict_to_json(input_data_dict):
    result = []
    for key, value in input_data_dict.items():
        res = {
            "name": key,
            "value": value
        }
        result.append(res)
    return result


def extract_document_data(country, document_score_dict, document_subject_keyword_dictionary, last_id="0", patch_obj=None):
    result = []
    doc_subject_dict = {}

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

    cntr  = 0
    while True:
        response = ESIndex.CLIENT.search(
            index=document_index_name,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)

        if hits_count == 0:
            break

        cntr += SEARCH_WINDOW_SIZE
        print("Extract Document Data: ", cntr)

        for hit in hits_data:
            _id = hit["_id"]
            last_id = _id
            _source = hit['_source']
            _source["_id"] = _id

            _source["keyword_subjects"] = {}
            _source["keyword_main_subject"] = "سایر"
            _source["keyword_subjects_words"] = {}

            if _id in document_score_dict:
                _source["keyword_subjects"] = dict_to_json(normalize_dictionary(concat_dictionary(document_score_dict[_id])))
                _source["keyword_subjects_words"] = dict_to_json_list(concat_dictionary(document_subject_keyword_dictionary[_id]))
                if concat_dictionary(document_score_dict[_id]).keys().__len__() > 0:
                    _source["keyword_main_subject"] = list(dict(heapq.nlargest(1, concat_dictionary(document_score_dict[_id]).items(), key=itemgetter(1))).keys())[0]

            doc_subject_dict[_id] = _source["keyword_main_subject"]
            result.append(_source)

    return result, doc_subject_dict


def add_doc_subject_to_para_data(doc_subject_dict, paragraph_data):
    for i in range(paragraph_data.__len__()):
        paragraph_data[i]["document_keyword_main_subject"] = doc_subject_dict[paragraph_data[i]["document_id"]]
    return paragraph_data


def apply():
    SOURCE_ID = 4
    patch_obj = None
    paragraph_score_dict, para_subject_keyword_dictionary = get_subject_para_keyword_data(SOURCE_ID, patch_obj=patch_obj)
    para_data, document_score_dict, document_subject_keyword_dictionary = extract_paragraph_data_for_add(SOURCE_ID, paragraph_score_dict, para_subject_keyword_dictionary, patch_obj=patch_obj)
    doc_data, doc_subject_dict = extract_document_data(SOURCE_ID, document_score_dict, document_subject_keyword_dictionary, patch_obj=patch_obj)
    para_data = add_doc_subject_to_para_data(doc_subject_dict, para_data)

    index_name = paragraph_index_name
    setting = paragraph_index_settings
    mapping = paragraph_index_mappings

    import math
    batch_size = 100000
    slice_count = math.ceil(para_data.__len__() / batch_size)
    for i in range(slice_count):
        print(f"Insert in Paragraph Index {i}/{slice_count}")
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, para_data.__len__())
        sub_list = para_data[start_idx:end_idx]
        new_index = IndexObjectWithId(index_name, setting, mapping)
        new_index.create()
        new_index.bulk_insert_documents(sub_list)

    index_name = document_index_name
    setting = document_index_settings
    mapping = document_index_mappings
    new_index = IndexObjectWithId(index_name, setting, mapping)
    new_index.create()
    new_index.bulk_insert_documents(doc_data)

    print("========================================\n\n", "Done\n\n\n========================================\n\n\n")

apply()