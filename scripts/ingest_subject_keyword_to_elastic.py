from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING
import heapq
from operator import itemgetter


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
            index=PARAGRAPH_MAPPING.NAME,
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


class ParagraphIndex(ESIndex):
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
        cntr += 1
        print(f"extract paragraph keywords {cntr}/{len(subject_dictionary.keys())}")
        for keyword in key_list:

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
    for para_id, value in para_subject_keyword_dictionary.items():
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
            index=PARAGRAPH_MAPPING.NAME,
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
            index=DOCUMENT_MAPPING.NAME,
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


def apply(patch_obj):
    paragraph_score_dict, para_subject_keyword_dictionary = get_subject_para_keyword_data(SOURCE_ID, patch_obj=patch_obj)
    para_data, document_score_dict, document_subject_keyword_dictionary = extract_paragraph_data_for_add(SOURCE_ID, paragraph_score_dict, para_subject_keyword_dictionary, patch_obj=patch_obj)
    doc_data, doc_subject_dict = extract_document_data(SOURCE_ID, document_score_dict, document_subject_keyword_dictionary, patch_obj=patch_obj)
    para_data = add_doc_subject_to_para_data(doc_subject_dict, para_data)

    index_name = PARAGRAPH_MAPPING.NAME
    setting = PARAGRAPH_SETTING.SETTING
    mapping = PARAGRAPH_MAPPING.MAPPING

    import math
    batch_size = 100000
    slice_count = math.ceil(para_data.__len__() / batch_size)
    for i in range(slice_count):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, para_data.__len__())
        sub_list = para_data[start_idx:end_idx]
        new_index = ParagraphIndex(index_name, setting, mapping)
        new_index.create()
        new_index.bulk_insert_documents(sub_list)

    index_name = DOCUMENT_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = DOCUMENT_MAPPING.MAPPING
    new_index = ParagraphIndex(index_name, setting, mapping)
    new_index.create()
    new_index.bulk_insert_documents(doc_data)

    print("========================================\n\n", "Done\n\n\n========================================\n\n\n")

