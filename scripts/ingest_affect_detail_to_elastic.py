import time
from elastic.connection import ESIndex, ES_CLIENT, SEARCH_WINDOW_SIZE, IndexObjectWithId
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING
import pandas as pd
import requests
import json

def get_doc_affect_data(doc_id, pid_2_doc_id, date_dict):
    url = f'http://127.0.0.1:50081/docs-status/{doc_id}/'  #
    response = requests.get(url)
    affect_data = json.loads(response.content)

    if not affect_data:
        return []

    doc_af_data = []
    for af_data in affect_data:
        if "extracted_status_detail" in af_data and af_data["extracted_status_detail"] != {}:
            extracted_status_detail = af_data["extracted_status_detail"]
            caused_by_pid = int(extracted_status_detail['document_caused_by_pid'])
            document_id = pid_2_doc_id[caused_by_pid] if caused_by_pid in pid_2_doc_id else ""
            document_date = date_dict[document_id] if document_id in date_dict else None
            doc_af_data.append({
                "status": extracted_status_detail['status'],
                "status_type": extracted_status_detail['status_type'],
                "clause": af_data["clause"].split("\n")[0].replace("\r", ""),
                "document_id": document_id,
                "document_date": document_date,
                "document_name": extracted_status_detail['document_caused_by_name'],
                "paragraph_text": extracted_status_detail[
                    'paragraph_caused_by_text'] if 'paragraph_caused_by_text' in extracted_status_detail else ""
            })

    return doc_af_data

def get_documents_date(source_id):
    res_query = {
        "term": {
            "source_id":  source_id
        }
    }
    result = {}
    i = 1
    last_id = "0"
    while True:
        index_name = DOCUMENT_MAPPING.NAME
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

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            try:
                date_val = _source["datetime"]
                if "hour" in date_val:
                    del date_val["hour"]
                    del date_val["minute"]
            except:
                continue
            result[_id] = date_val
        i += 1
        break
    return result

def extract_document_ingest_data(source_id, patch_obj, date_dictionary, pid_2_doc_id):
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

    result = []
    i = 1
    last_id = "0"
    while True:
        index_name = DOCUMENT_MAPPING.NAME
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

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            _source["affect_detail"] = get_doc_affect_data(_id, pid_2_doc_id, date_dictionary)
            last_id = _id
            result.append(_source)

        print(i*SEARCH_WINDOW_SIZE, "docs data extracted ...")
        i += 1

    return result

def apply(patch_obj=None):
    excel_dataframe = pd.read_excel(EXCEL_FILE_PATH)
    pid_2_doc_id = {}
    for index, row in excel_dataframe.iterrows():
        pid = int(row["pid"].replace("?IDS=", ""))
        hashed_file_name = "tanghih_" + row["hashed_file_name"]
        pid_2_doc_id[pid] = hashed_file_name

    date_dictionary = get_documents_date(SOURCE_ID)
    data = extract_document_ingest_data(SOURCE_ID, patch_obj, date_dictionary, pid_2_doc_id)
    import math
    batch_size = 50000
    index_name = DOCUMENT_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = DOCUMENT_MAPPING.MAPPING
    slice_count = math.ceil(data.__len__() / batch_size)
    for i in range(slice_count):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, data.__len__())
        sub_list = data[start_idx:end_idx]

        new_index = IndexObjectWithId(index_name, setting, mapping)
        new_index.create()
        new_index.bulk_insert_documents(sub_list)

    print("========================================\n\n", "Done\n\n\n========================================\n\n\n")