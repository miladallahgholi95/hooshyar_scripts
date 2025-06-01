import re
import hazm
from copy import deepcopy
from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE
from input_configs import *
from elastic.MAPPINGS import PARAGRAPH_VECTOR_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import PARAGRAPH_SETTING
from sentence_transformers import models, SentenceTransformer
from utils import huggingface
import requests

normalizer = hazm.Normalizer()

CURRENT_VECTOR_ID = 1


class ParagraphsVectorIndex(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, records):
        for record in records:
            new_doc = {
                "source_id": record["source_id"],
                "source_name": record["source_name"],
                "document_id": record["document_id"],
                "document_name": record["document_name"],
                "document_datetime": record["document_datetime"],
                "document_subject": record["document_subject"],
                "document_type": record["document_type"],
                "document_level": record["document_level"],
                "paragraph_number": record["paragraph_number"],
                "vector": record["vector_hooshyar"],
                "content": record["content"],
            }

            new_document = {
                "_index": self.name,
                "_source": new_doc,
                "_id": record["_id"]
            }
            yield new_document


def clean(txt):
    txt = normalizer.normalize(txt)
    txt = txt.replace("ي", "ی")
    txt = txt.replace("\t", " ")
    txt = txt.replace("..", ".")
    txt = txt.replace(":.", ":")
    txt = txt.replace(":\n", ": \n")
    txt = txt.replace(".\n", " .\n")
    txt = txt.replace(".\r\n", " . \n")
    txt = txt.replace("ك", "ک")
    txt = txt.replace("\u200c", " ")
    txt = txt.replace("  ", " ")
    txt = normalizer.normalize(txt)
    txt = txt.lstrip()
    if len(txt) > 1:
        txt = txt.replace(txt[-1], " .")
    while "  " in txt:
        txt = txt.replace("  ", " ")
    return txt


def sentence_extraction(text):
    sentences = []
    text = text.replace("  \n", " . \n")
    text = text.replace("\t", ". \n")
    text = text.replace("*", "")
    sent = hazm.sent_tokenize(text)
    for item in sent:
        if (re.sub('[^a-zA-Zا-ی]+', '', str(item))) != "":
            sentences.append(clean(item))
    return sentences


def get_exist_id(source_id):
    res_query = {
        "term": {
            "source_id": source_id
        }
    }

    last_id = "0"
    ids_list = []
    while True:
        response = ESIndex.CLIENT.search(
            index=PARAGRAPH_VECTOR_MAPPING.NAME,
            query=res_query,
            size=SEARCH_WINDOW_SIZE,
            search_after=[last_id] if last_id != "0" else None,
            sort=[{"_id": {"order": "asc"}}]
        )
        hits_data = response['hits']['hits']
        hits_count = len(hits_data)

        if hits_count == 0:
            break

        for row in hits_data:
            ids_list.append(row["_id"])
            last_id = row["_id"]

    return ids_list


def get_data_list(source_id, exist_ids_list, patch_obj):
    res_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term": {
                                "document_source_id": source_id
                            }
                        } if patch_obj is None else
                        {
                            "terms": {
                                "document_id": patch_obj
                            }
                        }
                        ,
                        {
                            "term": {
                                "document_type": "قانون"
                            }
                        }
                    ],
            }
    }

    i = 0
    corpus = []
    corpus_meta_data = []
    last_id = "0"
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

        i += 1
        print(i)

        if hits_count == 0:
            break

        last_id = hits_data[-1]["_id"]
        PARA_LENGTH_THRESHOLD = 50
        for row in hits_data:
            paragraph_id = row["_id"]
            if paragraph_id not in exist_ids_list:
                paragraph_content = row["_source"]["content"]
                if len(paragraph_content.replace(" ", "")) >= PARA_LENGTH_THRESHOLD:
                    corpus.append(paragraph_content)
                    doc_id = row["_source"]["document_id"]
                    doc_name = row["_source"]["document_name"]
                    doc_datetime = row["_source"]["document_datetime"]
                    source_id = row["_source"]["document_source_id"]
                    paragraph_number = row["_source"]["paragraph_number"]
                    source_name = row["_source"]["document_source_name"]
                    subject = row["_source"]["document_keyword_main_subject"]
                    doc_type = row["_source"]["document_type"]
                    doc_level = row["_source"]["document_level"]
                    res = {
                        "_id": paragraph_id,
                        "source_id": source_id,
                        "source_name": source_name,
                        "document_id": doc_id,
                        "document_name": doc_name,
                        "document_datetime": doc_datetime,
                        "document_subject": subject,
                        "document_type": doc_type,
                        "document_level": doc_level,
                        "paragraph_number": paragraph_number,
                        "content": paragraph_content
                    }
                    corpus_meta_data.append(res)

    return corpus, corpus_meta_data


def apply(patch_obj=None):
    check_exist_id = True

    # Check Exist
    if check_exist_id:
        exist_ids_list = get_exist_id(SOURCE_ID)
    else:
        exist_ids_list = []

    corpus, corpus_meta_data = get_data_list(SOURCE_ID, exist_ids_list, patch_obj)

    # Create index
    paragraph_vector_setting = PARAGRAPH_SETTING.SETTING
    paragraph_vector_mapping = PARAGRAPH_VECTOR_MAPPING.MAPPING
    paragraph_vector_index_name = PARAGRAPH_VECTOR_MAPPING.NAME

    paragraph_vector_new_index = ParagraphsVectorIndex(paragraph_vector_index_name,
                                                       paragraph_vector_setting,
                                                       paragraph_vector_mapping)
    # paragraph_vector_new_index.delete_index()
    paragraph_vector_new_index.create()

    # Create Embedding and Save to Elastic
    batch_size = 10000
    batch_count = int(len(corpus) / batch_size) + 1


    for batch_number in range(batch_count):
        start_idx = batch_number * batch_size
        end_idx = (batch_number + 1) * batch_size
        split_corpus = corpus[start_idx: end_idx]
        split_corpus_meta_data = corpus_meta_data[start_idx: end_idx]
        print(f"*****************************{batch_number} / {batch_count}*************************************")
        corpus_embeddings1 = huggingface.embeddingSentenceModel.encode(deepcopy(split_corpus), show_progress_bar=True)
        for i in range(corpus_embeddings1.__len__()):
            split_corpus_meta_data[i]["vector_hooshyar"] = list(corpus_embeddings1[i])
        paragraph_vector_new_index.bulk_insert_documents(split_corpus_meta_data)

