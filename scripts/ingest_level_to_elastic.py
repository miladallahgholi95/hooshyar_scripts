from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE, ES_CLIENT, IndexObjectWithId
import re
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING

# es configs
CURRENT_VECTOR_ID = 1

def extract_document_level_data(source_index, res_query, level, prefix, last_id="0", size=500000):
    result = []
    i = 1
    while i * SEARCH_WINDOW_SIZE <= size:
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

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            _source[prefix + "level"] = level
            last_id = _id
            result.append(_source)

        print(i * SEARCH_WINDOW_SIZE)
        i += 1

    return result, last_id


def replace_nbsp_with_space(text):
    text = re.sub(r'\u200c', ' ', text)
    text = re.sub(r'\u0020', ' ', text)
    text = re.sub(r'\u200f', ' ', text)
    return text


def arabic_char_preprocessing(text):
    arabic_char = {"آ": "ا", "أ": "ا", "إ": "ا", "ي": "ی", "ئ": "ی", "ة": "ه", "ۀ": "ه", "ك": "ک", "َ": "", "ُ": "",
                   "ِ": "",
                   "": ""}
    for key, value in arabic_char.items():
        text = text.replace(key, value)

    return text


def apply():
    asnad_bala_dasti_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase": {
                                            "category": "شورای عالی"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "category": "رهبری"
                                        }
                                    }
                                ],
                                "must_not": [
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزرای"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "کمیسیون"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    qanoon_query_type = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "term":
                                {
                                    "type": "قانون"
                                }
                        }
                    ]
            }
    }
    qanoon_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "category.keyword": "مجلس شورا"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "لایحه"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    moqarare_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزارت"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزیران"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزرای"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "معاون وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "هيات وزيران"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "نخست وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "category": "کمیسیون"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "category": "سازمان"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "آيين نامه"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "دستورالعمل"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "بخشنامه"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "شیوه نامه"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    nazar_mashverati_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "term":
                                {
                                    "type": "نظر مشورتی"
                                }
                        }
                    ]
            }
    }
    ara_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "source_id": SOURCE_ID
                                }
                        },
                        {
                            "term":
                                {
                                    "type": "رأی"
                                }
                        }
                    ]
            }
    }

    index_name = DOCUMENT_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = DOCUMENT_MAPPING.MAPPING
    data_to_search = [(index_name, asnad_bala_dasti_query, "اسناد بالا دستی", ""),
                      (index_name, qanoon_query_type, "قانون", ""), (index_name, qanoon_query, "قانون", ""),
                      (index_name, moqarare_query, "مقررات", ""),
                      (index_name, nazar_mashverati_query, "نظر مشورتی", ""), (index_name, ara_query, "رأی", ""),
                      (index_name, {"term": {"source_id": SOURCE_ID}}, "سایر", "")]
    ids = {}
    for index_name, res_query, level, prefix in data_to_search:
        last_id = "0"
        while True:
            all_data = []
            data, last_id = extract_document_level_data(index_name, res_query, level, prefix, last_id, size=10000)
            if len(data) == 0:
                break

            for item in data:
                if ids.get(item["_id"]) is None:
                    ids[item["_id"]] = True
                    all_data.append(item)
            del data
            new_index = ParagraphIndex(index_name, setting, mapping)
            new_index.create()
            new_index.bulk_insert_documents(all_data)


    asnad_bala_dasti_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase": {
                                            "document_category": "شورای عالی"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "document_category": "رهبری"
                                        }
                                    }
                                ],
                                "must_not": [
                                    {
                                        "match_phrase_prefix": {
                                            "category": "وزرای"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "category": "کمیسیون"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    qanoon_query_type = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "term":
                                {
                                    "document_type": "قانون"
                                }
                        }
                    ]
            }
    }
    qanoon_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "category.keyword": "مجلس شورا"
                                        }
                                    },
                                    {
                                        "term": {
                                            "type": "لایحه"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    moqarare_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "وزارت"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "وزیران"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "وزرای"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "معاون وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "هيات وزيران"
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "document_category": "نخست وزیر"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "document_category": "کمیسیون"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "document_category": "سازمان"
                                        }
                                    },
                                    {
                                        "term": {
                                            "document_type": "آيين نامه"
                                        }
                                    },
                                    {
                                        "term": {
                                            "document_type": "دستورالعمل"
                                        }
                                    },
                                    {
                                        "term": {
                                            "document_type": "بخشنامه"
                                        }
                                    },
                                    {
                                        "term": {
                                            "document_type": "شیوه نامه"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
            }
    }
    nazar_mashverati_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "term":
                                {
                                    "document_type": "نظر مشورتی"
                                }
                        }
                    ]
            }
    }
    ara_query = {
        "bool":
            {
                "must":
                    [
                        {
                            "term":
                                {
                                    "document_source_id": SOURCE_ID
                                }
                        } ,
                        {
                            "term":
                                {
                                    "document_type": "رأی"
                                }
                        }
                    ]
            }
    }

    index_name = PARAGRAPH_MAPPING.NAME
    setting = PARAGRAPH_SETTING.SETTING
    mapping = PARAGRAPH_MAPPING.MAPPING
    data_to_search = [(index_name, asnad_bala_dasti_query, "اسناد بالا دستی", "document_"),
                      (index_name, qanoon_query_type, "قانون", "document_"),
                      (index_name, qanoon_query, "قانون", "document_"),
                      (index_name, moqarare_query, "مقررات", "document_"),
                      (index_name, nazar_mashverati_query, "نظر مشورتی", "document_"),
                      (index_name, ara_query, "رأی", "document_"),
                      (index_name, {"term": {"document_source_id": SOURCE_ID}}, "سایر", "document_")]

    ids = {}
    for index_name, res_query, level, prefix in data_to_search:
        last_id = "0"
        while True:
            all_data = []
            data, last_id = extract_document_level_data(index_name, res_query, level, prefix, last_id, size=10000)
            if len(data) == 0:
                break
            for item in data:
                if ids.get(item["_id"]) is None:
                    ids[item["_id"]] = True
                    all_data.append(item)
            del data
            new_index = ParagraphIndex(index_name, setting, mapping)
            new_index.create()
            new_index.bulk_insert_documents(all_data)

    print("========================================\n\n\n\n\n",
          "Done========================================\n\n\n\n\n")