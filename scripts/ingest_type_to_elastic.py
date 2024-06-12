from elastic.connection import ESIndex, SEARCH_WINDOW_SIZE, IndexObjectWithId
import re
from input_configs import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING

# es configs
CURRENT_VECTOR_ID = 1


def extract_document_type_data(index_name, index_mapping, index_setting, prefix, source_id, last_id="0", is_paragraph=False):
    res_query = {
        "term": {
            prefix + "source_id":  source_id
        }
    }

    result = []
    i = 1
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

        for hit in hits_data:
            _id = hit["_id"]
            _source = hit['_source']
            _source["_id"] = _id
            _source[prefix + "type"] = doc_type_extractor(_source[prefix + "name"])
            last_id = _id
            result.append(_source)

        print(f"============{prefix}=========>", i*SEARCH_WINDOW_SIZE)
        i += 1

        if result.__len__() % 20000 == 0:
            new_index = IndexObjectWithId(index_name, index_setting, index_mapping)
            new_index.bulk_insert_documents(result)
            result = []

    if result.__len__() > 0:
        new_index = IndexObjectWithId(index_name, index_setting, index_mapping)
        new_index.bulk_insert_documents(result)


def replace_nbsp_with_space(text):
    text = re.sub(r'\u200c', ' ', text)
    text = re.sub(r'\u0020', ' ', text)
    text = re.sub(r'\u200f', ' ', text)
    return text

def arabic_char_preprocessing(text):
    arabic_char = {"آ": "ا", "أ": "ا", "إ": "ا", "ي": "ی", "ئ": "ی", "ة": "ه", "ۀ": "ه", "ك": "ک", "َ": "", "ُ": "", "ِ": "",
                   "": ""}
    for key, value in arabic_char.items():
        text = text.replace(key, value)

    return text

def doc_type_extractor(name):
    type_list = {"موافقت نامه": ["موافقتنامه", "موافقت نامه", "موافقت"],
                 "مصوبه": ["مصوبات", "مصوبه"],
                 "مقرره": ["مقررات", "مقرره"],
                 "آيين نامه": ["آيين نامه", "آییننامه", "آئين نامه", "آئيننامه"],
                 "طرح": ["طرح"],
                 "لغو": ["لغو"],
                 "انتصاب": ["انتصاب"],
                 "قانون": ["قانون"],
                 "ابطال": ["ابطال"],
                 "مجوز": ["مجوز"],
                 "تمدید": ["تمدید"],
                 "تامین": ["تامین"],
                 "ماده واحده": ["ماده واحده"],
                 "تصميم": ["تصميم"],
                 "پروانه": ["پروانه", "اعطای پروانه"],
                 "واگذاری": ["واگذاری"],
                 "پرداخت": ["پرداخت"],
                 "لایحه": ["لایحه"],
                 "اصلاحیه": ["اصلاحیه", "اصلاح", "اصلاحات"],
                 "اساسنامه": ["اساسنامه", "اساس نامه"],
                 "ابلاغیه": ["ابلاغیه", "ابلاغ"],
                 "رأی": ["رأی", "آرا", "تعارض آراء", "رای"],
                 "برنامه": ["برنامه"],
                 "حکم": ["حکم", "احکام"],
                 "دستورالعمل": ["دستورالعمل", "دستور العمل", "دستور"],
                 "بخشنامه": ["بخشنامه", "بخش نامه"],
                 "شیوه نامه": ["شیوه نامه", "شیوه نامه"],
                 "نظر مشورتی": ["نظر مشورتی", "نظرمشورتی"],
                 "ابلاغیه رهبری": ["ابلاغات رهبری", "ابلاغیه رهبری"],
                 "بیانات رهبری": ["بیانات رهبری", "بیانیه رهبری"],
                 }

    name = arabic_char_preprocessing(replace_nbsp_with_space(name))

    for main_type, key_list in type_list.items():
        for key in key_list:
            key = arabic_char_preprocessing(replace_nbsp_with_space(key))

            prefix_list = ["تصویب نامه", "تصويبنامه", "تصويب‌نامه", "تصویب"]
            for prefix in prefix_list:
                name = name.replace(prefix + " ", "")

            key1 = key.replace(" ", "\u200c")
            key2 = key.replace(" ", "")
            if name.startswith(key + " ") or name.startswith(key1 + " ") or name.startswith(key2 + " "):
                return main_type

    return "سایر"


def apply():

    index_name = DOCUMENT_MAPPING.NAME
    setting = DOCUMENT_SETTING.SETTING
    mapping = DOCUMENT_MAPPING.MAPPING
    extract_document_type_data(index_name, mapping, setting, "", SOURCE_ID, is_paragraph=False)

    index_name = PARAGRAPH_MAPPING.NAME
    setting = PARAGRAPH_SETTING.SETTING
    mapping = PARAGRAPH_MAPPING.MAPPING
    extract_document_type_data(index_name, mapping, setting, "document_", SOURCE_ID, is_paragraph=True)
