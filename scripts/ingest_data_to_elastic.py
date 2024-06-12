from pathlib import Path
import glob
import os
import pandas as pd
from tika import parser
from input_configs import *
from elastic.connection import ESIndex
from collections import defaultdict
from statistics import mean
from persiantools.jdatetime import JalaliDate
from elastic.connection import *
from elastic.MAPPINGS import DOCUMENT_MAPPING, PARAGRAPH_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING, PARAGRAPH_SETTING
from utils import huggingface

client = ESIndex
PAGE_SIZE = SEARCH_WINDOW_SIZE


def text_abs_length(text):
    ignoreList = ["!", "@", "$", "%", "^", "&", "*", "(", ")", "_", "+", "-", "/", ":",
                  "*", "'", "،", "؛", ",", "{", "}", '\xad', '­', "[", "]", "«", "»",
                  "<", ">", ".", "?", "؟", "\n", "\r\n", "\t", '"', "'", '۰', "٫", "\u200c", " "]

    for item in ignoreList:
        text = text.replace(item, "")

    return len(text)



def dictionary_list_averager(input_list):
    single_list = []
    for row in input_list:
        single_list += row

    result_dict = defaultdict(list)
    for dictionary in single_list:
        result_dict[dictionary['name']].append(dictionary['value'])
    result_dictionary = {k: mean(v) for k, v in result_dict.items()}

    result_dictionary_list = []
    max_key = None
    max_key_value = 0
    for key, value in result_dictionary.items():
        result_dictionary_list.append({"name": key, "value": value})
        if value > max_key_value:
            max_key = key
            max_key_value = value

    return result_dictionary_list, max_key


CONFIG = {
    "data_path": PATH_TO_EXTRACT_FILES,
    "excel_file_path": EXCEL_FILE_PATH,
    "excel_file_fields_mapping": INPUT_EXCEL_MAPPING,
    "run_sentiment_module": RUN_SENTIMENT_MODULE,
    "run_subject_module": RUN_SUBJECT_MODULE,
    "run_entity_module": RUN_ENTITY_MODULE,
    "add_bulk_size": INGEST_BULK_SIZE
    }


def ingest_data_to_elastic(document_ingest_list, paragraph_ingest_list):

    # # insert documents to index
    # document_index_name = DOCUMENT_MAPPING.NAME
    # document_index_settings = DOCUMENT_SETTING.SETTING
    # document_index_mappings = DOCUMENT_MAPPING.MAPPING
    #
    # new_index = IndexObjectWithId(document_index_name, document_index_settings, document_index_mappings)
    # new_index.create()
    # new_index.bulk_insert_documents(document_ingest_list)
    #
    # # insert paragraphs to index
    # paragraph_index_name = PARAGRAPH_MAPPING.NAME
    # paragraph_index_settings = PARAGRAPH_SETTING.SETTING
    # paragraph_index_mappings = PARAGRAPH_MAPPING.MAPPING
    #
    # new_index = IndexObjectWithId(paragraph_index_name, paragraph_index_settings, paragraph_index_mappings)
    # new_index.create()
    # new_index.bulk_insert_documents(paragraph_ingest_list)

    return True


# @after_response.enable
def apply():
    start_time = time.time()

    source_id = SOURCE_ID
    source_name = SOURCE_NAME

    print("1. Preprocessing")
    data_path = CONFIG["data_path"]
    all_files = glob.glob(data_path + "/*")
    excel_path = CONFIG["excel_file_path"]
    field_names_list = CONFIG["excel_file_fields_mapping"]
    excel_file_dictionary, affect_data_dictionary = excel_to_dict(excel_path, field_names_list)

    # filter and remove added document
    input_files = all_files

    # extract ingest data from file
    print(f"1. Start Process ...")
    input_files_count = input_files.__len__()
    print("Input Files Count: ", input_files_count)
    document_ingest_list = []
    paragraph_ingest_list = []
    cntr = 1
    for file_path in input_files:
        print("Percentage -->", cntr/input_files_count, document_ingest_list.__len__())
        cntr += 1
        try:
            text = open(file_path, encoding="utf8").read()

            if text is not None or text != "":
                hash_id = str(os.path.basename(file_path)).split(".")[0]
                document_dict, paragraph_dict_list = extract_data(source_id, source_name, hash_id, file_path,
                                                                  excel_file_dictionary, affect_data_dictionary, text)
                document_ingest_list.append(document_dict)
                paragraph_ingest_list += paragraph_dict_list
            if document_ingest_list.__len__() == CONFIG["add_bulk_size"]:
                ingest_data_to_elastic(document_ingest_list, paragraph_ingest_list)
                document_ingest_list = []
                paragraph_ingest_list = []

        except Exception as e:
            print(file_path, "\nError:", e)

    # insert last data to indexes
    ingest_data_to_elastic(document_ingest_list, paragraph_ingest_list)

    end_time = time.time()

    print('Documents & Paragraphs indices created (' + str(end_time - start_time) + ').')


# data functions
def extract_data(source_id, source_name, hash_id, file_path, excel_file_dict, affect_data_dict, document_text):

    document_dict = {"_id": hash_id, "file_path": file_path, "source_id": source_id, "source_name": source_name,
                     "content": document_text, "type": "نامشخص", "level": "نامشخص",
                     "keyword_subjects": {}, "keyword_main_subject": "", "keyword_subjects_words": {}, "actors": {},
                     "main_subject": "", "subjects": {}}

    # add static data to document dict
    static_data = excel_file_dict[hash_id]
    affect_data = affect_data_dict[hash_id]
    del static_data["_id"]
    for key, value in static_data.items():
        document_dict[key] = value
    for key, value in affect_data.items():
        document_dict[key] = value

    # for paragraph create paragraph_dict
    paragraph_list = document_text.split("\n")
    paragraph_dict_list = []
    paragraph_cntr = 1
    document_subject_list = []
    for paragraph_text in paragraph_list:
        if text_abs_length(paragraph_text) > 0:
            # init paragraph dict
            paragraph_dict = {"_id": str(document_dict["_id"]) + "_" + str(paragraph_cntr),
                              "document_id": document_dict["_id"], "document_name": document_dict["name"],
                              "document_source_id": document_dict["source_id"],
                              "document_source_name": document_dict["source_name"],
                              "document_datetime": document_dict["datetime"],
                              "document_last_status": document_dict["last_status"],
                              "document_category": document_dict["category"], "paragraph_number": paragraph_cntr,
                              "content": paragraph_text, "document_type": "نامشخص", "document_level": "نامشخص",
                              "keyword_subjects": {}, "keyword_main_subject": "", "keyword_subjects_words": {},
                              "document_keyword_main_subject": "", "sentiment": "", "main_subject": "", "subjects": {},
                              "entities": [{}]}

            if CONFIG["run_sentiment_module"]:
                sentiment = extract_sentiment(paragraph_text)
                paragraph_dict["sentiment"] = sentiment

            if CONFIG["run_subject_module"]:
                main_subject_dict, all_subjects_dict = extract_subject(paragraph_text)
                paragraph_dict["main_subject"] = main_subject_dict
                paragraph_dict["subjects"] = all_subjects_dict
                document_subject_list.append(all_subjects_dict)

            if CONFIG["run_entity_module"]:
                all_entities = extract_entity(paragraph_text)
                paragraph_dict["entities"] = all_entities

            # ingest paragraph_dict to paragraph_dict_list
            paragraph_dict_list.append(paragraph_dict)

            # increase paragraph_cntr
            paragraph_cntr += 1

    # document subject calculate
    if CONFIG["run_subject_module"]:
        document_subject_dict, document_main_subject = dictionary_list_averager(document_subject_list)
        document_dict["main_subject"] = document_main_subject
        document_dict["subjects"] = document_subject_dict

    for i in range(paragraph_dict_list.__len__()):
        paragraph_dict_list[i]["document_main_subject"] = document_dict["main_subject"]

    return document_dict, paragraph_dict_list


def numbers_preprocessing(text):
    persian_numbers = ['۰', '۱', '۲', '۳', '۴', '۵', '۶', '۷', '۸', '۹']
    arabic_numbers = ['٠', '١', '٢', '٣', '٤', '٥', '٦', '٧', '٨', '٩']
    for num in persian_numbers:
        text = text.replace(num, str(ord(num) - 1776))
    for num in arabic_numbers:
        text = text.replace(num, str(ord(num) - 1632))
    return text


def arabic_char_preprocessing(text):
    arabic_char = {"آ": "ا", "أ": "ا", "إ": "ا", "ي": "ی", "ئ": "ی", "ة": "ه", "ۀ": "ه", "ك": "ک", "َ": "", "ُ": "", "ِ": "",
                   "": ""}
    for key, value in arabic_char.items():
        text = text.replace(key, value)

    return text


def date_time_standardization(date_value, time_value):
    try:
        date_value = numbers_preprocessing(date_value.replace(" ", "").replace("/", "-"))
        year, month, day = map(int, date_value.split('-'))
        date_obj = JalaliDate(year, month, day)
        day_name = date_obj.strftime("%A", locale='fa')
        month_name = date_obj.strftime("%B", locale='fa')

        date_time_result_dict = {
            "year": year,
            "month": {"number": month, "name": month_name},
            "day": {"number": day, "name": day_name}
        }

        if time_value is not None:
            try:
                time_value = numbers_preprocessing(str(time_value).replace(" ", ""))
                hour = int(time_value.split(":")[0])
                minute = int(time_value.split(":")[1])
                date_time_result_dict["hour"] = hour
                date_time_result_dict["minute"] = minute
            except:
                date_time_result_dict["hour"] = 0
                date_time_result_dict["minute"] = 0

        return date_time_result_dict

    except Exception as e:
        return {
            "year": 0,
            "month": {"number": 0, "name": "نامشخص"},
            "day": {"number": 0, "name": "نامشخص"}
        }


def excel_to_dict(excel_path, field_names_list):
    excel_file_path = excel_path
    excel_dataframe = pd.read_excel(excel_file_path)
    affect_data_dict = excel_affect_relation_data_to_dict(excel_dataframe.copy(), field_names_list.copy())

    excel_dataframe = excel_dataframe.fillna("نامشخص")

    excel_file_dict = {}
    category_values = ["مصوبات مجلس شورا",
                       "مجلس شورای اسلامی",
                       "مجلس شورای ملی",
                       ]
    for i in range(category_values.__len__()):
        category_values[i] = arabic_char_preprocessing(category_values[i])

    for index, row in excel_dataframe.iterrows():
        temp_dict = {}

        for key, value in field_names_list.items():
            if key == "category":
                category_val = arabic_char_preprocessing(row[value])
                if category_val in category_values:
                    temp_dict[key] = "مجلس شورا"
                else:
                    temp_dict[key] = category_val
            elif key not in ("date", "time", "download_datetime", "affect_relation_data", "extra_data"):
                temp_dict[key] = row[value]

        temp_dict["download_datetime"] = UPLOAD_DATE

        date_value = row[field_names_list["date"]]

        time_value = None
        if field_names_list["time"] != "" or field_names_list["time"] is not None:
            time_value = row[field_names_list["time"]]

        temp_dict["datetime"] = date_time_standardization(date_value, time_value)

        excel_file_dict[str(temp_dict["_id"])] = temp_dict

        temp_dict["last_status"] = "معتبر"
        for extra_key in field_names_list["extra_data"]:
            temp_dict["extra_data"] = [
                {
                    'name': extra_key,
                    'value': row[extra_key]
                }
            ]
            if extra_key == "akharin_vaziat":
                temp_dict["last_status"] = row[extra_key].replace("نامشخص", "معتبر")

    return excel_file_dict, affect_data_dict


def extract_excel_affect_related_data(excel_dataframe, field_names_list):
    excel_file_dict = {}
    id_field_name = field_names_list["_id"]
    field_names_list = field_names_list["affect_relation_data"]
    for index, row in excel_dataframe.iterrows():
        temp_dict = {}
        doc_id = row[id_field_name]
        try:
            affected_by_value = str(row[field_names_list["affected_by"]])
            affected_by_list = affected_by_value.split("|")
            temp_dict["affected_by"] = affected_by_list
        except Exception as e:
            temp_dict["affected_by"] = None

        try:
            affecting_value = str(row[field_names_list["affecting"]])
            affecting_list = affecting_value.split("|")
            temp_dict["affecting"] = affecting_list
        except Exception as e:
            temp_dict["affecting"] = None

        try:
            regulation_related_value = str(row[field_names_list["regulation_related"]])
            regulation_related_list = regulation_related_value.split("|")
            temp_dict["regulation_related"] = regulation_related_list
        except Exception as e:
            temp_dict["regulation_related"] = None

        try:
            rules_related_value = str(row[field_names_list["rules_related"]])
            rules_related_list = rules_related_value.split("|")
            temp_dict["rules_related"] = rules_related_list
        except Exception as e:
            temp_dict["rules_related"] = None

        excel_file_dict[str(doc_id)] = temp_dict
    return excel_file_dict


def excel_affect_relation_data_to_dict(excel_dataframe, field_names_list):
    PID_DATA_DICT = {}
    for index, row in excel_dataframe.iterrows():
        pid = int(row[field_names_list["affect_relation_data"]["pid"]])
        hashed_file_name = row[field_names_list["_id"]]
        PID_DATA_DICT[pid] = hashed_file_name

    excel_affect_relation_data = extract_excel_affect_related_data(excel_dataframe, field_names_list)
    result_excel_relation_affect_data = {}
    for doc, ex_data in excel_affect_relation_data.items():
        try:
            # add from excel
            affected_by_list = []
            try:
                for affected_by in ex_data['affected_by']:
                    try:
                        affected_by = int(affected_by)
                        affected_by_id = PID_DATA_DICT[affected_by]
                        affected_by_list.append(affected_by_id)
                    except:
                        continue
            except Exception as e:
                pass

            affecting_list = []
            try:
                for affecting in ex_data['affecting']:
                    try:
                        affecting = int(affecting)
                        affecting_id = PID_DATA_DICT[affecting]
                        affecting_list.append(affecting_id)
                    except:
                        continue
            except Exception as e:
                pass

            regulation_related_list = []
            try:
                for reg in ex_data['regulation_related']:
                    try:
                        reg = int(reg)
                        reg_id = PID_DATA_DICT[reg]
                        regulation_related_list.append(reg_id)
                    except:
                        continue
            except Exception as e:
                pass

            rules_related_list = []
            try:
                for rule in ex_data['rules_related']:
                    try:
                        rule = int(rule)
                        rule_id = PID_DATA_DICT[rule]
                        rules_related_list.append(rule_id)
                    except:
                        continue
            except Exception as e:
                pass

            result_excel_relation_affect_data[doc] = {"affected_by": affected_by_list, 'affecting': affecting_list, "affect_detail": [],
                                             "regulation_related": regulation_related_list, "rules_related": rules_related_list}

        except Exception as e:
            pass

    return result_excel_relation_affect_data


def extract_subject(text):
    try:
        final_result = huggingface.classificationSentencePipeline(text)[0]
    except:
        window_size = 250
        text_parts = text.split(".")
        result = []
        counter = 0
        while counter < len(text_parts):
            my_text = text_parts[counter] + "."
            for j in range(counter + 1, len(text_parts)):
                new_text = text_parts[j]
                if len(my_text.split(" ")) + len(new_text.split(" ")) <= window_size:
                    my_text = my_text + new_text + "."
                else:
                    counter = j - 1
                    break
            else:
                counter = len(text_parts) - 1
            try:
                output = huggingface.classificationSentencePipeline(my_text)
            except:
                return []

            result.extend(output)
            counter = counter + 1

        final_result = []
        for i in range(8):
            label = result[0][i]['label']
            score = 0
            for j in range(len(result)):
                array = result[j]
                for obj in array:
                    if obj['label'] == label:
                        score += obj['score']
                        break
            final_result.append({'label': label, 'score': score / len(result)})

    all_subjects_dict = []
    main_subject = "نامشخص"
    main_subject_score = 0
    for subject in final_result:
        subject_label = subject["label"]
        subject_score = subject["score"]

        all_subjects_dict.append({"name": subject_label,
                                  "value": float(subject_score)})

        if subject_score > main_subject_score:
            main_subject = subject_label
            main_subject_score = subject_score

    main_subject_dict = main_subject

    return main_subject_dict, all_subjects_dict


def extract_sentiment(text):
    input_ids = huggingface.sentimentAnalyserTokenizer.encode(text, return_tensors="pt")
    generate_value = huggingface.sentimentAnalyserModel.generate(input_ids)
    sentiment_result = huggingface.sentimentAnalyserTokenizer.batch_decode(generate_value, skip_special_tokens=True)[0]
    if sentiment_result == "mixed":
        return "احساس ترکیبی از مثبت و منفی"
    elif sentiment_result == "neutral":
        return "احساس خنثی"
    elif sentiment_result == "borderline":
        return "احساس میان مثبت و منفی"
    elif sentiment_result == "no sentiment expressed":
        return "بدون ابراز احساسات"
    elif sentiment_result == "very positive":
        return 'احساس بسیار مثبت'
    elif sentiment_result == "positive":
        return 'احساس مثبت'
    elif sentiment_result == "very negative":
        return 'احساس بسیار منفی'
    elif sentiment_result == "negative":
        return 'احساس منفی'
    else:
        return 'نامشخص'


def extract_entity(text):
    try:
        final_result = huggingface.taggingSentencePipeline(text)
    except:
        window_size = 250
        text_parts = text.split(".")
        final_result = []
        counter = 0
        while counter < len(text_parts):
            my_text = text_parts[counter] + "."
            current_counter = counter
            for j in range(counter + 1, len(text_parts)):
                new_text = text_parts[j]
                if len(my_text.split(" ")) + len(new_text.split(" ")) <= window_size:
                    my_text = my_text + new_text + "."
                else:
                    counter = j - 1
                    break
            else:
                counter = len(text_parts) - 1
            try:
                output = huggingface.taggingSentencePipeline(my_text)
            except:
                output = "ERROR"
                return {"result": output}
            char_count = 0
            for i in range(current_counter):
                char_count = char_count + len(text_parts[i]) + 1
            for item in output:
                item['start'] += char_count
                item['end'] += char_count
            final_result.extend(output)
            counter = counter + 1

    final_result.sort(key=sort_json)

    persons_list = []
    locations_list = []
    organizations_list = []
    dates_list = []
    money_list = []
    times_list = []
    percent_list = []
    facilities_list = []
    products_list = []
    events_list = []

    for i in range(len(final_result)):
        item_object = final_result[i]

        item_object_start = item_object['entity'].split("-")[0]
        item_object_end = item_object['entity'].split("-")[1]
        if item_object_start == "I":
            continue

        if item_object_end not in ["person", "location", "organization", "date", "time", "money", "percent", "facility",
                                   "product", "event"]:
            continue

        end_word_index = item_object['end']
        start_word_index = item_object['start']

        for j in range(i + 1, len(final_result)):
            object_i = final_result[j]

            if object_i['entity'][0] == "B":
                break

            entity_i_object = object_i['entity'].split("-")[1]
            if entity_i_object != item_object_end:
                final_result[j]['entity'] = final_result[j]['entity'].replace("I", "B")
                break

            if end_word_index + 10 < object_i['start']:
                final_result[j]['entity'] = final_result[j]['entity'].replace("I", "B")
                break

            end_word_index = object_i['end']

        word = text[start_word_index:end_word_index]
        result_item = {'word': word, 'start': start_word_index, 'end': end_word_index}

        item_object_entity = item_object['entity'].split("-")[1]
        if item_object_entity == "person":
            persons_list.append(result_item)
        elif item_object_entity == "location":
            locations_list.append(result_item)
        elif item_object_entity == "organization":
            organizations_list.append(result_item)
        elif item_object_entity == "date":
            dates_list.append(result_item)
        elif item_object_entity == "time":
            times_list.append(result_item)
        elif item_object_entity == "money":
            money_list.append(result_item)
        elif item_object_entity == "percent":
            percent_list.append(result_item)
        elif item_object_entity == "facility":
            facilities_list.append(result_item)
        elif item_object_entity == "product":
            products_list.append(result_item)
        elif item_object_entity == "event":
            events_list.append(result_item)

    return {'persons': persons_list, 'locations': locations_list,
            'organizations': organizations_list, 'dates': dates_list,
            'times': times_list, 'money': money_list,
            'percents': percent_list, 'facilities': facilities_list,
            'products': products_list, 'events': events_list}


# Others
def sort_json(item):
    return item['start']
