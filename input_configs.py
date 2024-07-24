import os
import shutil
import jdatetime

ZIP_FILE_PATH = "/mnt/Data1/data/files.zip"
EXCEL_FILE_PATH = "/mnt/Data1/data/data.xlsx"
SOURCE_ID = 1
SOURCE_NAME = "مجموعه‌داده هوشیار"
SOURCE_LANGUAGE = "FA"
INPUT_EXCEL_MAPPING = {
                                "_id": "hashed_file_name",
                                "name": "name",
                                "category": "approve_by",
                                "date": "approve_date",
                                "time": "time",
                                "download_datetime": "download_datetime",
                                "source_url": "url",
                                "affect_relation_data": {
                                    "pid": "pid",
                                    "affected_by": "affected_by",
                                    "affecting": "affecting",
                                    "regulation_related": "regulation_related",
                                    "rules_related": "rules_related",
                                },
                                "extra_data": [
                                    "noe_ghanon",
                                    "tabaghe_bandi",
                                    "tarikh_tasvib",
                                    "shomare_tasvib",
                                    "shomare_eblagh",
                                    "tarikh_eblagh",
                                    "marjae_eblagh",
                                    "tarikh_ejra",
                                    "akharin_vaziat",
                                    "dastgah_mojri",
                                    "ronevesht",
                                    "shomare_roozname_rasmi",
                                    "tarikh_roozname_rasmi"
                                ]
                        }

PATH_TO_EXTRACT_FILES = "data_folder"
if os.path.exists(PATH_TO_EXTRACT_FILES) and os.path.isdir(PATH_TO_EXTRACT_FILES):
    shutil.rmtree(PATH_TO_EXTRACT_FILES)
os.mkdir(PATH_TO_EXTRACT_FILES)

RUN_SENTIMENT_MODULE = False
RUN_SUBJECT_MODULE = False
RUN_ENTITY_MODULE = False
RUN_VECTOR_MODULE = True


UPLOAD_DATE = jdatetime.date.today().strftime('%Y-%m-%d')

VECTOR_MODEL_PATH = "/mnt/Data1/hooshyar_scripts/law+LM_Pbert"
