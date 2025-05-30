import os
import shutil
import jdatetime

def get_nearest_crawl_day():
    today = jdatetime.date.today()
    weekday = today.weekday()

    days_to_saturday = (weekday - 0) % 7 or 7 # Saturday
    days_to_monday = (weekday - 2) % 7 or 7 # Monday
    days_to_thursday = (weekday - 5) % 7 or 7 # Thursday

    if days_to_saturday <= days_to_monday and days_to_saturday <= days_to_thursday:
        nearest_day = today - jdatetime.timedelta(days=days_to_saturday)
    elif days_to_monday <= days_to_saturday and days_to_monday <= days_to_thursday:
        nearest_day = today - jdatetime.timedelta(days=days_to_monday)
    else:
        nearest_day = today - jdatetime.timedelta(days=days_to_thursday) + jdatetime.timedelta(days=1)

    return nearest_day.strftime("%Y-%m-%d")


BOT_TOKEN = "1740739661:5UGBO8BjgeZEOhBBp5iBNoljCry2GEamJmF1oSxd"
CyberMap_CHAT_ID = "4526484028"
Backup_CHAT_ID = "5659980398"
BaleURL = f"https://tapi.bale.ai/bot{BOT_TOKEN}/sendMessage"
parent_folder = r"/mnt/Data1/abdal_crawlers/crawler-qavanin-ir-service/files"
folders = [f for f in os.listdir(parent_folder) if os.path.isdir(os.path.join(parent_folder, f))]
last_folder = sorted(folders)[-1]
ZIP_FILE_PATH = os.path.join(parent_folder, last_folder, "files.zip")
EXCEL_FILE_PATH = os.path.join(parent_folder, last_folder, "data.xlsx")
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
                                    "tarikh_roozname_rasmi",
                                    # "ghazi",
                                    # "shobe",
                                    # "noe_rai",
                                    # "payam",
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

UPLOAD_DATE = get_nearest_crawl_day() #"1403-12-20" #jdatetime.date.today().strftime('%Y-%m-%d')

VECTOR_MODEL_PATH = "HooshvareLab/bert-base-parsbert-uncased" #"/mnt/Data1/law+LM_Pbert"

