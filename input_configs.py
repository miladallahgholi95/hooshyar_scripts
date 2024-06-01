import os
import shutil


ZIP_FILE_PATH = "data/files.zip"
EXCEL_FILE_PATH = "data/data.xlsx"
SOURCE_ID = 1
SOURCE_NAME = "مجموعه‌داده هوشیار"
SOURCE_LANGUAGE = "FA"

PATH_TO_EXTRACT_FILES = "data_folder"
if os.path.exists(PATH_TO_EXTRACT_FILES) and os.path.isdir(PATH_TO_EXTRACT_FILES):
    shutil.rmtree(PATH_TO_EXTRACT_FILES)
os.mkdir(PATH_TO_EXTRACT_FILES)

EXCEL_FILE_MAPPING = {}
RUN_SENTIMENT_MODULE = False
RUN_SUBJECT_MODULE = False
RUN_ENTITY_MODULE = False
RUN_VECTOR_MODULE = False
