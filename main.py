from utils import zip_extractor
from input_configs import *
from scripts import source_to_elastic, ingest_data_to_elastic, ingest_type_to_elastic, ingest_level_to_elastic


print("------------------------------------------\nExtract ZIP File\n------------------------------------------")
zip_extractor.extractor(ZIP_FILE_PATH, PATH_TO_EXTRACT_FILES)

print("------------------------------------------\nSource To Elastic\n------------------------------------------")
source_to_elastic.apply()

print("------------------------------------------\nData To Elastic\n------------------------------------------")
ingest_data_to_elastic.apply()

print("------------------------------------------\nType To Elastic\n------------------------------------------")
ingest_type_to_elastic.apply()

print("------------------------------------------\nLevel To Elastic\n------------------------------------------")
ingest_level_to_elastic.apply()
