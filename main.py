# import input_configs
# from utils import zip_extractor
# from input_configs import *
from scripts import  ingest_paragraph_actors_to_elastic
# from utils.balebot import send_bale_message
# import traceback

# error_message = None
# MESSAGE = f"سلام وقت بخیر\n**اسناد هوشیار و هوشیار سرویس آپدیت شدند.**\n**تاریخ آخرین خزش:** \n{UPLOAD_DATE}"
# try:
    # print("------------------------------------------\nExtract ZIP File\n------------------------------------------")
    # zip_extractor.extractor(ZIP_FILE_PATH, PATH_TO_EXTRACT_FILES)
    #
    # print("------------------------------------------\nSource To Elastic\n------------------------------------------")
    # source_to_elastic.apply()
    #
    # print("------------------------------------------\nActors To Elastic\n------------------------------------------")
    # ingest_actors_to_elastic.apply()
    #
    # print("------------------------------------------\nData To Elastic\n------------------------------------------")
    # objects_ids = ingest_data_to_elastic.apply()
    #
    # print("------------------------------------------\nType To Elastic\n------------------------------------------")
    # ingest_type_to_elastic.apply(objects_ids)
    #
    # print("------------------------------------------\nLevel To Elastic\n------------------------------------------")
    # ingest_level_to_elastic.apply(objects_ids)
    #
    # print("------------------------------------------\nSubject To Elastic\n------------------------------------------")
    # ingest_subject_keyword_to_elastic.apply(objects_ids)
    #
    # print("------------------------------------------\nAll Actors To Elastic\n------------------------------------------")
    # ingest_all_actors_to_elastic.apply(objects_ids)
    #
    # # print("------------------------------------------\nAffected Data To Elastic\n------------------------------------------")
    # # ingest_affect_detail_to_elastic.apply(objects_ids)
    #
    # send_bale_message(MESSAGE , CyberMap_CHAT_ID)
    #
    # print("------------------------------------------\nAffected Graph To Elastic\n------------------------------------------")
    # ingest_affected_graph_to_elastic.apply(objects_ids)
    #
    # print(
    #     "------------------------------------------\nReferences To Elastic\n------------------------------------------")
    # ingest_references_to_elastic.apply()
    #
    # print("------------------------------------------\nClustering To Elastic\n------------------------------------------")
    # ingest_clustering_to_elastic.apply()
    #
    # # print("------------------------------------------\nVectors To Elastic\n------------------------------------------")
    # # ingest_vectors_to_elastic.apply(objects_ids)

ingest_paragraph_actors_to_elastic.apply()

# except Exception as e:
#     # error_message = f"خطا در اجرای اسکریپت:\n{traceback.format_exc()}"
#     # send_bale_message(error_message , Backup_CHAT_ID)
#     print()
