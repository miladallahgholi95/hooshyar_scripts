from utils import zip_extractor
from input_configs import *
from scripts import (source_to_elastic, ingest_data_to_elastic, ingest_type_to_elastic, ingest_level_to_elastic,
                     ingest_subject_keyword_to_elastic, ingest_all_actors_to_elastic, ingest_affected_graph_to_elastic,
                     ingest_references_to_elastic, ingest_clustering_to_elastic, ingest_actors_to_elastic, ingest_vectors_to_elastic)


# print("------------------------------------------\nExtract ZIP File\n------------------------------------------")
# zip_extractor.extractor(ZIP_FILE_PATH, PATH_TO_EXTRACT_FILES)
#
# print("------------------------------------------\nSource To Elastic\n------------------------------------------")
# source_to_elastic.apply()
#
# print("------------------------------------------\nActors To Elastic\n------------------------------------------")
# ingest_actors_to_elastic.apply()
# #
# print("------------------------------------------\nData To Elastic\n------------------------------------------")
# objects_ids = ingest_data_to_elastic.apply()

objects_ids = None

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
# print("------------------------------------------\nAffected Graph To Elastic\n------------------------------------------")
# ingest_affected_graph_to_elastic.apply(objects_ids)

print("------------------------------------------\nReferences To Elastic\n------------------------------------------")
ingest_references_to_elastic.apply()

print("------------------------------------------\nClustering To Elastic\n------------------------------------------")
ingest_clustering_to_elastic.apply()

print("------------------------------------------\nVectors To Elastic\n------------------------------------------")
ingest_vectors_to_elastic.apply(objects_ids)
