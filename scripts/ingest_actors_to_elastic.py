import json
from elastic.connection import ESIndex, ES_CLIENT, SEARCH_WINDOW_SIZE, IndexObjectWithId
from elastic.MAPPINGS import ACTORS_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING


def ingest_documents(index_obj, documents):
    if ESIndex.CLIENT.indices.exists(index=index_obj.name):
        delete_query = {"query": {"match_all": {}}}

        delete_response = ES_CLIENT.delete_by_query(index=index_obj.name,
                                                    timeout="120s",
                                                    refresh=True,
                                                    body=delete_query)

        print(f"Documents in {index_obj.name} deleted:")
        print(delete_response)
        print("-----------------------------------------------------------------")

        # Check deletion:
        resource_doc_count = ES_CLIENT.count(index=index_obj.name,
                                             body=delete_query)['count']

        print(f"Documents count after deletion is: {resource_doc_count}")
        print("-----------------------------------------------------------------")

    else:
        index_obj.create()

    # ingest to index
    index_obj.bulk_insert_documents(documents)


def apply():
    result_ingest_list = []

    fullActorsInformationFile = "./other_files/full_actors_list.json"

    f = open(fullActorsInformationFile, encoding="utf-8")
    fullActorsInformation = json.load(f)
    f.close()

    index = 1
    for category in fullActorsInformation:

        actors = fullActorsInformation[category]

        for actor in actors:
            actor_form_list = actors[actor]
            actor_obj = {
                "_id": index,
                "name": actor,
                "category": category,
                "forms": actor_form_list,
                "source_series_data": [],
                "arima_prediction_data": []
            }
            result_ingest_list.append(actor_obj)
            index += 1

    actor_index = IndexObjectWithId(ACTORS_MAPPING.NAME,
                                    DOCUMENT_SETTING.SETTING,
                                    ACTORS_MAPPING.MAPPING)

    ingest_documents(actor_index, result_ingest_list)
    print('Actors added.')
