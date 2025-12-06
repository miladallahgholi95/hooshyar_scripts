import math
from elastic.connection import SEARCH_WINDOW_SIZE, ES_CLIENT, IndexObjectWithId, BUCKET_SIZE
from elastic.SETTINGS import PARAGRAPH_SETTING
from elastic.MAPPINGS import PARAGRAPH_ACTOR_MAPPING, ACTORS_MAPPING, REGULATORS_MAPPING, PARAGRAPH_MAPPING

import pandas as pd


def apply():
    create_actor_time_series_data()
    create_actor_correlation_data()



def get_all_actors_list():
    res_query = {
        "match_all":{}
    }
    response = ES_CLIENT.search(
        index=ACTORS_MAPPING.NAME,
        query=res_query,
        size = 10000
    )
    actor_list = response['hits']['hits']
    return actor_list


def get_doc_years_list():
    year_bucket_list = []
    res_agg = {
        "year_agg":{
            "terms": {
                "field": "document_datetime.year",
                "size": BUCKET_SIZE
            }
        }
    }

    response = ES_CLIENT.search(
        index=PARAGRAPH_ACTOR_MAPPING.NAME,
        query={
            "match_all": {}
        },
        aggregations=res_agg,
        size = 1000
    )

    year_buckets = response['aggregations']['year_agg']['buckets']

    for bucket in year_buckets:
        year_bucket_list.append(bucket['key'])

    return year_bucket_list

def get_year_bucket_list(actor_name,role_name):
    res_query = {
    "nested": {
      "path": "actors",
      "query": {
          "bool":{
              "filter":[
                  
                  {
                      "term":{
                          "actors.name.keyword":actor_name
                      }
                  }
              ]
          }
      }
    }
    }

    if role_name != "همه":
        role_filter = {
            "term":{
                "actors.role":role_name
            }
        }
        res_query['nested']['query']['bool']['filter'].append(role_filter)

    res_agg = {
        "year_agg":{
            "terms": {
                "field": "document_datetime.year",
                "size": BUCKET_SIZE
            }
        }
    }



    response = ES_CLIENT.search(
        index=PARAGRAPH_ACTOR_MAPPING.NAME,
        query=res_query,
        aggregations=res_agg,
        size = 0
    )

    year_buckets = response['aggregations']['year_agg']['buckets']
    return year_buckets

def update_year_dict(year_dict,bucket_list):
    for bucket in bucket_list:
        year_dict[bucket['key']] = bucket['doc_count']

def convert_to_series_data(year_dict):
    series_data = []
    for year,count in year_dict.items():
        series_data.append(
            {"year":year,"count":count}
        )
    return series_data


def create_actor_time_series_data():    
    # get all actors
    actors_list = get_all_actors_list()
    year_list = sorted(get_doc_years_list())

    actor_year_dict = {}
    role_names = ['متولی اجرا','همکار','دارای صلاحیت اختیاری','همه']
    

    # create source_series_data for each actor
    c = 0
    for actor in actors_list[:]:
        c+= 1
        print(f"source_series_data {c}/{len(actors_list)}")

        actor['_source']['source_series_data'] = []

        for role_name in role_names:
            actor_year_dict = dict.fromkeys(year_list,0)
            bucket_list = get_year_bucket_list(actor['_source']['name'],role_name)

            update_year_dict(actor_year_dict,bucket_list)
            source_series_data = {
                'source_id':"1",
                'role_name':role_name,
                'series_data':convert_to_series_data(actor_year_dict)
            }
            if source_series_data not in actor['_source']['source_series_data']:
                actor['_source']['source_series_data'].append(source_series_data)
                

    actor_index = IndexObjectWithId(ACTORS_MAPPING.NAME,
                             PARAGRAPH_SETTING.SETTING,
                             ACTORS_MAPPING.MAPPING)
    
    # delete index
    actor_index.delete_index()
    
    # create index
    actor_index.create()
    
    actor_index.bulk_insert_documents(actors_list)
    print('Actors updated.')
    


def create_actor_correlation_data():
    # get all actors
    actors_list = get_all_actors_list()

    role_names = ['متولی اجرا','همکار','دارای صلاحیت اختیاری','همه']
                
    # calculate correlation between actors
    actors_dict = {actor['_id']: actor for actor in actors_list}
    c = 0
    for actor in actors_list[:]:
        c+= 1
        print(f"correlation {c}/{len(actors_list)}")
        actor['_source']['source_correlation_data'] = []
        for role_name in role_names:
            correlation_result = get_correlation(actor['_id'], role_name, actors_dict)
            actor['_source']['source_correlation_data'].append({
                'source_id': "1",
                'role_name': role_name,
                'correlation_data': correlation_result
            })
                

    actor_index = IndexObjectWithId(ACTORS_MAPPING.NAME,
                             PARAGRAPH_SETTING.SETTING,
                             ACTORS_MAPPING.NAME)
    
    # delete index
    actor_index.delete_index()
    
    # create index
    actor_index.create()
    
    actor_index.bulk_insert_documents(actors_list)
    print('Actors updated.')



def get_correlation(requested_actor_id, role_name, actors):
    all_actors = {}
    correlation_result = []

    # Get data for the current actor
    curr_actor = None
    
    for actor_id, actor in actors.items():
        # Get actor name
        actor_name = actor['_source']['name']

        # Get source series data for the actor
        source_series_data = actor['_source']['source_series_data']
        
        # Get source series data for the requested role
        role_data = None
        for role in source_series_data:
            if role['role_name'] == role_name:
                role_data = role['series_data']
                break
        
        # Process data for role
        series_data = {year["year"]: year["count"] for year in role_data}
        
        # Get count series for the role
        value_list = list(series_data.values())[1:]
        actor_year_count = pd.Series(value_list)
        
        # Store actor data in the dictionary of all actors
        all_actors[actor_id] = {
            "actor_name": actor_name,
            "year_count": actor_year_count,
        }
         

    curr_actor = all_actors[requested_actor_id]

    # Calculate correlation with other actors for each role
    for other_actor_id, other_actor in all_actors.items():
        if other_actor_id != requested_actor_id:
            actor_year_count = curr_actor["year_count"]
            other_actor_year_count = other_actor["year_count"]
            correlation_value = round(actor_year_count.corr(other_actor_year_count), 2)

            # Store the correlation value in the correlation result dictionary
            correlation_value = float(correlation_value) if not math.isnan(correlation_value) else 0
            if correlation_value != 0:
                correlation_result.append({
                    "actor_id": other_actor_id,
                    "actor_name": other_actor["actor_name"],
                    "correlation_value": correlation_value,
                })
           
    return correlation_result