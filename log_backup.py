# from elastic.connection import *
# from docx import Document, enum
# from docx.shared import Pt
# from docx.enum.text import WD_ALIGN_PARAGRAPH
# from docx.shared import RGBColor
# import re
#
# es_client = Elasticsearch('http://127.0.0.1:5602', request_timeout=100)
#
#
# def get_user_dict(client):
#     idx_name = "irhooshyar_rahnemud_accounts_users_index"
#     name_field = "name"
#     user_dictionary = {}
#     query = {
#         "query": {
#             "match_all": {}
#         },
#         "_source": [name_field]
#     }
#
#     response_user = es_client.search(index=idx_name, body=query, size=5000)
#     hits = response_user["hits"]["hits"]
#
#     # Process initial batch
#     for hit in hits:
#         source = hit["_source"]
#         user_id = hit["_id"]
#         user_name = source.get(name_field)
#         if user_id and user_name:
#             user_dictionary[user_id] = user_name
#
#     return user_dictionary
#
#
# user_dict = get_user_dict(es_client)
#
# index_name = "irhooshyar_rahnemud_accounts_user_logs_index"
# res_query = {
#     "query": {
#         "bool": {
#             "filter": [
#                 {
#                     "range": {
#                         "submitted_at": {
#                             "gte": "2024-10-28T20:30:00",
#                             "lte": "2024-11-28T20:30:00"
#                         }
#                     }
#                 }
#             ]
#         }
#     },
#     "sort": [
#         {"submitted_at": {"order": "desc"}}
#     ]
# }
# response = es_client.search(
#     index=index_name,
#     body=res_query,
#     size=SEARCH_WINDOW_SIZE,
# )
#
# results = response['hits']['hits']
#
# final_result = []
# for row in results:
#     _source = row['_source']
#
#     user_id = _source["user"]
#     user_name = user_dict[user_id]
#     referrer = _source["referrer"]
#     url = _source["url"]
#     action = row["action"]


import requests
import json
import pandas as pd

url = "http://37.156.144.109:7106/accounts/logs/?source=__all__&submitted_from=1403-09-07&submitted_to=1403-09-10&user__source=name&user__source=username&page_size=1500&page=1"
jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzMzNjUwMTA1LCJpYXQiOjE3MzI3ODYxMDUsImp0aSI6ImEwNmMwN2ZiYTgzNjQ3YzQ5ZWE2NTIzNzdkMDMxNWU0IiwidXNlcl9pZCI6NX0.n6fkIkmFG_M0U3FMGvetM4miYB-ShHpSjstt9a0VYlo"
headers = {
    "Authorization": f"Bearer {jwt_token}"
}

response = requests.get(url=url, headers=headers)
response_json = response.json()['hits']

# Prepare data for saving
data_list = []
for row in response_json:
    data = {
        "user_id": row['user'].get("id", None),
        "user_name": row['user'].get("name", None),
        "user_ip": row["user_ip"],
        "submitted_at": row["submitted_at"],
        "action": row["action"],
        "panel": row["panel"],
        "referrer": row["referrer"],
        "url_path": row["url_path"],
        "method": row["method"],
        "response_status_code": row["response_status_code"],
        "host": row["host"],
        "url_params": row.get("url_params", None),
        "body_params": row.get("body_params", None),
        "request_params": row.get("request_params", None),
    }
    data_list.append(data)


print(data_list.__len__())
# Save to JSON file
with open("output_logs.json", "w", encoding="utf-8") as json_file:
    json.dump(data_list, json_file, ensure_ascii=False, indent=4)

# Save to Excel file
df = pd.DataFrame(data_list)
df.to_excel("output_logs.xlsx", index=False)

print("Data saved to output_logs.json and output_logs.xlsx")
