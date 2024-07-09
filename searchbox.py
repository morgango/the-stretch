from elasticsearch import Elasticsearch
import wikipedia

from typing import Any, List
from decouple import config
from icecream import ic

from streamlit_searchbox import st_searchbox


elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')

def query_elastic_by_field(searchterm: str, 
                  index_name=elastic_index_name, 
                  field_name="",
                  cloud_id=elastic_cloud_id, 
                  api_key=elastic_api_key) -> List[Any]:
    
    client = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key
    )

    search_body = {
        "query": {
            "match": { 
                field_name : {
                    "query": searchterm
                }
            }
        }
    }

    response = client.search(index=index_name, body=search_body)
    hits = response['hits']['hits']

    return hits

def search_elastic(searchterm: str) -> List[Any]:

    index_field_name = "text"

    hits = query_elastic_by_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  cloud_id=elastic_cloud_id,
                                  api_key=elastic_api_key)

    ic(hits)

    text_values = [hit['_source']['text'] for hit in hits if '_source' in hit and 'text' in hit['_source']]

    return text_values

def suggest_elastic(searchterm: str) -> List[Any]:

    index_field_name = "text_completion"

    suggestions = query_elastic_by_field(searchterm, 
                                        index_name=elastic_index_name, 
                                        field_name=index_field_name,
                                        cloud_id=elastic_cloud_id,
                                        api_key=elastic_api_key)

    text_values = [suggestion['_source']['text'] for suggestion in suggestions]

    return text_values

# function with list of labels
def search_wikipedia(searchterm: str) -> List[any]:
    return wikipedia.search(searchterm) if searchterm else []

# def search_elastic(searchterm: str) -> List[Any]:

#     cloud_id = elastic_cloud_id
#     api_key = elastic_api_key
    
#     client = Elasticsearch(
#         cloud_id=cloud_id,
#         api_key=api_key
#     )
    
#     search_body = {
#         "query": {
#             "match": { 
#                 "text": {
#                     "query": searchterm
#                 }
#             }
#         }
#     }
    
#     response = client.search(index=elastic_index_name, body=search_body)
#     hits = response['hits']['hits']

#     ic(hits)

#     text_values = [hit['_source']['text'] for hit in hits if '_source' in hit and 'text' in hit['_source']]

#     ic(elastic_index_name, search_body, text_values)
#     return text_values

# def suggest_elastic(searchterm: str) -> List[Any]:

#     cloud_id = elastic_cloud_id
#     api_key = elastic_api_key
    
#     client = Elasticsearch(
#         cloud_id=cloud_id,
#         api_key=api_key
#     )

#     search_body = {
#         "query": {
#             "match": { 
#                 "text_completion": {
#                     "query": searchterm
#                 }
#             }
#         }
#     }

#     response = client.search(index=elastic_index_name, body=search_body)
#     suggestions = response['hits']['hits']

#     text_values = [suggestion['_source']['text'] for suggestion in suggestions]

#     ic(elastic_index_name, search_body, text_values)
#     return text_values

# pass search function to searchbox
selected_value = st_searchbox(
    suggest_elastic,
    key="elastic_searchbox",
)

# pass search function to searchbox
selected_value = st_searchbox(
    search_wikipedia,
    key="wiki_searchbox",
)