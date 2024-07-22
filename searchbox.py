from elasticsearch import Elasticsearch
import wikipedia

from typing import Any, List
from decouple import config
from icecream import ic

from streamlit_searchbox import st_searchbox


elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')

elastic_client = Elasticsearch(
    cloud_id=elastic_cloud_id,
    api_key=elastic_api_key,
)

def query_elastic_by_field(searchterm: str, 
                  index_name=elastic_index_name, 
                  field_name="",
                  search_type="match",
                  fuzziness: str = None,
                  client=elastic_client) -> List[Any]:

    query_body = {"query": {}}

    if search_type == "match":
        # using a match_phrase_prefix should allow this to use synonyms, but just prefixes
        query_body["query"]["match"] = {
            field_name : {
                "query": searchterm
            }
        }

    elif search_type == "fuzzy":
        query_body["query"]["fuzzy"] = {
            field_name : {
                "value": searchterm,
                "fuzziness": fuzziness if fuzziness else "AUTO"
            }
        }


    response = client.search(index=index_name, body=query_body)
    hits = response['hits']['hits']

    return hits

def search_elastic(searchterm: str) -> List[Any]:

    index_field_name = "text"

    hits = query_elastic_by_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type="match",
                                  client=elastic_client)

    ic(hits)

    text_values = [hit['_source'][index_field_name] for hit in hits if '_source' in hit and index_field_name in hit['_source']]

    return text_values


def suggest_elastic(searchterm: str, field_name = "text_completion") -> List[Any]:

    # index_field_name = "text_completion"
    index_field_name = field_name

    suggestions = query_elastic_by_field(searchterm, 
                                        index_name=elastic_index_name, 
                                        field_name=index_field_name,
                                        search_type="match",
                                        client=elastic_client)

    text_values = [suggestion['_source']['text'] for suggestion in suggestions]

    return text_values

def synonym_elastic(searchterm: str, field_name = "text_synonym") -> List[Any]:

    index_field_name = field_name

    hits = query_elastic_by_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type="match",
                                  client=elastic_client)

    text_values = [hit['_source'][index_field_name] for hit in hits if '_source' in hit and index_field_name in hit['_source']]

    return text_values


def fuzzy_elastic(searchterm: str) -> List[Any]:

    index_field_name = "text"

    hits = query_elastic_by_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type="fuzzy",
                                  fuzziness=2,
                                  client=elastic_client)

    ic(hits)

    text_values = [hit['_source'][index_field_name] for hit in hits if '_source' in hit and index_field_name in hit['_source']]

    return text_values



# function with list of labels
def search_wikipedia(searchterm: str) -> List[any]:
    return wikipedia.search(searchterm) if searchterm else []

# pass search function to searchbox
selected_value = st_searchbox(
    search_elastic,
    key="elastic_suggestbox",
    label="Search Elastic",
)

# pass search function to searchbox
selected_value = st_searchbox(
    fuzzy_elastic,
    key="elastic_fuzzybox",
    label="Search Elastic (fuzzy)",
)

# pass search function to searchbox
selected_value = st_searchbox(
    suggest_elastic,
    key="elastic_searchbox",
    label="Search Elastic (with suggestions)",
)

# pass search function to searchbox
selected_value = st_searchbox(
    synonym_elastic,
    key="elastic_synonymbox",
    label="Search Elastic (with synonyms)",
)

# pass search function to searchbox
selected_value = st_searchbox(
    search_wikipedia,
    key="wiki_searchbox",
)