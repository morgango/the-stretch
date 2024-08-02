from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic

from utils import query_elastic_by_single_field, get_elastic_client, display_results

elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')

elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)

from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic
import re


from utils import query_elastic_by_multiple_fields, get_elastic_client, build_search_metadata,add_to_search_history

# get the environment variables
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')

page_title = "Hybrid Search"
st.title(page_title)
st.session_state.current_page = page_title
suggestion_fields = []

if 'previous_page' not in st.session_state:
    st.session_state.previous_page = None

# get the elastic client
elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)


def build_fields_list(index_name :str, 
                      included_types=['text', 'sparse_vector', 'dense_vector'], 
                      excluded_fields=[], 
                      client = elastic_client):
    """
    Build a list of fields and their types from an index

    Args:
        index_name: the name of the index to get the fields from
        included_types: list of types to include
        excluded_fields: list of fields to exclude
        client: the elastic client to use
    
    Returns:
        sorted_fields: a list of tuples with the field name and type
    """

    mappings = client.indices.get_mapping(index=index_name)

    # Extract the field names and types
    fields = mappings[index_name]['mappings']['properties']
    field_types = {field: properties.get('type', 'unknown') for field, properties in fields.items() if field not in excluded_fields and properties.get('type', 'unknown') in included_types}

    # Sort the fields
    sorted_fields = sorted(field_types.items(), key=lambda x: x[0])

    return sorted_fields

def build_query_from_checkbox(status: dict,
                              fields: List[str]):
    
    f = dict(fields)

    text_fields = []
    sparse_vector_fields = []
    dense_vector_fields = []

    query = {}

    # categorize all the fields and their types.
    for item in status:
        if status[item]:
            if f[item] == 'text':
                text_fields.append(item)
            if f[item]  == 'sparse_vector':
                sparse_vector_fields.append(item)
            if f[item] == 'dense_vector':
                dense_vector_fields.append(item)

    text_field_query = {}
    sparse_field_query = {}   
    dense_field_query = {}   

    if text_fields:

        # put all the fields into a single multi_match query
        text_field_query = {
                "standard": {
                    "query": {
                        "multi_match": {
                            "query": "{query}",
                            "fields": text_fields
                        }
                    }
                }
            }

    if sparse_vector_fields:
        # build the shell of the query
        sparse_field_query = {
            "standard": {
                "query": {
                    }
                }
            }

        # add in the details for each sparse vector feild
        for field in sparse_vector_fields:
            sparse_field_query["standard"]["query"][field] = {}
            sparse_field_query["standard"]["query"][field]["model_id"] = ".elser_model_2_linux-x86_64"
            sparse_field_query["standard"]["query"][field]["model_text"] = "{query}"
   
    if dense_vector_fields:
          
        # build the shell of the query
        dense_vectory_query = {
          "knn": {
            "field": "my_embeddings.predicted_value",
            "k": 10,
            "num_candidates": 100,
            "query_vector_builder": {
                "text_embedding": {
                    "model_id": "sentence-transformers__msmarco-minilm-l-12-v3",
                    "model_text": "{query}"
                }
                }
            }
        }
 
    ic(text_fields, sparse_vector_fields, dense_vector_fields, text_field_query, sparse_field_query)

def hybrid_elastic(searchterm: str, 
                     field_names: List[str] = suggestion_fields,
                     display_field_name="text") -> List[Any]:

    index_field_names = field_names
    excluded_fields = ['_index', '_id', 'text', 'heading', 'text_synonym', 'text_sparse_embedding','model_id']
    search_type = "match"

    hits, query = query_elastic_by_multiple_fields(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_names=index_field_names,
                                  search_type=search_type,
                                  client=elastic_client)

    text_values = [suggestion['_source']['text'] for suggestion in hits]
    
    m = build_search_metadata(text_values,
                              searchterm,
                              search_type,
                              index_field_names,
                              display_field_name,
                              hits,
                              excluded_fields, 
                              query=query)
    
    add_to_search_history(m)

    return text_values

sorted_fields = build_fields_list(index_name=elastic_index_name, 
                                  included_types=['text', 'sparse_vector', 'dense_vector'],
                                  excluded_fields=['_index', '_id', 'heading_completion','text_completion','model_id'],
                                  client=elastic_client)

st.header("Fields to use")
checkbox_status = {field: st.checkbox(f'{field} ({field_type})') for field, field_type in sorted_fields}

build_query_from_checkbox(status=checkbox_status,
                          fields=sorted_fields)

results = st_searchbox(
    hybrid_elastic,
    key=page_title,
    label=page_title,
    clear_on_submit=True,
    default_use_searchterm=True,
    rerun_on_update=True,
)

display_results(page_title, results=results)

st.session_state.previous_page = page_title