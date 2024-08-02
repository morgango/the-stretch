from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic

from utils import query_elastic_by_single_field, get_elastic_client, build_search_metadata, add_to_search_history, display_results

elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')

page_title = "Fuzzy Search"

st.title(page_title)
st.session_state.current_page = page_title

if 'previous_page' not in st.session_state:
    st.session_state.previous_page = None

elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)

def fuzzy_elastic(searchterm: str, 
                     field_name = "text", 
                     display_field_name="text") -> List[Any]:

    index_field_name = field_name
    excluded_fields = ['_index', '_id', 'text_completion', 'heading_completion', 'text_synonym', 'text_sparse_embedding','model_id']
    search_type = "match"

    hits, query = query_elastic_by_single_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type="fuzzy",
                                  fuzziness=2,
                                  client=elastic_client,
                                  highlight=True)

    text_values = [hit['_source'][index_field_name] for hit in hits if '_source' in hit and index_field_name in hit['_source']]
        
    m = build_search_metadata(text_values,
                              searchterm,
                              search_type,
                              index_field_name,
                              display_field_name,
                              hits,
                              excluded_fields,
                              query=query)
    add_to_search_history(m)

    return text_values

# pass search function to searchbox
results = st_searchbox(
    fuzzy_elastic,
    key=page_title,
    label=page_title,
    clear_on_submit=True,
    default_use_searchterm=True,
    rerun_on_update=True,
)

display_results(page_title, results=results)

st.session_state.previous_page = page_title
