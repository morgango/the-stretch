from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic

from utils import query_elastic_by_single_field, get_elastic_client

elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')

elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)

def suggest_elastic(searchterm: str, field_name = "text_completion") -> List[Any]:

    index_field_name = field_name

    suggestions = query_elastic_by_single_field(searchterm, 
                                        index_name=elastic_index_name, 
                                        field_name=index_field_name,
                                        search_type="match",
                                        client=elastic_client,
                                        fields_to_drop=['_index', '_id', 'text_synonym','model_id', 'text_sparse_embedding'])

    text_values = [suggestion['_source']['text'] for suggestion in suggestions]
    ic(suggestions)

    st.session_state.searchterm = searchterm

    return text_values


# pass search function to searchbox
found_value = st_searchbox(
    suggest_elastic,
    key="elastic_synonymbox",
    label="Search Elastic (with synonyms)",
    clear_on_submit=True,
    default_use_searchterm=True,
    rerun_on_update=True
)

if found_value:
    st.html(f"<h2>{found_value}</h2>")


if 'df_hits' in st.session_state:
    st.html(st.session_state.hits)

