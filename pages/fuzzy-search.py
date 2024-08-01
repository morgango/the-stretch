from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic

from utils import query_elastic_by_single_field, get_elastic_client,flatten_hits, df_to_html, replace_with_highlight, build_search_metadata, add_to_search_history

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
    fields_to_drop = ['_index', '_id', 'text_completion', 'heading_completion', 'text_synonym', 'text_sparse_embedding','model_id']
    search_type = "match"

    hits = query_elastic_by_single_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type="fuzzy",
                                  fuzziness=2,
                                  client=elastic_client,
                                  highlight=True,
                                  fields_to_drop=fields_to_drop)

    text_values = [hit['_source'][index_field_name] for hit in hits if '_source' in hit and index_field_name in hit['_source']]
        
    m = build_search_metadata(text_values,
                              searchterm,
                              search_type,
                              index_field_name,
                              display_field_name,
                              hits,
                              fields_to_drop)
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

# we only want to generate results if we are typing on the page
if st.session_state.previous_page == page_title and \
    st.session_state.current_page == page_title and \
        results:

    ic(st.session_state.previous_page, results)
    # write the header
    if 'text_values' in st.session_state.search_last.keys():
        header = st.html(f"<h2>{st.session_state.search_last['search_term']}</h2>")

    # write the actual values, with some formatting
    if 'df_hits_html' in st.session_state.search_last.keys():
        table = st.html(st.session_state.search_last['df_hits_html'])

st.session_state.previous_page = page_title
