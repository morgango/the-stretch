from typing import Any, List
from streamlit_searchbox import st_searchbox
import streamlit as st
from typing import Any, List
from decouple import config
from icecream import ic

from utils import query_elastic_by_single_field, get_elastic_client, flatten_hits, df_to_html, build_search_metadata

# get the environment variables
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')

# get the elastic client
elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)


def semantic_elastic(searchterm: str, 
                     field_name = "text_sparse_embedding", 
                     display_field_name="text") -> List[Any]:

    index_field_name = field_name
    fields_to_drop = ['_index', '_id', 'text_synonym','model_id', 'text_completion', 'heading_completion']
    search_type = "semantic"

    hits = query_elastic_by_single_field(searchterm, 
                                  index_name=elastic_index_name, 
                                  field_name=index_field_name,
                                  search_type=search_type,
                                  client=elastic_client,
                                  fields_to_drop=fields_to_drop)

    text_values = [hit['_source'][display_field_name] for hit in hits if '_source' in hit and display_field_name in hit['_source']]

    m = build_search_metadata(text_values,
                              searchterm,
                              search_type,
                              index_field_name,
                              display_field_name,
                              hits,
                              fields_to_drop)
    st.session_state.search_metadatda = m

    return text_values

# pass search function to searchbox
results = st_searchbox(
    semantic_elastic,
    key="elastic_semanticbox",
    label="Semantic Search Elastic",
    clear_on_submit=True,
    rerun_on_update=True
)

if results:

    # write the header
    if 'text_values' in st.session_state.search_metadatda.keys():
        header = st.html(f"<h2>{st.session_state.search_metadatda['search_term']}</h2>")

    # write the actual values, with some formatting
    if 'df_hits_html' in st.session_state.search_metadatda.keys():
        table = st.html(st.session_state.search_metadatda['df_hits_html'])

    

    
 
