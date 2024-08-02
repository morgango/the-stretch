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

page_title = "Multi-Suggest Search"
st.title(page_title)
st.session_state.current_page = page_title
suggestion_fields = []

if 'previous_page' not in st.session_state:
    st.session_state.previous_page = None

# get the elastic client
elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, 
                                   api_key=elastic_api_key)

def check_fields(fields:List[str]):
    """
    Check if the fields are in the correct format

    Args:
        fields: list of fields to check

    Returns:
        None
    """
    # Pattern for a field name, a carat, and a number
    pattern = re.compile(r"^.+\^.+$")

    for field in fields:
        field = field.strip()

        if not pattern.match(field):
            st.error(f"Invalid input: {field}. Please make sure to enter a field name, a carat, and a number.")


def suggest_elastic(searchterm: str, 
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

fields_text = st.text_input("Fields to search", value="text_completion^3, heading_completion^5.5")
suggestion_fields = fields_text.split(',')

check_fields(suggestion_fields)

results = st_searchbox(
    suggest_elastic,
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

    ic('a',st.session_state.previous_page, results)
    # write the header
    if 'text_values' in st.session_state.search_last.keys():
        header = st.html(f"<h2>{st.session_state.search_last['search_term']}</h2>")

    # write the actual values, with some formatting
    if 'df_hits_html' in st.session_state.search_last.keys():
        table = st.html(st.session_state.search_last['df_hits_html'])

st.session_state.previous_page = page_title