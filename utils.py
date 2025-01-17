from elasticsearch import Elasticsearch

from typing import Any, List, Dict
from decouple import config
from icecream import ic

import pandas as pd

from streamlit_searchbox import st_searchbox
import streamlit as st

elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_sparse_model_name = config('ELASTIC_SPARSE_MODEL_NAME', default='none')

@st.cache_resource
def get_elastic_client(cloud_id, api_key):
    """
    Get the Elasticsearch client.

    Args:
        cloud_id (str): The cloud ID for the Elasticsearch cluster.
        api_key (str): The API key for authentication.

    Returns:
        Elasticsearch: The Elasticsearch client.
    """
    elastic_client = Elasticsearch(
        cloud_id=elastic_cloud_id,
        api_key=elastic_api_key,
    )

    return elastic_client

elastic_client = get_elastic_client(cloud_id=elastic_cloud_id, api_key=elastic_api_key)

def display_results(page_title:str, results:str):
    """
    Display the results of the search, based on the page title and the sesion state
    
    Parameters

        results: str - the results of the search
        page_title: str - the title of the page
        st.session_state.previous_page: str - the previous page title
        st.session_state.current_page: str - the current page title
        st.session_state.search_last: dict - the last search metadata
    
    Returns

            None

    """

    # We don't want to generate HTML if we are on the page for the first time.
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

def add_to_search_history(search_metadata, max_history_size=100):
    """
    Add the search metadata to the search history.

    Args:
        search_metadata (dict): The search metadata to add to the search history.
        max_history_size (int): The maximum size of the search history. Default is 100.
    """

    st.session_state.search_last = search_metadata

    if 'search_history' not in st.session_state:
        st.session_state.search_history = []
    
    if len(st.session_state.search_history) > max_history_size:
        st.session_state.search_history = st.session_state.search_history[1:]
    
    st.session_state.search_history.append(search_metadata)

def build_search_metadata(text_values, 
                            searchterm, 
                            search_type, 
                            index_field_name, 
                            display_field_name, 
                            hits,
                            excluded_fields,
                            query = [], 
                            min_stearchterm_length=1,
                            add_to_history=False,
                            max_history_size=100) -> Dict:
    """
    Build the search metadata.

    Args:

        text_values (list): The text values to display in the search results.
        searchterm (str): The search term used in the query.
        search_type (str): The type of search used in the query.
        index_field_name (str): The name of the field used in the query.
        display_field_name (str): The name of the field to display in the search results.
        hits (list): The hits from the Elasticsearch query.
        query (list): The query used in the Elasticsearch query.
        excluded_fields (list): A list of fields to drop from the DataFrame.
        min_stearchterm_length (int): The minimum length of the search term. Default is 4.
        add_to_history (bool): Whether to add the search metadata to the search history. Default is False.
        max_history_size (int): The maximum size of the search history. Default is 100.

    Returns:

        dict: The search metadata.
        st.session_state.search_last: The search metadata.
        st.session_state.search_history: This value is also added to the search history

    """

    if 'search_last' not in st.session_state:
        st.session_state.search_last = {}

    search_metadata = {}

    if len(searchterm) > min_stearchterm_length:

        # save raw data to the session state so that they can be displayed as the keyboard is being typed
        search_metadata['search_time'] = pd.Timestamp.now()
        search_metadata['text_values'] = text_values
        search_metadata['search_term'] = searchterm
        search_metadata['search_type'] = search_type
        search_metadata['search_field'] = index_field_name
        search_metadata['search_display_field'] = display_field_name
        search_metadata['search_query'] = query

        if hits:
            updated_hits = [replace_with_highlight(hit) for hit in hits]

            search_metadata['hits'] = updated_hits
            search_metadata['df_hits'] = flatten_hits(search_metadata['hits'], excluded_fields=excluded_fields)
            search_metadata['df_hits_html'] = df_to_html(search_metadata['df_hits'], remove_highlights=True)
        else:
            search_metadata['hits'] = []
            search_metadata['df_hits'] = pd.DataFrame()
            search_metadata['df_hits_html'] = ""
        
        if add_to_history:
            add_to_search_history(search_metadata, max_history_size=max_history_size)
    
    return search_metadata
    
def replace_with_highlight(hit):
    """

        Replace the original text with the exerpts of highlighted text from Elasticsearch.

        Args:       
            hit (dict): The hit from Elasticsearch.

        Returns:
            dict: The hit with the highlighted text.

    """    
    if 'highlight' in hit:
        for key in hit['highlight']:
            if key in hit['_source']:
                hit['_source'][key] = ' '.join(hit['highlight'][key])
    return hit

def df_to_html(df, 
               remove_highlights=True,
               remove_fields=[]) -> str:
    """
    Convert a pandas DataFrame to an HTML table.

    Args:
        df (pandas.DataFrame): The DataFrame to convert.
        description (str): The description to display above the table. Default is "Search Details".
        remove_fields (list): A list of fields to remove from the DataFrame. Default is [].

    Returns:
        str: The HTML representation of the DataFrame as a table.
    """

    if remove_highlights:
        if 'highlight' in df.columns:
            df = df.drop('highlight', axis=1)

    df = df.drop(remove_fields, axis=1)

    html = df.to_html(index=False, escape=False)
    html = f'''
            <style>
                table {{
                    width: 100%;
                }}
                th {{
                    text-align: center;
                }}
                td, th {{
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                }}
                em {{
                    background-color: #ff0; /* bright yellow background */
                    color: #000; /* black text */
                    font-weight: bold; /* bold text */
                }}
            </style>
            {html}
        '''
    return html

def flatten_hits(hits: List[dict], excluded_fields=['_id', '_index', 'text_synonym']) -> List[dict]:
    """
    Flatten the hits from an Elasticsearch query.

    Args:
        hits (list): A list of dictionaries containing the hits from an Elasticsearch query.

    Returns:
        pandas.DataFrame: A dataframe containing the values in hits.
    """
    flattened_data = []
    for hit in hits:
        flattened_dict = {**hit, **hit['_source']}
        del flattened_dict['_source']
        flattened_data.append(flattened_dict)

    # Convert the list of dictionaries into a pandas DataFrame
    tmp = pd.DataFrame(flattened_data)

    valid_fields = set(excluded_fields).intersection(set(tmp.columns))       
    df = tmp.drop(valid_fields, axis=1)

    return df

def query_elastic_by_single_field(searchterm: str, 
                                  
                  index_name=elastic_index_name, 
                  field_name="",
                  search_type="match",
                  fuzziness: str = None,
                  highlight: bool = False,
                  model: str = elastic_sparse_model_name,
                  client=elastic_client) -> List[Any]:
    """
    Query Elasticsearch by field.

    Args:
        searchterm (str): The search term to query.
        index_name (str): The name of the Elasticsearch index to search in.
        field_name (str): The name of the field to search in.
        search_type (str): The type of search to perform. Options: "match" (default), "fuzzy".
        fuzziness (str): The fuzziness parameter for fuzzy search. Default is None.
        highlight (bool): Whether to highlight the search term in the results. Default is False.
        model (str): The name of the model to use for semantic search. Default is "none".
        excluded_fields (list): A list of fields to drop from the DataFrame. Default is ['_id', '_index', 'text_synonym'].
        client (Elasticsearch): The Elasticsearch client to use for the query.

    Returns:
        list: A list of Elasticsearch hits.
        query_body (dict): The query body used in the Elasticsearch query.
        st.session_state.df_hits: A dataframe with all the hits 
        st.session_state.hits: An HTML representation of the dataframe

    """

    if search_type in ["semantic", "text_expansion", "vector"]:
        search_type = "semantic"
        highlight = False

    query_body = {"query": {}}

    if search_type == "match":
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

    elif search_type == "semantic":

        query_body["query"]["semantic"] = {
            "field": field_name,
            "query": searchterm
        }

    if 'highlight' not in query_body:
        query_body['highlight'] = {}

    if 'fields' not in query_body['highlight']:
        query_body['highlight']['fields'] = {}

    if highlight:
        query_body["highlight"]["fields"][field_name] = {}

    response = client.search(index=index_name, body=query_body)
    hits = response['hits']['hits']

    return hits, query_body

def query_elastic_by_multiple_fields(searchterm: str, 
                  index_name=elastic_index_name, 
                  field_names=None, 
                  search_type="match",
                  fuzziness: str = None,
                  client=elastic_client) -> List[Any]:
    """
    Query Elasticsearch by multiple fields.

    Args:
        searchterm (str): The search term to query.
        index_name (str): The name of the Elasticsearch index to search in.
        field_names (list): A list of field names to search in.
        search_type (str): The type of search to perform. Options: "match" (default), "fuzzy".
        fuzziness (str): The fuzziness parameter for fuzzy search. Default is None.
        client (Elasticsearch): The Elasticsearch client to use for the query.

    Returns:
        hits: A list of Elasticsearch hits.
        query_body (dict): The query body used in the Elasticsearch query.

    """

    if search_type in ["semantic", "text_expansion", "vector"]:
        search_type = "semantic"
        highlight = False

    query_body = {"query": {}}

    if search_type == "match":
        # Use multi_match query to search in multiple fields
        query_body["query"]["multi_match"] = {
            "query": searchterm,
            "fields": field_names
        }
    elif search_type == "fuzzy":
        # You need to write a loop for fuzzy search in multiple fields
        query_body["query"]["bool"] = {
            "should": [
                {
                    "fuzzy": {
                        field_name: {
                            "value": searchterm,
                            "fuzziness": fuzziness if fuzziness else "AUTO"
                        }
                    }
                }
                for field_name in field_names
            ]
        }

    if 'highlight' not in query_body:
        query_body['highlight'] = {}

    if 'fields' not in query_body['highlight']:
        query_body['highlight']['fields'] = {}


    response = client.search(index=index_name, body=query_body)
    hits = response['hits']['hits']

    return hits, query_body
