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
elastic_model_name = config('ELASTIC_API_KEY', default='none')

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


def df_to_html(df, 
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

        # Remove selected fields
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
            </style>
            {html}
        '''
    return html

def flatten_hits(hits: List[dict], fields_to_drop=['_id', '_index', 'text_synonym']) -> List[dict]:
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
    df = tmp.drop(fields_to_drop, axis=1)

    return df


def query_elastic_by_single_field(searchterm: str, 
                  index_name=elastic_index_name, 
                  field_name="",
                  search_type="match",
                  fuzziness: str = None,
                  highlight: bool = False,
                  model: str = elastic_model_name,
                  fields_to_drop=['_id', '_index', 'text_synonym'],
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
        fields_to_drop (list): A list of fields to drop from the DataFrame. Default is ['_id', '_index', 'text_synonym'].
        client (Elasticsearch): The Elasticsearch client to use for the query.

    Returns:
        list: A list of Elasticsearch hits.
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
        query_body["query"]["text_expansion"] = {
            field_name : {
                "model_id": model,
                "model_text": searchterm
            }
        }

    if highlight:
        query_body["highlight"]["fields"][field_name] = {}

    response = client.search(index=index_name, body=query_body)
    hits = response['hits']['hits']

    # save raw data to the session state so that they can be displayed as the keyboard is being typed
    if hits:
        st.session_state.df_hits = flatten_hits(hits, fields_to_drop=fields_to_drop)
        st.session_state.hits = df_to_html(st.session_state.df_hits)

    return hits