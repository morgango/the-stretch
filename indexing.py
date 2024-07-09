from elasticsearch import Elasticsearch
from icecream import ic
import glob
import os
import hashlib
import re

from decouple import config
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
raw_data = config('RAW_DATA', default='none')

def create_index_with_fields():

    # Assign the provided Elasticsearch cloud ID and API key to local variables
    cloud_id = elastic_cloud_id
    api_key = elastic_api_key
    index_name = elastic_index_name
    
    # Create an Elasticsearch client using the provided cloud ID and API key
    client = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key
    )

    # Define the mappings for the index
    # The mappings specify the fields in the documents and their types
    # mapping = {
    #     "mappings": {
    #         "properties": {
    #             "file_name": { "type": "text" },
    #             "line_number": { "type": "integer" },
    #             "heading": { "type": "text" },
    #             "text": { "type": "text" },
    #             "text_completion": { "type": "completion" }
    #         }
    #     }
    # }

    mapping = {
        "settings": {
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 10
                    }
                },
                "analyzer": {
                    "autocomplete": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "file_name": { "type": "text" },
                "line_number": { "type": "integer" },
                "heading": { "type": "text" },
                "heading_completion": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "text": { "type": "text" },
                "text_completion": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                }
            }
        }
    }


    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        ic("Deleted index {}".format(index_name))

    client.indices.create(index=index_name, body=mapping)
    ic("Created index {}".format(index_name))

def index_file_to_elasticsearch(file_path: str):
    cloud_id = elastic_cloud_id
    api_key = elastic_api_key
    index_name = elastic_index_name

    client = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key
    )
    last_heading = None  # This will keep track of the last seen heading

    with open(file_path, 'r', encoding='utf-8') as file:

        ic("Opened {}".format(file_path))
        
        for line_number, line in enumerate(file, start=1):
            unique_id = hashlib.sha256((os.path.basename(file_path) + str(line_number)).encode()).hexdigest()

            # Check if the line is a heading
            if re.match(r'^#{1,6} ', line):
                last_heading = line.strip('# ').rstrip()  # Remove '#' and trailing spaces
                continue # don't index the header itself.

            doc = {
                "file_name": os.path.basename(file_path),
                "line_number": line_number,
                "heading": last_heading.strip(), 
                "heading_completion": last_heading.strip(),
                "text": line.strip(),
                "text_completion": line.strip(),
                # "text_completion": {
                #     "input": line.strip()
                # },
            }

            if line not in['', '\n']:
                client.index(index=index_name, body=doc, id=unique_id)

def index_directory_to_elasticsearch():

    glob_pattern = raw_data 
    ic("Indexing {}".format(glob_pattern))
    
    for file_path in glob.glob(glob_pattern, recursive=True):
        index_file_to_elasticsearch(file_path)

if __name__ == "__main__":
    create_index_with_fields()
    index_directory_to_elasticsearch()
    ic(elastic_index_name, raw_data)