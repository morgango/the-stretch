from elasticsearch import Elasticsearch
from icecream import ic
import glob
import os
import hashlib
import re
import csv


from decouple import config
elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
raw_data = config('RAW_DATA', default='none')
elastic_synonym_fn = config('ELASTIC_SYNONYM_FILE', default='none')
elastic_synonym_id = config('ELASTIC_SYNONYM_ID', default='none')

elastic_client = Elasticsearch(
    cloud_id=elastic_cloud_id,
    api_key=elastic_api_key,
)

def read_synonyms_from_csv(synonyms_fn = elastic_synonym_fn):  
    
    count = 0
    synonyms_set = []

    with open(synonyms_fn, 'r') as f:
        lines = f.readlines()
        for line in lines:
            count += 1
            synonym_dict = {}
            synonym_dict["id"] = "synonym-{}".format(count)
            synonym_dict["synonyms"] = line.strip()
            synonyms_set.append(synonym_dict)

    return synonyms_set


def create_synonyms_with_csv(client = elastic_client, 
                             synonyms_fn= elastic_synonym_fn, 
                             synonyms_id=elastic_synonym_id):
    
    synonyms_set = read_synonyms_from_csv(synonyms_fn=synonyms_fn)
    client.synonyms.put_synonym(id=synonyms_id, synonyms_set=synonyms_set)

def create_index_with_fields(client = elastic_client, 
                             index_name=elastic_index_name):

    settings = {
        "analysis": {
            "filter": {
                "autocomplete_filter": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 10
                },
                "acme_synonym_filter": {
                    "type": "synonym_graph",
                    "synonyms_set": elastic_synonym_id,
                    "updateable": True,
                }
            },
            "analyzer": {
                "autocomplete": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "autocomplete_filter",
                    ]
                },
                "acme_synonym_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "acme_synonym_filter",
                    ]
                }
            },
        }
    }

    mappings = {
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
            },
            "text_synonym": {
                "type": "text",
                "analyzer": "autocomplete",
                "search_analyzer": "acme_synonym_analyzer"
            }
        }
    }


    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        ic("Deleted index {}".format(index_name))

    client.indices.create(index=index_name, mappings=mappings, settings=settings)
    ic("Created index {}".format(index_name))

def index_file_to_elasticsearch(file_path: str, 
                                client = elastic_client, 
                                index_name=elastic_index_name):
    
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
                "text_synonym": line.strip(),

            }

            if line not in['', '\n']:
                client.index(index=index_name, body=doc, id=unique_id)

def index_directory_to_elasticsearch(client = elastic_client, 
                                     index_name=elastic_index_name, 
                                     raw_data=raw_data):

    glob_pattern = raw_data 

    ic("Indexing {}".format(glob_pattern))
    
    for file_path in glob.glob(glob_pattern, recursive=True):
        index_file_to_elasticsearch(file_path)

if __name__ == "__main__":

    create_synonyms_with_csv(client = elastic_client, 
                             synonyms_fn= elastic_synonym_fn, 
                             synonyms_id=elastic_synonym_id)
    create_index_with_fields(client = elastic_client, 
                             index_name=elastic_index_name)
    index_directory_to_elasticsearch(client=elastic_client, 
                                     index_name=elastic_index_name, 
                                     raw_data=raw_data)
    
    ic(elastic_index_name, raw_data)