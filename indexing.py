from elasticsearch import Elasticsearch, NotFoundError
from icecream import ic
import glob
import os
import hashlib
import re
import fire

from decouple import config

elastic_cloud_id = config('ELASTIC_CLOUD_ID', default='none')
elastic_api_key = config('ELASTIC_API_KEY', default='none')
elastic_index_name = config('ELASTIC_INDEX_NAME', default='none')
raw_data = config('RAW_DATA', default='none')
elastic_synonym_fn = config('ELASTIC_SYNONYM_FILE', default='none')
elastic_synonym_id = config('ELASTIC_SYNONYM_ID', default='none')
elastic_pipeline_name = config('ELASTIC_PIPELINE_NAME', default='none')
elastic_model_name = config('ELASTIC_MODEL_NAME', default='none')
elastic_input_field_name = config('ELASTIC_INPUT_FIELD_NAME', default='none')
elastic_output_field_name = config('ELASTIC_OUTPUT_FIELD_NAME', default='none')

elastic_client = Elasticsearch(
    cloud_id=elastic_cloud_id,
    api_key=elastic_api_key,
)

ic(elastic_cloud_id, elastic_api_key, elastic_index_name, raw_data, elastic_synonym_fn, elastic_synonym_id)

def create_pipeline(client=elastic_client,
                    output_field_name = elastic_output_field_name, 
                    input_field_name = elastic_input_field_name, 
                    model_name = elastic_model_name, 
                    pipeline_name=elastic_pipeline_name):
    
    pipeline = {
        "description": "Inference pipeline for ELSER embeddings",
        "processors": [
            {
                "inference": {
                    "model_id": model_name,
                    "input_output": [
                        {
                            "input_field": input_field_name,
                            "output_field": output_field_name,
                        }
                    ]
                }
            }
        ],
    }

    try: 
        client.ingest.delete_pipeline(id=pipeline_name)
        ic("Deleted pipeline {}".format(pipeline_name))
    except NotFoundError:
        pass

    client.ingest.put_pipeline(id=pipeline_name, body=pipeline)
    ic("Created pipeline {}".format(pipeline_name))

def read_synonyms_from_csv(synonyms_fn=elastic_synonym_fn):
    """
    Read synonyms from a CSV file and return a list of synonym dictionaries.

    Args:
        synonyms_fn (str): The path to the CSV file containing synonyms.

    Returns:
        list: A list of synonym dictionaries, where each dictionary has an "id" and "synonyms" key.
    """
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

def create_synonyms_with_csv(client=elastic_client, 
                             synonyms_fn=elastic_synonym_fn, 
                             synonyms_id=elastic_synonym_id):
    """
    Create synonyms in Elasticsearch using a CSV file.

    Args:
        client (Elasticsearch): The Elasticsearch client.
        synonyms_fn (str): The path to the CSV file containing synonyms.
        synonyms_id (str): The ID to assign to the synonyms set in Elasticsearch.
    """
    synonyms_set = read_synonyms_from_csv(synonyms_fn=synonyms_fn)
    client.synonyms.put_synonym(id=synonyms_id, synonyms_set=synonyms_set)

def create_index_with_fields(client=elastic_client, 
                             pipeline_name= elastic_pipeline_name,
                             index_name=elastic_index_name,
                             model_name=elastic_model_name,
                             input_field_name=elastic_input_field_name,
                             output_field_name=elastic_output_field_name):
    """
    Create an Elasticsearch index with custom analysis settings and mappings.

    Args:
        client (Elasticsearch): The Elasticsearch client.
        pipeline_name (str): The name of the pipeline to create.
        index_name (str): The name of the index to create.
    """

    settings = {
        "index": {
            "default_pipeline": pipeline_name
        },
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
            "file_name": {"type": "text"},
            "line_number": {"type": "integer"},
            "heading": {
                "type": "text",
            },
            "heading_completion": {
                "type": "text",
                "analyzer": "autocomplete",
                "search_analyzer": "standard"
            },
            "text": {
                "type": "text",
            },
            output_field_name: {
                "type": "sparse_vector",
            },
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
    ic("Creating embeddings in {}".format(output_field_name))

    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        ic("Deleted index {}".format(index_name))

    create_pipeline(client=client, 
                output_field_name=output_field_name, 
                input_field_name=input_field_name, 
                model_name=model_name, 
                pipeline_name=pipeline_name)


    client.indices.create(index=index_name, mappings=mappings, settings=settings)
    ic("Created index {}".format(index_name))


def index_file_to_elasticsearch(file_path: str, 
                                client=elastic_client, 
                                index_name=elastic_index_name):
    """
    Index a file to Elasticsearch.

    Args:
        file_path (str): The path to the file to index.
        client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to index the file into.
    """
    last_heading = None  # This will keep track of the last seen heading

    with open(file_path, 'r', encoding='utf-8') as file:
        ic("Opened {}".format(file_path))

        for line_number, line in enumerate(file, start=1):
            unique_id = hashlib.sha256((os.path.basename(file_path) + str(line_number)).encode()).hexdigest()

            # Check if the line is a heading
            if re.match(r'^#{1,6} ', line):
                last_heading = line.strip('# ').rstrip()  # Remove '#' and trailing spaces
                continue  # don't index the header itself.

            doc = {
                "file_name": os.path.basename(file_path),
                "line_number": line_number,
                "heading": last_heading.strip(),
                "heading_completion": last_heading.strip(),
                "text": line.strip(),
                "text_synonym": line.strip(),
                "text_completion": line.strip(),
            }

            if line not in ['', '\n']:
                client.index(index=index_name, body=doc, id=unique_id, pipeline=elastic_pipeline_name)
        
def index_directory_to_elasticsearch(client=elastic_client, 
                                     index_name=elastic_index_name,
                                     raw_data=raw_data):
    """
    Index all files in a directory to Elasticsearch.

    Args:
        client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to index the files into.
        raw_data (str): The path pattern to match the files to index.
    """
    glob_pattern = raw_data

    ic("Indexing {}".format(glob_pattern))

    for file_path in glob.glob(glob_pattern, recursive=True):
        index_file_to_elasticsearch(client=client, 
                                    file_path=file_path, 
                                    index_name=index_name)


def all(client=elastic_client, 
        index_name=elastic_index_name,
        pipeline_name=elastic_pipeline_name,
        input_field_name=elastic_input_field_name,
        output_field_name=elastic_output_field_name,
        model_name=elastic_model_name,
        synonyms_fn=elastic_synonym_fn, 
        synonyms_id=elastic_synonym_id, 
        raw_data=raw_data):
    """
    Perform all steps: create synonyms, create index, and index files.

    Args:
        client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to create and index the files into.
        synonyms_fn (str): The path to the CSV file containing synonyms.
        synonyms_id (str): The ID to assign to the synonyms set in Elasticsearch.
        raw_data (str): The path pattern to match the files to index.
    """
    # create_pipeline(client=client, 
    #                 output_field_name=output_field_name, 
    #                 input_field_name=input_field_name, 
    #                 model_name=model_name, 
    #                 pipeline_name=pipeline_name)
    create_synonyms_with_csv(client=client, 
                             synonyms_fn=synonyms_fn, 
                             synonyms_id=synonyms_id)
    create_index_with_fields(client=client, 
                             index_name=index_name,
                             pipeline_name=pipeline_name,
                             output_field_name=output_field_name)
    index_directory_to_elasticsearch(client=client, 
                                     index_name=index_name, 
                                     raw_data=raw_data)

if __name__ == "__main__":

    # Use Fire to automatically generate a CLI.
    #
    # Invoking this function would look something like:
    #   python indexing.py pipelines  (grabs defaults from .env)
    #   python indexing.py synonyms  (grabs defaults from .env)
    #   python indexing.py index  (grabs defaults from .env)
    #   python indexing.py load  (grabs defaults from .env)
    #   python indexing.py all --index-name acme --synonyms_fn synonyms.csv --synonyms_id acme-synonyms --raw_data "site/*.txt" (overrides defaults)

    fire.Fire({
        # "pipelines": create_pipeline,
        "synonyms": create_synonyms_with_csv,
        "index": create_index_with_fields,
        "load": index_directory_to_elasticsearch,
        "all": all
    })


    ic(elastic_index_name, raw_data)