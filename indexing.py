from elasticsearch import Elasticsearch, NotFoundError, exceptions
from icecream import ic
import glob
import os
import time
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
elastic_sparse_field_name = config('ELASTIC_SPARSE_FIELD_NAME', default='none')
elastic_sparse_model_name = config('ELASTIC_SPARSE_MODEL_NAME', default='none')
elastic_dense_field_name = config('ELASTIC_DENSE_FIELD_NAME', default='none')
elastic_dense_field_model_name = config('ELASTIC_DENSE_FIELD_MODEL_NAME', default='none')
elastic_dense_field_dims = config('ELASTIC_DENSE_FIELD_DIMS', default=0, cast=int)
elastic_sparse_inference_endpoint_name = config('ELASTIC_SPARSE_INFERENCE_ENDPOINT_NAME', default='none')

elastic_client = Elasticsearch(
    cloud_id=elastic_cloud_id,
    api_key=elastic_api_key,
)


def create_inference_endpoint(inference_endpoint_name=elastic_sparse_inference_endpoint_name, 
                              client=elastic_client):
    """
    Create an inference endpoint in Elasticsearch.

    This code is lifted almost directly from Elasticsearch Labs: 
        https://github.com/elastic/elasticsearch-labs/blob/main/notebooks/search/09-semantic-text.ipynb

    Args:
        inference_endpoint_name (str): The name of the inference endpoint to create.
        client (Elasticsearch): The Elasticsearch client.
    
    Raises:
        exceptions.BadRequestError: If the inference endpoint already exists.

    Returns:
        dict: The information about the created inference endpoint.

    """
    
    ic("Creating inference endpoints", inference_endpoint_name, client)

    try:
        client.inference.delete_model(inference_id=inference_endpoint_name)
        ic("Deleted inference endpoint {}".format(inference_endpoint_name))
    except exceptions.NotFoundError:
        # Inference endpoint does not exist
        pass

    try:
        client.options(
            request_timeout=60, max_retries=3, retry_on_timeout=True
        ).inference.put_model(
            task_type="sparse_embedding",
            inference_id=inference_endpoint_name,
            body={
                "service": "elser",
                "service_settings": {"num_allocations": 1, "num_threads": 1},
            },
        )
        
        ic("Created inference endpoint {}".format(inference_endpoint_name))

    except exceptions.BadRequestError as e:
        if e.error == "resource_already_exists_exception":
            ic("Inference endpoint already exists {}".format(inference_endpoint_name))
        else:
            raise e
        
    inference_endpoint_info = client.inference.get_model(
        inference_id=inference_endpoint_name,
    )

    ic(dict(inference_endpoint_info))
    
    model_id = inference_endpoint_info["endpoints"][0]["service_settings"]["model_id"]

    # deploy the ELSER model if it is not already deployed
    while True:
        status = client.ml.get_trained_models_stats(
            model_id=model_id,
        )

        deployment_stats = status["trained_model_stats"][0].get("deployment_stats")
        if deployment_stats is None:
            ic("ELSER Model is currently being deployed.")
            time.sleep(5)
            continue

        nodes = deployment_stats.get("nodes")
        if nodes is not None and len(nodes) > 0:
            ic("ELSER Model has been successfully deployed.")
            break
        else:
            ic("ELSER Model is currently being deployed.")
        time.sleep(5)

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
    ic("Created synonyms with CSV", synonyms_fn, synonyms_id)

def create_index_with_fields(client=elastic_client, 
                             inference_endpoint_name = elastic_sparse_inference_endpoint_name,
                             index_name=elastic_index_name,
                             sparse_field_name=elastic_sparse_field_name,
                             dense_field_name=elastic_dense_field_name,
                             dense_field_dims=elastic_dense_field_dims):
    """
    Create an Elasticsearch index with custom analysis settings and mappings.

    Args:
        client (Elasticsearch): The Elasticsearch client.
        inference_endoint_name (str): The name of the inference endpoint to use.
        index_name (str): The name of the index to create.
        sparse_field_name (str): The name of the output field.
        dense_field_name (str): The name of the dense field.
        dense_field_dims (int): The number of dimensions for the dense field.
        
    """

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
                "copy_to": ["text_sparse_embedding"]
            },
            sparse_field_name: {
                "type": "semantic_text",
                "inference_id": inference_endpoint_name
            },
            dense_field_name: {
                "type": "dense_vector",
                "dims": dense_field_dims,
                "index": True,
                "similarity": "cosine",
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
            },
        }
    }

    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        ic("Deleted index {}".format(index_name))

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

    from elasticsearch import helpers, exceptions

    last_heading = None  # This will keep track of the last seen heading
    actions = []  # This will store all the actions to be performed in bulk

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
                "text_completion": line.strip(),
                "text_synonym": line.strip(),
            }

            if line not in ['', '\n']:
                action = {
                    "_index": index_name,
                    "_id": unique_id,
                    "_source": doc
                }
                actions.append(action)

    # Perform all actions in bulk
    if actions:
        try:
            helpers.bulk(client, actions)
        except helpers.BulkIndexError as e:
            ic(f"Bulk index error: {e.errors}")
        
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
        sparse_field_name=elastic_sparse_field_name,
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
    create_inference_endpoint(inference_endpoint_name=elastic_sparse_inference_endpoint_name,
                                client=client)
    create_synonyms_with_csv(client=client, 
                             synonyms_fn=synonyms_fn, 
                             synonyms_id=synonyms_id)
    create_index_with_fields(client=client, 
                             index_name=index_name,
                             sparse_field_name=sparse_field_name)
    index_directory_to_elasticsearch(client=client, 
                                     index_name=index_name, 
                                     raw_data=raw_data)

if __name__ == "__main__":

    # Use Fire to automatically generate a CLI.
    #
    # Invoking this function would look something like:
    #   python indexing.py inference  (grabs defaults from .env)
    #   python indexing.py synonyms  (grabs defaults from .env)
    #   python indexing.py index  (grabs defaults from .env)
    #   python indexing.py load  (grabs defaults from .env)
    #   python indexing.py all --index-name acme --synonyms_fn synonyms.csv --synonyms_id acme-synonyms --raw_data "site/*.txt" (overrides defaults)

    fire.Fire({
        "inference": create_inference_endpoint,
        "synonyms": create_synonyms_with_csv,
        "index": create_index_with_fields,
        "load": index_directory_to_elasticsearch,
        "all": all
    })


    ic(elastic_index_name, raw_data)