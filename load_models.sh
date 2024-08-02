#!/bin/bash

source .env

# Install eland[pytorch] Python package
#python -m pip install 'eland[pytorch]'

# Import hub model using eland_import_hub_model
eland_import_hub_model \
    --cloud-id $ELASTIC_CLOUD_ID \
    --es-api-key $ELASTIC_API_KEY \
    --hub-model-id $ELASTIC_DENSE_FIELD_MODEL_HUB_ID \
    --task-type $ELASTIC_DENSE_FIELD_TASK_TYPE \
    --clear-previous \
    --start