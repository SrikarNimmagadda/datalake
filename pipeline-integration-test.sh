#!/bin/bash

set -e

. deploy.sh

install_tools

echo "Deploying app for integration testing"
deploy_app


echo "Running integration tests"
pip install pytest
pytest tests/functional-acceptance-tests

echo "Deleting objects from buckets so we can delete the buckets"

CODE_BUCKET=$(get_bucket CodeBucket)
aws s3 rm s3://$CODE_BUCKET --recursive

echo "Tearing down integration testing stack"

remove_app

