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

clean_bucket CodeBucket

echo "Tearing down integration testing stack"

remove_app

