#!/bin/bash

set -e

. deploy.sh

install_tools

echo "Deploying app for integration testing"
deploy_app


echo "Running integration tests"
pip install pytest
pip install boto3
cd tests/functional-acceptance-tests && pytest -q --stackname $STACK_NAME && cd ../..

echo "Deleting objects from buckets so we can delete the buckets"

clean_bucket CodeBucket

echo "Tearing down integration testing stack"

remove_app

