#!/bin/bash

set -e

echo "************************"
echo "*** Exiting functional test script since there are no functional tests."
echo "*** Restore the commented code when we have valid tests."
echo "*** Otherwise spinning the environment up and down is a waste of time and money."
echo "************************"


# app should be deployed as a separate step.
# This script should just runs the tests, assuming a valid deployment


# echo "Running integration tests"
# #pip install pytest
# #pip install boto3
# cd tests/functional-acceptance-tests && pytest -q --stackname $STACK_NAME && cd ../..

# echo "Deleting objects from buckets so we can delete the buckets"

# clean_bucket CodeBucket

# echo "Tearing down integration testing stack"

# The remove step should be separate, just like the deployment.
# Hmm. maybe this is just the script that glues the 3 pieces together
# or perhaps that's a make rule
# remove_app

