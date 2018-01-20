#!/bin/bash

set -e

# This script just runs the tests, assuming a valid deployment

pip install pytest

cd tests/functional-acceptance-tests && pytest -q --stackname $STACK_NAME && cd ../..
