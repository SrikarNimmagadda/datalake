#!/bin/bash -x

# phases:
#  install:
#    commands:
# npm install serverless-package-python-functions
pip install -r dev-dependencies.txt

#  pre_build:
#    commands:
chmod +x *.sh

./prebuild.sh

#  build:
#    commands:
echo Build started on `date`
echo "installing dependencies"
pyb install_dependencies 
pyb extract_metadata package_lambda_code
pyb route_raw package_lambda_code
pyb start_job_store package_lambda_code

echo "executing initial build"
pyb


# hmm if serverless is doing the pull of dependencies on package, then the unit tests have to run against the package?
# or do we have to pull dependencies twice? That would break the principle of "build once"
#serverless package

#  post_build:
#    commands:
./test.sh