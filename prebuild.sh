#!/bin/bash

# install virutalenv for pybuilder
pip install virtualenv
virtualenv venv
source venv/Scripts/activate

# start virtual environment for pybuilder
echo "installing PyBuilder"
pip install pybuilder
pip install pytest
#echo "installing PipEnv"
#pip install pipenv
#pipenv install requests
echo "installing boto3"
pip install boto3
