#!/bin/bash

# install virutalenv for pybuilder
pip install virtualenv
virtualenv venv
source venv/Scripts/activate

# start virtual environment for pybuilder
echo "Installing PyBuilder"
pip install pybuilder
echo "Installing PyTest"
pip install pytest
echo "Installing Boto3"
pip install boto3
