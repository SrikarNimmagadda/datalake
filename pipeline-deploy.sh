#!/bin/bash

set -e

# include deploy scripts
. deploy.sh

# Install dependencies
install_tools

# Manage the app
deploy_app
#NOTE: to remove the app, comment out the deploy_app line and uncomment the remove_app line
#remove_app
