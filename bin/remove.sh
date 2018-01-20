#!/bin/bash

# Removes a deployment, including its data and aws resources

set -e

# When calling this script while the STAGE environment variable is set to a
# protected value, the user must pass a --force flag to override and allow
# the uninstall since this script removes data from s3 buckets

force=false


while [[ $1 != "" ]]; do
  case $1 in
    --force)
      force=true
      ;;
  esac

  shift
done

# if --file was provided, open it for writing, else duplicate stdout
if [[ $force = "false" && ($STAGE = "prod" || $STAGE = "qa" || $STAGE = "master") ]]; then
    echo "WARNING! You are attempting to remove a protected environment ($STAGE). If you are sure you want to remove it, call this script with the '--force' flag."
    exit 1
fi

# include shared functions
. bin/shared-utility.sh

function clean_bucket {
  BUCKET_NAME=$(get_bucket $1)
  echo Removing all objects from $BUCKET_NAME
  aws s3 rm s3://$BUCKET_NAME --recursive
}

function remove {
  echo REMOVE - START

  # TODO: Eventually should clean all buckets, because they'll have data
  # if we're testing.

  clean_bucket CodeBucket

  serverless remove --verbose

  echo REMOVE - DONE
}

verify_environment_vars

remove
