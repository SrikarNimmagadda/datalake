#!/bin/bash

set -e

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
  # Should also create a --force flag and protect specific branches,
  # including master, qa and prod

  CODE_BUCKET=$(get_bucket CodeBucket)
  clean_bucket CodeBucket

  serverless remove --verbose

  echo REMOVE - DONE
}

verify_environment_vars

remove
