#!/bin/bash

function verify_environment_vars {
  : "${COOP:?Need to set COOP env var to Mother Hen coop name.}"
  echo "Coop: $COOP"

  : "${HEN_NAME:?Need to set HEN_NAME env var to Mother Hen hen name.}"
  echo "Hen Name: $HEN_NAME"

  : "${STACK_NAME:?Need to set STACK_NAME env var to the name of the cloudformation stack being deployed.}"
  echo "Stack Name: $STACK_NAME"

  : "${STAGE:?Need to set STAGE env var to the branch name of the code being deployed.}"
  echo "Stage: $STAGE"

  : "${SERVICE_NAME:?Need to set SERVICE_NAME env var for serverless.yml to identify the application.}"
  echo "Service Name: $SERVICE_NAME"
}
export -f verify_environment_vars


# pass the output key from the stack as the argument to this function (e.g. get_bucket CodeBucket)
function get_bucket {
  QUERY="Stacks[0].Outputs[?OutputKey=='$1'].OutputValue"
  BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query $QUERY --output text)
  echo $BUCKET_NAME
}
export -f get_bucket
