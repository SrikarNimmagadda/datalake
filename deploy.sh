#!/bin/bash

function install_tools
{
  echo INSTALLING TOOLS - START
  python --version
  echo Nodejs $(node --version)
  echo npm $(npm --version)
  echo Serverless $(serverless --version)
  echo INSTALLING TOOLS - DONE
}
export -f install_tools

function get_bucket() {
  BUCKET_NAME=$((aws cloudformation describe-stacks --stack-name $STACK_NAME --query 'Stacks[0].Outputs[?OutputKey==`$1`].OutputValue' --output text) 2>&1)
  echo BUCKET_NAME
}
export -f get_bucket

function clean_bucket() {
  BUCKET_NAME=$(get_bucket $1)
  echo Removing all objects from $BUCKET_NAME
  # aws s3 rm s3://$BUCKET_NAME --recursive
}
export -f clean_bucket

# FUNCTION PURPOSE: Deploy the app
function deploy_app
{
  echo PRE DEPLOY - START
  # Get github commit hash for application repository
  aws codepipeline --region us-east-1 get-pipeline-state --name $PIPELINE | jq -r '.stageStates[0].actionStates[0].currentRevision.revisionId' > apphash.txt
  echo "apphash is $(cat apphash.txt)"

  # Check to see if stack is in ROLLBACK_COMPLETE. If it is, delete it first.
  STACK_STATUS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query Stacks[0].StackStatus --output text || echo "NOT_FOUND")
  echo "Current stack status is $STACK_STATUS."
  if [ $STACK_STATUS = ROLLBACK_COMPLETE ] || [ $STACK_STATUS = ROLLBACK_FAILED ] || [ $STACK_STATUS = DELETE_FAILED ]; then
    remove_app
    until [[ $STACK_STATUS =~ COMPLETE|FAILED ]]; do
        sleep 10
        STACK_STATUS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query 'Stacks[0].StackStatus' --output text)
        echo "Current stack status is $STACK_STATUS."
    done
  fi

  # SECRETS MANAGEMENT
  # Secrets are kept in an S3 bucket and can be accessed at runtime or at build time.
  # For instructions on updating secrets and accessing secrets, see https://github.com/GameStopCorp/gs.serverless.support/blob/master/secrets-uploader/README.md
  # To see an example of client-side encryption/decryption, view the commands at https://github.com/GameStopCorp/gs.aws.pipelines/blob/master/buildspec.yml
  echo PRE DEPLOY - DONE


  echo DEPLOY - START
  serverless deploy -v

  CODE_BUCKET=$(get_bucket CodeBucket)
  echo deploying spark code to code bucket $CODE_BUCKET

  # commenting out to unbreak the build until I figure out how to fix
  # aws s3 sync ./spark s3://$CODE_BUCKET --delete

  echo DEPLOY - DONE
}
export -f deploy_app


# FUNCTION PURPOSE: Remove the app
function remove_app
{
  echo REMOVE - START
  serverless remove
  echo REMOVE - DONE
}
export -f remove_app

# Date required for test reporting S3 object paths
stage_name=${STAGE%%tests}
declare -r DATE=$(date +%Y%b%d)
declare -r TIME=$(date +%H%M%S%Z)
S3_REPORT_PATH="s3://motherhen-$COOP-$HEN_NAME-testresults/$SERVICE_NAME-$stage_name/$DATE"

function report_unit_tests
{   
    s3_path="$S3_REPORT_PATH/unit/$TIME"
    echo "Syncronizing Unit Test Results to Bucket"
    aws s3 sync ./coverage "$s3_path"
    echo "Unit Test Results Syncronized"
}
export -f report_unit_tests

function report_integration_tests
{
    s3_path="$S3_REPORT_PATH/int/$TIME"
    echo "Syncronizing Integration Test Results to Bucket"
    aws s3 sync ./int-test-report  "$s3_path"
    echo "Integration Test Results Syncronized"
}
export -f report_integration_tests