#!/bin/bash

set -e

# include deploy scripts
. bin/shared-utility.sh

# FUNCTION PURPOSE: Deploy the app
function clean_stack {
  echo PRE DEPLOY - START

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

  echo PRE DEPLOY - DONE
}

function deploy {
  echo DEPLOY - START

  serverless deploy -v

  echo "######################"
  echo Deploying spark code to code bucket $CODE_BUCKET
  echo "######################"

  CODE_BUCKET=$(get_bucket CodeBucket)

  aws s3 sync ./spark s3://$CODE_BUCKET --delete

  echo DEPLOY - DONE
}

verify_environment_vars

clean_stack

deploy
