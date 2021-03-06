# RUN THIS FROM THE ROOT OF THE PROJECT: > bin/run-dev-env

# Set up the environment variables that change between projects
$COOP = "tbdatalake"
$HEN_NAME = "d0062"
$SERVICE_NAME = "tb-app-datalake"
$IMAGE = "gamestop/gs.docker.buildenv.serverless:1.25.0"
$REGION = "us-east-1"

#
# from here down should be all template--shouldn't change per project
#

# the current branch name
$STAGE = (git symbolic-ref --short HEAD)

$STACK_NAME = "$SERVICE_NAME-$STAGE"

docker run `
  --rm `
  --mount type=bind,source="$(Get-Location)",target=/app `
  -e SHELL=/bin/bash `
  -e AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN `
  -e AWS_DEFAULT_REGION=$REGION `
  -e AWS_DEFAULT_OUTPUT=json `
  -e COOP=$COOP `
  -e HEN_NAME=$HEN_NAME `
  -e STAGE=$STAGE `
  -e STACK_NAME=$STACK_NAME `
  -w /app `
  -it $IMAGE `
  bash
