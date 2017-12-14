# Builds and runs a docker container to emulate the CodeBuild build environment
docker build -f docker/dev/Dockerfile -t tb-app-datalake .
docker run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_DEFAULT_REGION -e AWS_DEFAULT_OUTPUT -it tb-app-datalake bash

# since we only have a latest tag for our image, we generate some cruft when we recreate it. This line removes the cruft
echo "Pruning old images for this application"
docker system prune --force --filter label=application=tb-app-datalake
