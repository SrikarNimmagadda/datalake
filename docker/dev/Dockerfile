# Create the local build environment

FROM gamestop/gs.docker.serverless.nodejs.4.3.2

#Add scripts
#COPY scripts /usr/local/bin
#RUN chmod +x /usr/local/bin/build.sh && chmod +x /usr/local/bin/unittests.sh && chmod +x /usr/local/bin/package.sh

ENV SERVICE_NAME=tb-app-datalake \
    PIPELINE=tb.app.datalake-local \
    STACK_NAME=tb-app-datalake-master \
    STAGE=local \
    COOP=apparch \
    HEN_NAME=d0083 \
    NPM_TOKEN=none \
    SLS_DEBUG=true

LABEL "application"="tb-app-datalake"

COPY . /app

WORKDIR /app


