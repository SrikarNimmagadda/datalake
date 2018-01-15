# Create the local build environment

FROM gamestop/gs.docker.serverless.nodejs.4.3.2

ENV PIPELINE=tb.app.datalake-master \
    STACK_NAME=tb-app-datalake-local \
    STAGE=local \
    COOP=tb \
    HEN_NAME=preprod \
    NPM_TOKEN=none \
    SLS_DEBUG=true

LABEL "application"="tb-app-datalake-dev"

WORKDIR /app

ENTRYPOINT [ "bash" ]
