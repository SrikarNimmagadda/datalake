# Create the local build environment

FROM python:2

ENV SERVICE_NAME=tb-app-datalake-dev \
    PIPELINE=tb.app.datalake-master \
    STACK_NAME=tb-app-datalake-local \
    STAGE=local \
    COOP=tb \
    HEN_NAME=preprod \
    NPM_TOKEN=none \
    SLS_DEBUG=true

LABEL "application"="tb-app-datalake-dev"

RUN pip install pybuilder

WORKDIR /app


