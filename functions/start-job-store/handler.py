#
# Copied from tb-us-east-1-dl-dev-EMR-StoreJobs on D0062 Sandbox account
#

import os
# from boto3.client import InstanceGroups
from datetime import datetime

import boto3
from step_builder import step_builder

S3 = boto3.resource('s3')
CONN = boto3.client('emr')

BUCKETS = {
    'raw_pii': os.getenv('RAW_PII_BUCKET'),
    'raw_hr': os.getenv('RAW_HR_BUCKET'),
    'raw_regular': os.getenv('RAW_REGULAR_BUCKET'),
    'discovery_pii': os.getenv('DISCOVERY_PII_BUCKET'),
    'discovery_hr': os.getenv('DISCOVERY_HR_BUCKET'),
    'discovery_regular': os.getenv('DISCOVERY_REGULAR_BUCKET'),
    'refined_pii': os.getenv('REFINED_PII_BUCKET'),
    'refined_hr': os.getenv('REFINED_HR_BUCKET'),
    'refined_regular': os.getenv('REFINED_REGULAR_BUCKET'),
    'delivery': os.getenv('DELIVERY_BUCKET')
}


def lambda_handler(event, context):

    # for file in raw_file_list:
    #    print(file)
    builder = step_builder(S3, BUCKETS, datetime.now())

    steps = builder.BuildSteps

    cluster_id = CONN.add_job_flow_steps(
        JobFlowId='????',
        Steps=steps
    )

    print cluster_id
