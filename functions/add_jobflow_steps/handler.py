""" Lambda entry point for add_jobflow_steps functions

Acts as the composition root for the application and calls other classes
to do all the work, which allows for easier unit testing of the other
components.
"""

import os
from datetime import datetime

import boto3

from cluster_finder import ClusterFinder
from step_factory import StepFactory

from step_builder_store import StepBuilderStore
from step_builder_customer import StepBuilderCustomer
from step_builder_employee import StepBuilderEmployee
from step_builder_product import StepBuilderProduct

S3 = boto3.resource('s3')
CFN = boto3.client('cloudformation')
EMR = boto3.client('emr')

BUCKETS = {
    'code': os.getenv('CODE_BUCKET'),
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

EMR_STACK_NAME = os.getenv('EMR_STACK_NAME')
PROCESS_METADATA_TABLE = os.getenv('PROCESS_METADATA_TABLE')


def lambda_handler(event, context):
    """Lambda entry point"""
    finder = ClusterFinder(CFN)
    clusterid = finder.find_cluster(EMR_STACK_NAME)

    builder = choose_builder(event)
    steps = builder.build_steps()

    EMR.add_job_flow_steps(
        JobFlowId=clusterid,
        Steps=steps
    )


def choose_builder(event):
    """Return a specific step builder based on a switch in the event."""
    factory = StepFactory(BUCKETS['code'])
    now = datetime.now()
    switch = event['builder']

    if switch == 'store':
        return StepBuilderStore(factory, S3, BUCKETS, now)
    elif switch == 'customer':
        return StepBuilderCustomer(factory, S3, BUCKETS, now)
    elif switch == 'employee':
        return StepBuilderEmployee(factory, S3, BUCKETS, now)
    elif switch == 'product':
        return StepBuilderProduct(factory, S3, BUCKETS, now)
    else:
        raise Exception('Could not find a step builder for input: ' + switch)
