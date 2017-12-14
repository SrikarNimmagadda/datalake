"""Route s3 objects from the landing bucket to the correct raw bucket."""
#
# Originally copied from tb-us-east-1-dl-dev-copy-extraction on D0062 Sandbox account
#

import os
import logging
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

BUCKETS = {
    'pii': os.getenv('RAW_PII_BUCKET'),
    'hr': os.getenv('RAW_HR_BUCKET'),
    'regular': os.getenv('RAW_REGULAR_BUCKET')
}

LOGGER.info(BUCKETS['pii'])

S3 = boto3.client('s3')


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """handler entry point."""

    # no logic in the main handler except passing the S3 boto object.
    # this will allow us to unit test the main logic with a mock of that service
    handle_event(event, S3)


def handle_event(event, s3_service):
    """Based on incoming object's key prefix, determines output bucket and copies object."""
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        copy_source = {'Bucket': bucket, 'Key': key}

        target_bucket = determine_target(key)

        s3_service.copy_object(Bucket=target_bucket, Key=key,
                               CopySource=copy_source)


def determine_target(key):
    """Select a target bucket name string based on an object's key's prefix."""
    if key.startswith('PII'):
        return BUCKETS['pii']
    elif key.startswith('HR'):
        return BUCKETS['hr']
    else:
        return BUCKETS['regular']
