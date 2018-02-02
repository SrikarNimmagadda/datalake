"""Route s3 objects from the landing bucket to the correct raw bucket."""
# Originally copied from tb-us-east-1-dl-dev-copy-extraction on D0062 Sandbox
# account

import boto3
import logging
import os

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

BUCKETS = {
    'pii': os.getenv('RAW_PII_BUCKET'),
    'hr': os.getenv('RAW_HR_BUCKET'),
    'regular': os.getenv('RAW_REGULAR_BUCKET')
}

S3 = boto3.client('s3')


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """handler entry point."""

    LOGGER.info(BUCKETS['pii'])

    # no logic in the main handler except passing the S3 boto object.
    # this will allow us to unit test the main logic with a mock of that
    # service
    # handle_event(event, S3)


def handle_event(event, s3_service):
    """Based on incoming object's key prefix"""
    # determines output bucket and copies object.
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key1 = record['s3']['object']['key']

        copy_source = {'Bucket': bucket, 'Key': key1}

        target_bucket = determine_target('key')

        myDict = {'AT': 'AT_T_MyResults', 'Approved': 'Approved FTE By Location/', 'B2B': 'B2B/', 'C&C': 'C&C Training Report/', 'CategoryNumber': 'Product Category/',
                  'SalesTransactions': 'SalesTransactions/', 'DealerCodes': 'DealerCodes/', 'Employee': 'Employee/', 'Inventory': 'Inventory/', 'Location': 'Location/',
                  'Operational': 'Operational Efficiency/', 'PII_Customer': 'Customer/', 'Spring': 'SpringSFTP/', 'BAE': 'BAE/', 'DTV': 'DTV/', 'Multi': 'MultiTracker/',
                  'SpringMobile': 'SpringMobile/', 'ProductIdentifier': 'ProductIdentifier/', 'PurchaseOrder': 'PurchaseOrder/', 'ReceivingInvoiceHistory': 'ReceivingInvoiceHistory/',
                  'Goal_Points': 'Goal_Points/', 'Coupons': 'Coupons/'}

        myKey = [v for k, v in myDict.items() if key1.startswith(k)]

        s3_service.copy_object(Bucket=target_bucket,
                               Key=myKey + key1,
                               CopySource=copy_source)


def determine_target(key):
    """Select a target bucket name string based on an object's key's prefix."""
    if key.startswith('PII'):
        return BUCKETS['pii']
    elif key.startswith('HR'):
        return BUCKETS['hr']
    else:
        return BUCKETS['regular']
