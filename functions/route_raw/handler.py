
"""Route s3 objects from the landing bucket to the correct raw bucket."""
#
# Originally copied from tb-us-east-1-dl-dev-copy-extraction on D0062 Sandbox
# account
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

S3 = boto3.client('s3')


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """handler entry point."""

    LOGGER.info(BUCKETS['pii'])

    # no logic in the main handler except passing the S3 boto object.
    # this will allow us to unit test the main logic with a mock of that
    # service
    handle_event(event, S3)


def handle_event(event, s3_service):
    # Based on incoming object's key prefix,determines output bucket and copies object.
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key1 = record['s3']['object']['key']

        copy_source = {'Bucket': bucket, 'Key': key1}

        target_bucket = determine_target('key')

        if key1.startswith('AT'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='AT_T_MyResults/' + key1, CopySource=copy_source)

        elif key1.startswith('Approved'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='Approved FTE By Location/' + key1, CopySource=copy_source)

        elif key1.startswith('B2B'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='B2B/' + key1, CopySource=copy_source)

        elif key1.startswith('C&C'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='C&C Training Report/' + key1, CopySource=copy_source)

        elif key1.startswith('CategoryNumber'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='Product Category/' + key1, CopySource=copy_source)

        elif key1.startswith('SalesTransactions'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='SalesTransactions/' + key1, CopySource=copy_source)

        elif key1.startswith('DealerCodes'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='DealerCodes/' + key1, CopySource=copy_source)

        elif key1.startswith('Employee'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='Employee/' + key1, CopySource=copy_source)

        elif key1.startswith('Inventory'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='Inventory/' + key1, CopySource=copy_source)

        elif key1.startswith('Inventory'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='Inventory/' + key1, CopySource=copy_source)

        elif key1.startswith('Location'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='Location/' + key1, CopySource=copy_source)

        elif key1.startswith('Operational'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='Operational Efficiency/' + key1, CopySource=copy_source)

        elif key1.startswith('PII_Customer'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='Customer/' + key1, CopySource=copy_source)

        elif key1.startswith('Spring'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='SpringSFTP/' + key1, CopySource=copy_source)

        elif key1.startswith('BAE'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='BAE/' + key1, CopySource=copy_source)

        elif key1.startswith('DTV'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='DTV/' + key1, CopySource=copy_source)

        elif key1.startswith('Multi'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='MultiTracker/' + key1, CopySource=copy_source)

        elif key1.startswith('SpringMobile'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='SpringMobile/' + key1, CopySource=copy_source)

        elif key1.startswith('ProductIdentifier'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='ProductIdentifier/' + key1, CopySource=copy_source)

        elif key1.startswith('PurchaseOrder'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='PurchaseOrder/' + key1, CopySource=copy_source)

        elif key1.startswith('ReceivingInvoiceHistory'):
            s3_service.copy_object(Bucket=target_bucket, 
            Key='ReceivingInvoiceHistory/' + key1, CopySource=copy_source)

        elif key1.startswith('Goal_Points'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='Goal_Points/' + key1, CopySource=copy_source)

        elif key1.startswith('Coupons'):
            s3_service.copy_object(Bucket=target_bucket,
            Key='Coupons/' + key1, CopySource=copy_source)

        else:
            print("Folder does not exist")


def determine_target(key):
    """Select a target bucket name string based on an object's key's prefix."""
    if key.startswith('PII'):
        return BUCKETS['pii']
    elif key.startswith('HR'):
        return BUCKETS['hr']
    else:
        return BUCKETS['regular']
