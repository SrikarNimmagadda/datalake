"""Notification for data processing errors"""

import boto3
import os


SNS = boto3.client('sns')
STS = boto3.client('sts')
S3 = boto3.client('s3')
my_session = boto3.session.Session()

hen_name = os.getenv('HEN_NAME')
topic = os.getenv('NOTIFICATION')
data_processing = os.getenv('DATA_PROCESSING_ERRORS_BUCKET'),


def lambda_handler(event, context):

    account_id = STS.get_caller_identity()["Account"]
    print(account_id)
    my_region = my_session.region_name
    print(my_region)
    ARN = "arn:aws:sns:" + my_region + ":" + account_id + ":" + topic
    print(ARN)

    bucket = data_processing

    print(bucket)

    def get_last_modified(obj):
        return int(obj['LastModified'].strftime('%s'))
    objs = S3.list_objects(Bucket=bucket)['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)][0]

    print(last_added)

    response = SNS.publish(TopicArn=ARN,
                           Subject="Techbrands Data Processing Failure" + " " + hen_name,
                           Message="Data Processing failed for" + " " + last_added
                           )
    print(response)
