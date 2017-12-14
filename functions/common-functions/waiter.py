# coding: utf-8
from __future__ import print_function

import json
import time
import urllib
import boto3

# waiter will return the bucket name when an object is dropped into the bucket this handler is triggered from
def handler(event, context):
    s3 = boto3.client('s3')
    # get the bucket name and key for the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'])
    print(key)
    key_path = event['Records'][0]['s3']['object']['key']
    
    try:
        print("Using get_waiter to wait for object to exist in s3")
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)
        # retreive the bucket and object key from the s3 response
        response = s3.head_object(Bucket=bucket, Key=key)
        return response

    except Exception:
        print(Exception)
        print('Error getting object {} from bucket {} Make sure they exist '
                'and your bucket is in the same region as this '
                'function.'.format(key, bucket))
        raise Exception
