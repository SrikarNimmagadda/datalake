#
# Copied from tb-us-east-1-dl-dev-extraction on D0062 Sandbox account
#

from __future__ import print_function
import boto3
import json
import datetime
import string
import properties

s3 = boto3.client('s3', aws_access_key_id=properties.aws_access_key_id,
                  aws_secret_access_key=properties.aws_secret_access_key)
s3r = boto3.resource('s3', aws_access_key_id=properties.aws_access_key_id,
                     aws_secret_access_key=properties.aws_secret_access_key)
dynamodb = boto3.resource('dynamodb', region_name=properties.region_name,
                          aws_access_key_id=properties.aws_access_key_id, aws_secret_access_key=properties.aws_secret_access_key)


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        response = s3.head_object(Bucket=bucket, Key=key)
        # Accessing key and values information of metadata
        AcceptRanges = response['AcceptRanges']
        ContentType = response['ContentType']
        HTTPStatusCode = response['ResponseMetadata']['HTTPStatusCode']
        RetryAttempts = response['ResponseMetadata']['RetryAttempts']
        HostId = response['ResponseMetadata']['HostId']
        RequestId = response['ResponseMetadata']['RequestId']
        contentlength = response['ResponseMetadata']['HTTPHeaders']['content-length']
        x_amz_id_2 = response['ResponseMetadata']['HTTPHeaders']['x-amz-id-2']
        acceptranges = response['ResponseMetadata']['HTTPHeaders']['accept-ranges']
        server = response['ResponseMetadata']['HTTPHeaders']['server']
        last_modified = response['ResponseMetadata']['HTTPHeaders']['last-modified']
        x_amz_request_id = response['ResponseMetadata']['HTTPHeaders']['x-amz-request-id']
        etag = response['ResponseMetadata']['HTTPHeaders']['etag']
        date = response['ResponseMetadata']['HTTPHeaders']['date']
        content_type = response['ResponseMetadata']['HTTPHeaders']['content-type']
        LastModified = response['LastModified']
        ContentLength = response['ContentLength']
        ETag = response['ETag']

    try:
        obj = s3r.Object(bucket, key)
        filedata = obj.get()["Body"].read()
        filedata_formatted = filedata.decode('utf8').splitlines()
        for row in filedata_formatted:
            if filedata_formatted.index(row) == 0:
                items = row.split(',')
        table = dynamodb.Table(properties.dynamodb_table)
        id = '1'
        columnname = 'Available'
        table.put_item(
            Item={
                'id': id,
                'Columnname': columnname,
                'timestamp': str(datetime.datetime.now()),
                'bucketname': bucket,
                'ObjectName': key,
                'AcceptRanges': AcceptRanges,
                'ContentType': ContentType,
                'HTTPStatusCode': HTTPStatusCode,
                'RetryAttempts': RetryAttempts,
                'HostId': HostId,
                'RequestId': RequestId,
                'contentlength': contentlength,
                'x_amz_id_2': x_amz_id_2,
                'acceptranges': acceptranges,
                'server': server,
                'last_modified': last_modified,
                'x_amz_request_id': x_amz_request_id,
                'etag': etag,
                'date': date,
                'content_type': content_type,
                'LastModified': str(LastModified),
                'ContentLength': ContentLength,
                'ETag': ETag,
                'Columnheaders': items
            }
        )

    except:
        table = dynamodb.Table(properties.dynamodb_table)
        id = '1'
        items = 'NA'
        columnname = 'NA'
        table.put_item(
            Item={
                'id': id,
                'Columnname': columnname,
                'timestamp': str(datetime.datetime.now()),
                'bucketname': bucket,
                'ObjectName': key,
                'AcceptRanges': AcceptRanges,
                'ContentType': ContentType,
                'HTTPStatusCode': HTTPStatusCode,
                'RetryAttempts': RetryAttempts,
                'HostId': HostId,
                'RequestId': RequestId,
                'contentlength': contentlength,
                'x_amz_id_2': x_amz_id_2,
                'acceptranges': acceptranges,
                'server': server,
                'last_modified': last_modified,
                'x_amz_request_id': x_amz_request_id,
                'etag': etag,
                'date': date,
                'content_type': content_type,
                'LastModified': str(LastModified),
                'ContentLength': ContentLength,
                'ETag': ETag,
                'Columnheaders': items

            }
        )
