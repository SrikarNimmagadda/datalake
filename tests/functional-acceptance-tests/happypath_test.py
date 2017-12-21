import boto3
import pytest
from botocore.client import Config
import filecmp
import time

client = boto3.client('s3')
s3 = boto3.resource('s3')
cfclient = boto3.client('cloudformation',region_name='us-east-1')


def get_bucket_id_by_logical_id(stackname, logical_id):
    resource = cfclient.describe_stack_resource(
        StackName=stackname,
        LogicalResourceId=logical_id
    )
    return resource['StackResourceDetail']['PhysicalResourceId']
# Upload File to landing bucket
def upload_to_landing(landing_bucket, landing_bucket_filename):
    data = open(landing_bucket_filename, 'rb')
    s3.Bucket(landing_bucket).put_object(Key=landing_bucket_filename, Body=data)
    print('File uploaded to landing successfully')

# Wait for file in delivery bucket
def verify_file_exists(delivery_bucket, num_attempts, time_between_attempts):
    my_delivery_bucket = s3.Bucket(delivery_bucket)
    for x in range(0,num_attempts):
        print(sum(1 for _ in my_delivery_bucket.objects.all()))
        if sum(1 for _ in my_delivery_bucket.objects.all())>0:
            print('File exists in delivery')
            return True
        else:
            print('Wait #' + str(x+1) + ' for ' + str(time_between_attempts) + ' sec')
            time.sleep(time_between_attempts)
    print('File not found in delivery after ' + str(num_attempts) + ' tries')
    return False

# Get the file once it appears
def download_file(delivery_bucket, delivery_bucket_filename):
    objs = client.list_objects(Bucket=delivery_bucket)['Contents']
    obj = s3.Bucket(delivery_bucket).download_file(objs[0]['Key'],delivery_bucket_filename)
    print('File downloaded successfullly')

# Compare files 
def compare_files(testfile, delivery_bucket_filename):
    print('Comparing local file with output file from datalake')
    return filecmp.cmp(testfile, delivery_bucket_filename)

def empty_bucket(bucketname):
    bucket = s3.Bucket(bucketname)
    bucket.objects.all().delete()

def test_happy_path(stackname):
    print('Running tests in stack: ' + str(stackname))

    landing_bucket_filename = 'testfile'
    landing_bucket = get_bucket_id_by_logical_id(stackname, 'S3BucketLanding')
    delivery_bucket = get_bucket_id_by_logical_id(stackname, 'S3BucketLanding')
    delivery_bucket_filename = 'download1.txt'
    num_attempts = 3
    time_between_attempts = 30
    test_downloaded_file = 'testfile'

    upload_to_landing(landing_bucket, landing_bucket_filename)

    if verify_file_exists(delivery_bucket, num_attempts, time_between_attempts):
        download_file(delivery_bucket,delivery_bucket_filename)
    comparision = compare_files(test_downloaded_file,delivery_bucket_filename)
    print(comparision)
    assert comparision is True, 'Happy Path Functional acceptance test failed'
    empty_bucket(landing_bucket)
    empty_bucket(delivery_bucket)

