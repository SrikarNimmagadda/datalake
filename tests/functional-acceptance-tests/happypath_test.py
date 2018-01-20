import boto3

import filecmp
import time

client = boto3.client('s3')
s3 = boto3.resource('s3')
cfclient = boto3.client('cloudformation', region_name='us-east-1')


def get_bucket_id_by_logical_id(stackname, logical_id):
    resource = cfclient.describe_stack_resource(
        StackName=stackname,
        LogicalResourceId=logical_id
    )
    return resource['StackResourceDetail']['PhysicalResourceId']


# Upload File to landing bucket
def upload_file_to_bucket(bucket, filename):
    data = open(filename, 'rb')
    s3.Bucket(bucket).put_object(Key=filename, Body=data)
    print('File uploaded to landing successfully')


# Wait for file in delivery bucket
def verify_file_exists_in_bucket(bucket, num_attempts, time_between_attempts):
    s3_bucket = s3.Bucket(bucket)
    for x in range(0, num_attempts):
        print(sum(1 for _ in s3_bucket.objects.all()))
        if sum(1 for _ in s3_bucket.objects.all()) > 0:
            print('File exists in delivery')
            return True
        else:
            print('Wait #' + str(x + 1) + ' for ' +
                  str(time_between_attempts) + ' sec')
            time.sleep(time_between_attempts)
    print('File not found in delivery after ' + str(num_attempts) + ' tries')
    return False


# Get the file once it appears
def download_file(bucket, filename):
    objs = client.list_objects(Bucket=bucket)['Contents']
    s3.Bucket(bucket).download_file(objs[0]['Key'], filename)
    print('File downloaded successfullly')


# Compare files
def compare_files(testfile, downloaded_file_name):
    print('Comparing local file with output file from datalake')
    return filecmp.cmp(testfile, downloaded_file_name)


def empty_bucket(bucketname):
    bucket = s3.Bucket(bucketname)
    bucket.objects.all().delete()


def test_happy_path(stackname):
    print('Running tests in stack: ' + str(stackname))

    testfile_to_upload = 'testfile'
    bucket_to_upload_to = get_bucket_id_by_logical_id(
        stackname, 'S3BucketLanding')
    bucket_to_expect_generated_file = get_bucket_id_by_logical_id(
        stackname, 'S3BucketLanding')
    downloaded_file_name = 'download1.txt'
    num_attempts = 3
    time_between_attempts = 30
    file_to_compare_against = 'testfile'

    upload_file_to_bucket(bucket_to_upload_to, testfile_to_upload)

    if verify_file_exists_in_bucket(bucket_to_expect_generated_file, num_attempts, time_between_attempts):
        download_file(bucket_to_expect_generated_file, downloaded_file_name)
    comparision = compare_files(file_to_compare_against, downloaded_file_name)
    print(comparision)
    assert comparision is True, 'Happy Path Functional acceptance test failed'
    empty_bucket(bucket_to_upload_to)
    empty_bucket(bucket_to_expect_generated_file)
