# Understanding and writing functional acceptance tests
Inorder to execute functional tests, a brand new environment is automatically created and the tests in the 'tests/functional-acceptance-test' folder of the solution are executed in this new environment. In this solution pytest is used to execute the tests

There is an example test provided in the solution. In the example, a test file is dropped in the landing bucket of the datalake and an ouput file is expected in a bucket. Here is a breakdown of the happy path test

The example contains 2 file

1. [conftest.py][Test Details]

```python
def get_bucket_id_by_logical_id(stackname, logical_id):
    ...
```
This function is used to retrieve the Bucket names as created by the cloudformation template in the serverless file

```python
def upload_file_to_bucket(bucket, filename):
    ...
```
This method is used to upload a file. In the case of the happy path example, it is used upload a file to the S3 landing bucket of the datalake, but it can be used to upload a file to any other bucket of the data lake


```python
def verify_file_exists_in_bucket(bucket, num_attempts, time_between_attempts):
    ...
```
This function is used to check if a bucket contains a file or if it is empty. `bucket` is the name of the bucket to check in. The `num_attempts` and the `time_between_attempts` are used for retrying when a file does  not exist in the bucket to wait and retry for a set number of times before failing the test.

```python
def download_file(bucket, filename):
    ...
```
This function is used to download the file in order to be able to compare with an expected version

```python
def compare_files(testfile, downloaded_file_name):
    ...
```
This function is used to compare the expected file to the file downloaded from the specified bucket in datalake

```python
def empty_bucket(bucketname):
    ...
```
This function is used to empty the s3 bucket after the tests are run inorder to facilitate destroying the test environment.


```python
def test_happy_path(stackname):
    ...
```
This method is used to run the test


## Test details

Defining the source, destination file names and the necessary bucket names

```python
testfile_to_upload = 'testfile'
bucket_to_upload_to = get_bucket_id_by_logical_id(stackname, 'S3BucketLanding')
bucket_to_expect_generated_file = get_bucket_id_by_logical_id(stackname, 'S3BucketLanding')
downloaded_file_name = 'download1.txt'
num_attempts = 3
time_between_attempts = 30
file_to_compare_against = 'testfile'
```

Upload the test file to landing bucket
```python
upload_file_to_bucket(bucket_to_upload_to, testfile_to_upload)
```

Verify file exists or retry, download and compare file with copy to pass the test
```python
if verify_file_exists_in_bucket(bucket_to_expect_generated_file, num_attempts, time_between_attempts):
    download_file(bucket_to_expect_generated_file,downloaded_file_name)
comparision = compare_files(file_to_compare_against,downloaded_file_name)
print(comparision)
assert comparision is True, 'Happy Path Functional acceptance test failed'
```