#
# Copied from tb-us-east-1-dl-dev-EMR-StoreJobs on D0062 Sandbox account
#

import os
# from boto3.client import InstanceGroups
from datetime import datetime

import boto3

S3 = boto3.resource('s3')
CONN = boto3.client('emr')

BUCKETS = {
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


def lambda_handler(event, context):

    date_parts = {
        'today_time': datetime.now().strftime('%Y%m%d%H%M'),
        'today_year': datetime.now().strftime('%Y'),
        'todaymonth': datetime.now().strftime('%m')
    }

    discovery_paths = build_discovery_paths(
        BUCKETS['discovery_regular'], date_parts)

    refined_paths = build_refined_paths(
        BUCKETS['refined_regular'], date_parts)

    # for file in raw_file_list:
    #    print(file)

    step1 = BuildStepLocationMasterRQ4ToParquet(date_parts['today_time'])
    step2 = BuildStepDimStoreRefined(discovery_paths, date_parts['today_time'])
    step3 = BuildStepATTDealerCodeRefined(
        discovery_paths['dealer'], date_parts['today_time'])
    step4 = BuildStepStoreDealerCodeAssociationRefine(
        discovery_paths['dealer'], date_parts['today_time'])
    step5 = BuildStepDimTechBrandHierarchy(refined_paths)
    step6 = BuildStepAttDealerCodeDelivery(refined_paths['att_dealer'])
    step7 = BuildStepStoreDealerCodeAssociationDelivery(
        refined_paths['association'])
    step8 = BuildStepDimStoreDelivery(refined_paths)

    cluster_id = CONN.run_job_flow(
        Name='GameStopCluster',
        Instances={
            'InstanceGroups': [{
                'Name': 'Master Instance Group',
                'InstanceRole': 'MASTER',
                'InstanceCount': 1,
                'InstanceType': 'm4.large',
                'Market': 'ON_DEMAND'
            }, {
                'Name': 'Slave Instance Group',
                'InstanceRole': 'CORE',
                'InstanceCount': 2,
                'InstanceType': 'm4.large',
                'Market': 'ON_DEMAND'
            }],
            'Ec2SubnetId': 'subnet-5b38da67',
            'Ec2KeyName': 'GameStopKeyPair',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': True,
        },
        LogUri='s3://gs-us1-sandbox-jar/EMRLogs/',
        ReleaseLabel='emr-5.8.0',
        Applications=[{'Name': 'Spark'}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Steps=[step1, step2, step3, step4, step5, step6, step7, step8]
    )

    print cluster_id


def build_discovery_paths(bucket, date_parts):
    return {
        'location': build_path(bucket, date_parts, 'Store', 'location'),
        'dealer': build_path(bucket, date_parts, 'Store', 'Dealer'),
        'multi': build_path(bucket, date_parts, 'Store', 'multiTracker'),
        'spring': build_path(bucket, date_parts, 'Store', 'springMobile'),
        'bae': build_path(bucket, date_parts, 'Store', 'BAE')
    }


def build_refined_paths(bucket, date_parts):
    return {
        'att_dealer': build_path(bucket, date_parts, 'Store', 'ATTDealerCodeRefine'),
        'association': build_path(bucket, date_parts, 'Store', 'StoreDealerAssociationRefine'),
        'store_refine': build_path(bucket, date_parts, 'Store', 'StoreRefined')
    }


def build_path(bucket, date_parts, domain, name):
    return 's3n://' + bucket + '/' + domain + '/' + \
        date_parts['year'] + '/' + date_parts['month'] + '/' + \
        name + date_parts['time'] + '/*.parquet'


def update_file(bucketname, filter_prefix, listname):
    bucket = S3.Bucket(bucketname)
    data = [obj for obj in list(bucket.objects.filter(
        Prefix=filter_prefix)) if obj.key != filter_prefix]
    length = len(data)
    # print(length)
    i = 0
    for obj in data:
        i = i + 1
        if i == length:
            str1 = "s3n://" + bucketname + '/' + obj.key
            listname.append(str1)


def BuildStepLocationMasterRQ4ToParquet(TimePart):
    raw_file_list = []

    # TODO: refactor these calls to a function that works thru a list of filter prefixes
    update_file(BUCKETS['raw_regular'],
                'Store/locationMasterList', raw_file_list)
    update_file(BUCKETS['raw_regular'],
                'Store/BAE', raw_file_list)
    update_file(BUCKETS['raw_regular'],
                'Store/dealer', raw_file_list)
    update_file(BUCKETS['raw_regular'],
                'Store/multiTracker', raw_file_list)
    update_file(BUCKETS['raw_regular'],
                'Store/springMobile', raw_file_list)

    args = [
        "/usr/bin/spark-submit",
        "--jars",
        "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/LocationMasterRQ4Parquet.py",
        raw_file_list[0],
        raw_file_list[1],
        raw_file_list[2],
        raw_file_list[3],
        raw_file_list[4],
        's3n://tb-us-east-1-dev-discovery-regular/Store',
        TimePart
    ]

    step = {
        "Name": "CSVToParquet",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepDimStoreRefined(DiscoveryPaths, TimePart):

    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreRefined.py",
        DiscoveryPaths['location'],
        DiscoveryPaths['bae'],
        DiscoveryPaths['dealer'],
        DiscoveryPaths['spring'],
        DiscoveryPaths['multi'],
        's3n://tb-us-east-1-dev-refined-regular/Store',
        TimePart
    ]

    step = {
        "Name": "StoreRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepATTDealerCodeRefined(DealerPath, TimePart):
    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeRefine.py",
        DealerPath,
        's3n://tb-us-east-1-dev-refined-regular/Store',
        TimePart
    ]

    step = {
        "Name": "ATTDealerRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepStoreDealerCodeAssociationRefine(DealerPath, TimePart):
    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationRefine.py",
        DealerPath,
        's3n://tb-us-east-1-dev-refined-regular/Store',
        TimePart
    ]

    step = {
        "Name": "StoreDealerAssociationRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepDimTechBrandHierarchy(RefinedPaths):
    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimTechBrandHierarchy.py",
        RefinedPaths['store_refine'],
        RefinedPaths['att_dealer'],
        's3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/']

    step = {
        "Name": "TechBrandHierarchy",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepAttDealerCodeDelivery(AttDealerPath):
    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeDelivery.py",
        AttDealerPath,
        's3n://tb-us-east-1-dev-delivery-regular/WT_ATT_DELR_CDS/Current'
    ]

    step = {
        "Name": "DealerCodeDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepStoreDealerCodeAssociationDelivery(AssociationPath):
    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationDelivery.py",
        AssociationPath,
        's3n://tb-us-east-1-dev-delivery-regular/WT_STORE_DELR_CD_ASSOC/Current/']

    step = {
        "Name": "StoreDealerAssociationDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step


def BuildStepDimStoreDelivery(RefinedPaths):
    tech_brand_op_name = "s3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/"

    args = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreDelivery.py",
        RefinedPaths['att_dealer'],
        RefinedPaths['association'],
        RefinedPaths['store_refine'],
        tech_brand_op_name,
        's3n://tb-us-east-1-dev-delivery-regular/WT_STORE/Current/'
    ]

    step = {
        "Name": "DimStoreDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': args
        }
    }

    return step
