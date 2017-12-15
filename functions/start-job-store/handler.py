#
# Copied from tb-us-east-1-dl-dev-EMR-StoreJobs on D0062 Sandbox account
#

import os
#from boto3.client import InstanceGroups
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

    raw_file_list = []
    # discovery_file_list = []
    # refined_file_list = []

    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/locationMasterList", raw_file_list)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/BAE", raw_file_list)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/dealer", raw_file_list)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/multiTracker", raw_file_list)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/springMobile", raw_file_list)

    today_time = datetime.now().strftime('%Y%m%d%H%M')
    today_year = datetime.now().strftime('%Y')
    todaymonth = datetime.now().strftime('%m')

    dis_location_path = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "location" + today_time + "/*.parquet"
    dis_dealer_path = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "Dealer" + today_time + "/*.parquet"
    dis_multi_path = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "multiTracker" + today_time + "/*.parquet"
    dis_spring_path = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "springMobile" + today_time + "/*.parquet"
    dis_bae_path = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "BAE" + today_time + "/*.parquet"

    ref_att_dealer_path = "s3n://tb-us-east-1-dev-refined-regular/Store/" + today_year + \
        "/" + todaymonth + '/' + "ATTDealerCodeRefine" + today_time + "/*.parquet"
    ref_association_path = "s3n://tb-us-east-1-dev-refined-regular/Store/" + today_year + \
        "/" + todaymonth + '/' + "StoreDealerAssociationRefine" + today_time + "/*.parquet"
    ref_store_refine_path = "s3n://tb-us-east-1-dev-refined-regular/Store/" + \
        today_year + "/" + todaymonth + '/' + "StoreRefined" + today_time + "/*.parquet"

    tech_brand_op_name = "s3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/"

    # for file in raw_file_list:
    #    print(file)

    step_arg1 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/LocationMasterRQ4Parquet.py",
        raw_file_list[0],
        raw_file_list[1],
        raw_file_list[2],
        raw_file_list[3],
        raw_file_list[4],
        's3n://tb-us-east-1-dev-discovery-regular/Store',
        today_time
    ]

    step_arg2 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreRefined.py",
        dis_location_path,
        dis_bae_path,
        dis_dealer_path,
        dis_spring_path,
        dis_multi_path,
        's3n://tb-us-east-1-dev-refined-regular/Store',
        today_time
    ]

    step_arg3 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeRefine.py",
        dis_dealer_path,
        's3n://tb-us-east-1-dev-refined-regular/Store',
        today_time
    ]

    step_arg4 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationRefine.py",
        dis_dealer_path,
        's3n://tb-us-east-1-dev-refined-regular/Store',
        today_time
    ]

    step_arg5 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimTechBrandHierarchy.py",
        ref_store_refine_path,
        ref_att_dealer_path,
        's3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/']

    step_arg6 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeDelivery.py",
        ref_att_dealer_path,
        's3n://tb-us-east-1-dev-delivery-regular/WT_ATT_DELR_CDS/Current'
    ]

    step_arg7 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationDelivery.py",
        ref_association_path,
        's3n://tb-us-east-1-dev-delivery-regular/WT_STORE_DELR_CD_ASSOC/Current/']

    step_arg8 = [
        "/usr/bin/spark-submit",
        "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
        "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreDelivery.py",
        ref_att_dealer_path,
        ref_association_path,
        ref_store_refine_path,
        tech_brand_op_name,
        's3n://tb-us-east-1-dev-delivery-regular/WT_STORE/Current/'
    ]

    step1 = {
        "Name": "CSVToParquet",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg1
        }
    }

    step2 = {
        "Name": "StoreRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg2
        }
    }

    step3 = {
        "Name": "ATTDealerRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg3
        }
    }

    step4 = {
        "Name": "StoreDealerAssociationRefinery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg4
        }
    }

    step5 = {
        "Name": "TechBrandHierarchy",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg5
        }
    }

    step6 = {
        "Name": "DealerCodeDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg6
        }
    }

    step7 = {
        "Name": "StoreDealerAssociationDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg7
        }
    }

    step8 = {
        "Name": "DimStoreDelivery",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_arg8
        }
    }

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


def update_file(bucketname, prefix, listname):
    bucket = S3.Bucket(bucketname)
    data = [obj for obj in list(bucket.objects.filter(
        Prefix=prefix)) if obj.key != prefix]
    length = len(data)
    # print(length)
    i = 0
    for obj in data:
        i = i + 1
        if i == length:
            str1 = "s3n://" + bucketname + '/' + obj.key
            listname.append(str1)