#
# Copied from tb-us-east-1-dl-dev-EMR-StoreJobs on D0062 Sandbox account
#

import sys
import time

import boto3
from array import array
#from boto3.client import InstanceGroups
from datetime import datetime

s3 = boto3.resource('s3', aws_access_key_id='redacted',
                    aws_secret_access_key='redacted')
conn = boto3.client('emr', region_name='us-east-1',
                    aws_access_key_id='redacted', aws_secret_access_key='redacted')


def lambda_handler(event, context):

    rawFileList = []
    discoveryFileList = []
    refinedFileList = []

    def update_file(bucketname, prefix, listName):
        bucket = s3.Bucket(bucketname)
        data = [obj for obj in list(bucket.objects.filter(
            Prefix=prefix)) if obj.key != prefix]
        length = len(data)
        # print(length)
        i = 0
        for obj in data:
            i = i + 1
            if i == length:
                str1 = "s3n://" + bucketname + '/' + obj.key
                listName.append(str1)

    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/locationMasterList", rawFileList)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/BAE", rawFileList)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/dealer", rawFileList)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/multiTracker", rawFileList)
    update_file("tb-us-east-1-dev-raw-regular",
                "tb-us-east-1-dev-raw-emr/Store/springMobile", rawFileList)

    TodayTime = datetime.now().strftime('%Y%m%d%H%M')
    todayyear = datetime.now().strftime('%Y')
    todaymonth = datetime.now().strftime('%m')

    DisLocationPath = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "location" + TodayTime + "/*.parquet"
    DisDealerPath = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "Dealer" + TodayTime + "/*.parquet"
    DisMultiPath = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "multiTracker" + TodayTime + "/*.parquet"
    DisSpringPath = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "springMobile" + TodayTime + "/*.parquet"
    DisBAEPath = "s3n://tb-us-east-1-dev-discovery-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "BAE" + TodayTime + "/*.parquet"

    RefATTDealerPath = "s3n://tb-us-east-1-dev-refined-regular/Store/" + todayyear + \
        "/" + todaymonth + '/' + "ATTDealerCodeRefine" + TodayTime + "/*.parquet"
    RefAssociationPath = "s3n://tb-us-east-1-dev-refined-regular/Store/" + todayyear + \
        "/" + todaymonth + '/' + "StoreDealerAssociationRefine" + TodayTime + "/*.parquet"
    RefStoreRefinePath = "s3n://tb-us-east-1-dev-refined-regular/Store/" + \
        todayyear + "/" + todaymonth + '/' + "StoreRefined" + TodayTime + "/*.parquet"

    TechBrandOPName = "s3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/"

    # for file in rawFileList:
    #    print(file)

    step_arg1 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/LocationMasterRQ4Parquet.py",
                 rawFileList[0], rawFileList[1], rawFileList[2], rawFileList[3], rawFileList[4],
                 's3n://tb-us-east-1-dev-discovery-regular/Store', TodayTime]

    step_arg2 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreRefined.py",
                 DisLocationPath, DisBAEPath, DisDealerPath, DisSpringPath, DisMultiPath,
                 's3n://tb-us-east-1-dev-refined-regular/Store', TodayTime]

    step_arg3 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeRefine.py",
                 DisDealerPath, 's3n://tb-us-east-1-dev-refined-regular/Store', TodayTime]

    step_arg4 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationRefine.py",
                 DisDealerPath, 's3n://tb-us-east-1-dev-refined-regular/Store', TodayTime]

    step_arg5 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/DimTechBrandHierarchy.py",
                 RefStoreRefinePath, RefATTDealerPath, 's3n://tb-us-east-1-dev-delivery-regular/Store/Store_Hier/Current/']

    step_arg6 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/ATTDealerCodeDelivery.py",
                 RefATTDealerPath, 's3n://tb-us-east-1-dev-delivery-regular/WT_ATT_DELR_CDS/Current']

    step_arg7 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/StoreDealerCodeAssociationDelivery.py",
                 RefAssociationPath, 's3n://tb-us-east-1-dev-delivery-regular/WT_STORE_DELR_CD_ASSOC/Current/']

    step_arg8 = ["/usr/bin/spark-submit",
                 "--jars", "s3n://tb-us-east-1-dev-jar/EMRJars/spark-csv_2.11-1.5.0.jar,s3n://tb-us-east-1-dev-jar/EMRJars/spark-excel_2.11-0.8.6.jar",
                 "s3n://tb-us-east-1-dev-script/EMRScripts/DimStoreDelivery.py",
                 RefATTDealerPath, RefAssociationPath, RefStoreRefinePath, TechBrandOPName, 's3n://tb-us-east-1-dev-delivery-regular/WT_STORE/Current/']

    step1 = {"Name": "CSVToParquet",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg1

             }

             }

    step2 = {"Name": "StoreRefinery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg2

             }

             }

    step3 = {"Name": "ATTDealerRefinery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg3

             }

             }

    step4 = {"Name": "StoreDealerAssociationRefinery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg4

             }

             }
    step5 = {"Name": "TechBrandHierarchy",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg5

             }

             }

    step6 = {"Name": "DealerCodeDelivery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg6

             }

             }

    step7 = {"Name": "StoreDealerAssociationDelivery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg7

             }

             }

    step8 = {"Name": "DimStoreDelivery",

             'ActionOnFailure': 'CONTINUE',

             'HadoopJarStep': {

                 'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',

                 'Args': step_arg8

             }

             }

    cluster_id = conn.run_job_flow(
        Name='GameStopCluster',
        Instances={
            'InstanceGroups': [{
                'Name': 'Master Instance Group',
                'InstanceRole': 'MASTER',
                'InstanceCount': 1,
                'InstanceType': 'm4.large',
                'Market': 'ON_DEMAND'
            },
                {
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
        Applications=[
            {
                'Name': 'Spark'


            }],

        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',

        Steps=[step1, step2, step3, step4, step5, step6, step7, step8]
    )

    print(cluster_id)
