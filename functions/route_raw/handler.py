"""Route s3 objects from the landing bucket to the correct raw bucket."""
# Originally copied from tb-us-east-1-dl-dev-copy-extraction on D0062 Sandbox
# account

import boto3
import logging
import os
import datetime

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

BUCKETS = {
    'pii': os.getenv('RAW_CUSTOMER_PII_BUCKET'),
    'hr': os.getenv('RAW_HR_PII_BUCKET'),
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
    """Based on incoming object's key prefix"""
    # determines output bucket and copies object.
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key1 = record['s3']['object']['key']

        copy_source = {'Bucket': bucket, 'Key': key1}

        target_bucket = determine_target('key')

        myDict = {'ATTHistorical_AT_TMyResultsHistoricalAnalysis.RptKPI_Grid_SFTP':
                  'AT_T_MyResults_RPT',
                  'ATTHistorical_AT_TMyResultsHistoricalAnalysis.2_Grid_SFTP':
                  'AT_T_MyResults_SFTP',
                  'C&C': 'C&C Training Report', 'CategoryNumber':
                  'ProductCategory',
                  'SalesTransactions': 'Sales', 'SpringScorecardGoals_GoalsforSQL': 'StoreGoals',
                  'StoreTraffic': 'StoreTraffic',
                  'HR_Employee': 'Employee', 'Inventory': 'Inventory',
                  'Location': 'Location',
                  'Operational': 'Operational Efficiency',
                  'PII_Customer': 'Customer', 'BAE': 'BAE', 'DTVNow': 'DTV',
                  'MultiTracker_SpringMobileMultiTracker': 'MultiTracker',
                  'OperationalEFC_TotalLoss': 'EmpOperationalEfficiency',
                  'ScoreCardGoals_GoalsforSQL': 'StoreGoals',
                  'DealerCodes': 'ATTDealerCodes',
                  'SpringMobileStoreList': 'SpringMobileStore',
                  'Report202020': 'SalesLeads',
                  'ProductIdentifier': 'ProductIdentifier',
                  'Product': 'Product', 'PurchaseOrder': 'PurchaseOrder',
                  'TransAdjStore': 'StoreTransAdjustments/StoreTrans',
                  'MiscAdjustments': 'StoreTransAdjustments/MISC_input',
                  'ReceivingInvoiceHistory': 'ReceivingInvoiceHistory',
                  'GoalPoints_EmployeeScorecard': 'TBGoalPointEmployee',
                  'GoalPoints_StoreScorecard': 'TBGoalPointStore',
                  'Coupons': 'Coupons',
                  'ReportingDailyGP_Q4ForecastbyDay':
                  'StoreDailyGoalForecast',
                  'EmpGpGoal': 'Employee_GP_Goal_SFTP', 'TransAdjEMP': 'EmpTransAdjustment',
                  'CustExp_OurPromiseFIle': 'StoreCustomerExperience',
                  'ApprovedFTE_CurrentHeadcount':
                  'StoreRecruitingHeadcount',
                  'CCTrainingReport_CCAuditReport': 'EmpCNCTraining'}

        myKey = [v for k, v in myDict.items() if key1.startswith(k)]
        today = datetime.date.today()
        sysdate = today.strftime("%m-%d-%Y")
        S3.delete_object(Bucket=target_bucket, Key=myKey[0] + '/Working/' + myKey[0])

        S3.copy_object(Bucket=target_bucket, Key=myKey[0] + '/Working/' + myKey[0], CopySource=copy_source)

        S3.copy_object(Bucket=target_bucket, Key=myKey[0] + '/loaded_date=' + str(sysdate) + '/' + key1, CopySource=copy_source)
        
        s3.delete_object(Bucket=bucket, Key=key1)

def determine_target(key):
    """Select a target bucket name string based on an object's key's prefix."""
    if key.startswith('PII'):
        return BUCKETS['pii']
    elif key.startswith('HR'):
        return BUCKETS['hr']
    else:
        return BUCKETS['regular']
