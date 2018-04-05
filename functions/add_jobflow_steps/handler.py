""" Lambda entry point for add_jobflow_steps functions
Acts as the composition root for the application and calls other classes
to do all the work, which allows for easier unit testing of the other
components.
"""

import os
from datetime import datetime

import boto3

from cluster_finder import ClusterFinder
from step_factory import StepFactory

from step_builder_customer_master import StepBuilderCustomer
from step_builder_employee_master import StepBuilderEmployee
from step_builder_product_master import StepBuilderProduct
from step_builder_store_master import StepBuilderStore
from step_builder_tb_goal_point import StepBuilderTBGoalPoint
from step_builder_store_headcount_store_and_employee_goal import StepBuilderStoreHeadCountStoreandEmpGoal
from step_builder_store_daily_goal_forecast import StepBuilderStoreDailyGoalForecast
from step_builder_sales_leads import StepBuilderSalesLeads
from step_builder_store_traffic import StepBuilderStoreTraffic
from step_builder_store_transaction_adjustmet import StepBuilderStoreTransAdjustment
from step_builder_att_sales_actuals import StepBuilderATTSalesActuals
from step_builder_employee_transaction import StepBuilderEmpTransAdjustment
from step_builder_employee_opperational_efficiency import StepBuilderEmpOppEfficiency
from step_builder_store_customer_experience import StepBuilderStoreCustomerExperience
from step_builder_sales_details import StepBuilderSalesDetails
from step_builder_sales_kpi import StepBuilderSalesKPI

S3 = boto3.resource('s3')
CFN = boto3.client('cloudformation')
EMR = boto3.client('emr')

BUCKETS = {
    'code': os.getenv('CODE_BUCKET'),
    'data_processing_errors': os.getenv('DATA_PROCESSING_ERRORS_BUCKET'),
    'raw_customer_pii': os.getenv('RAW_CUSTOMER_PII_BUCKET'),
    'raw_hr_pii': os.getenv('RAW_HR_PII_BUCKET'),
    'raw_regular': os.getenv('RAW_REGULAR_BUCKET'),
    'discovery_customer_pii': os.getenv('DISCOVERY_CUSTOMER_PII_BUCKET'),
    'discovery_hr_pii': os.getenv('DISCOVERY_HR_PII_BUCKET'),
    'discovery_regular': os.getenv('DISCOVERY_REGULAR_BUCKET'),
    'refined_customer_pii': os.getenv('REFINED_CUSTOMER_PII_BUCKET'),
    'refined_hr_pii': os.getenv('REFINED_HR_PII_BUCKET'),
    'refined_regular': os.getenv('REFINED_REGULAR_BUCKET'),
    'delivery_customer_pii': os.getenv('DELIVERY_CUSTOMER_PII_BUCKET'),
    'delivery_regular': os.getenv('DELIVERY_REGULAR_BUCKET')
}

EMR_STACK_NAME = os.getenv('EMR_STACK_NAME')
PROCESS_METADATA_TABLE = os.getenv('PROCESS_METADATA_TABLE')


def lambda_handler(event, context):
    """Lambda entry point"""
    finder = ClusterFinder(CFN)
    clusterid = finder.find_cluster(EMR_STACK_NAME)

    builder = choose_builder(event)
    steps = builder.build_steps()

    EMR.add_job_flow_steps(
        JobFlowId=clusterid,
        Steps=steps
    )


def choose_builder(event):
    """Return a specific step builder based on a switch in the event."""
    factory = StepFactory(BUCKETS['code'])
    now = datetime.now()
    switch = event['builder']

    if switch == 'customer':
        return StepBuilderCustomer(factory, S3, BUCKETS, now)
    elif switch == 'employee':
        return StepBuilderEmployee(factory, S3, BUCKETS, now)
    elif switch == 'product':
        return StepBuilderProduct(factory, S3, BUCKETS, now)
    elif switch == 'store':
        return StepBuilderStore(factory, S3, BUCKETS, now)
    elif switch == 'tbgoalpoint':
        return StepBuilderTBGoalPoint(factory, S3, BUCKETS, now)
    elif switch == 'storeheadcountStoreandempgoals':
        return StepBuilderStoreHeadCountStoreandEmpGoal(factory, S3, BUCKETS, now)
    elif switch == 'storedlygoalfcst':
        return StepBuilderStoreDailyGoalForecast(factory, S3, BUCKETS, now)
    elif switch == 'salesleads':
        return StepBuilderSalesLeads(factory, S3, BUCKETS, now)
    elif switch == 'storetraffic':
        return StepBuilderStoreTraffic(factory, S3, BUCKETS, now)
    elif switch == 'storetransadj':
        return StepBuilderStoreTransAdjustment(factory, S3, BUCKETS, now)
    elif switch == 'attsalesactls':
        return StepBuilderATTSalesActuals(factory, S3, BUCKETS, now)
    elif switch == 'emptransadj':
        return StepBuilderEmpTransAdjustment(factory, S3, BUCKETS, now)
    elif switch == 'empoperefcny':
        return StepBuilderEmpOppEfficiency(factory, S3, BUCKETS, now)
    elif switch == 'storecustexprc':
        return StepBuilderStoreCustomerExperience(factory, S3, BUCKETS, now)
    elif switch == 'salesdtls':
        return StepBuilderSalesDetails(factory, S3, BUCKETS, now)
    elif switch == 'saleskpi':
        return StepBuilderSalesKPI(factory, S3, BUCKETS, now)
    else:
        raise Exception('Could not find a step builder for input: ' + switch)
