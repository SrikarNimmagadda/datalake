#!/usr/bin/python
#######################################################################################################################
# Author           : Harikesh R
# Date Created     : 02/07/2018
# Purpose          : To transform and move data from Refined to Delivery layer for Store Recruiting Headcount
# Version          : 1.0
# Revision History :
########################################################################################################################

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
from datetime import datetime

OutputPath = sys.argv[1]
StoreRecHCInputPath = sys.argv[2]

Config = SparkConf().setAppName("StoreRecHeadcountDelivery")
SparkCtx = SparkContext(conf=Config)
spark = SparkSession.builder.config(conf=Config).\
        getOrCreate()

Log4jLogger = SparkCtx._jvm.org.apache.log4j
logger = Log4jLogger.LogManager.getLogger('store_rec_hc_ref_to_delivery')


#########################################################################################################
#                                 Read the source file                                                  #
#########################################################################################################

dfStoreCustExp = spark.read.parquet(StoreRecHCInputPath + '/StoreRecruitingHeadcount/Working').registerTempTable(
    "StoreRecHC")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfStoreRecHCFinal = spark.sql("select report_date as RPT_DT,store_number as STORE_NUM,companycd as CO_CD,"
                              "approved_headcount as APRV_HEAD_CNT,"
                              "store_managers_count as SMS_CNT,business_assistant_manager_count as BUS_ASST_MGR_CNT,"
                              "fulltime_equivalent_count as FTM_EQUIV_CNT,parttime_equivalent_count as PRTTM_EQUIV_CNT,"
                              "fulltime_floater_count as FTM_FLOATER_CNT,district_lead_sales_consultant_count as "
                              "DSTRC_LEAD_SALES_CNSLT_CNT,"
                              "mit_count as MIT_CNT,seasonal_count as SEAS_CNT,actual_headcount as ACTL_HDCT,"
                              "a.dealer_code as DLR_CD"
                              " from StoreRecHC a")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfStoreRecHCFinal.coalesce(1). \
    write.format("com.databricks.spark.csv"). \
    option("header", "true").mode("overwrite").save(OutputPath + '/' + 'Current')

dfStoreRecHCFinal.coalesce(1). \
    write.format("com.databricks.spark.csv"). \
    option("header", "true").mode("append").save(OutputPath + '/' + 'Previous')

spark.stop()
