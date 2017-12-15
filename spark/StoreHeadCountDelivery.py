#This module for Product Delivery#############

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

StoreHeadCountRefineInp = sys.argv[1]
StoreHeadCountOutputArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("StoreHeadCountDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfStoreHeadCount  = spark.read.parquet(StoreHeadCountRefineInp)

dfStoreHeadCount.registerTempTable("storeheadcount")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 


dfSpringComm = spark.sql("select distinct a.storenumber as STORE_NUM,a.companycd as CO_CD,a.report_date as RPT_DT,"
        + "a.plannedftecount as PLN_FTE_CNT,a.rdproposedchangecount as RD_PRPSD_CHANGE_CNT,a.approvedftecount as APRV_FTE_CNT,"
        + "a.approvedheadcount as APRV_HEAD_CNT,a.storemanagercount as SMS_CNT,a.businessassistantmanager1 as BUS_ASST_MGR_1_CNT, "
        + "a.businessassistantmanager2 as BUS_ASST_MGR_2_CNT,a.fulltimeequivalent as FTM_EQUIV,a.parttimeequivalent as PRTTM_EQUIV,"
        + "a.fulltimeflotercount as FTM_FLOATER_CNT,a.dlscdistrictleadsalesconsultantcount as DSTRC_LEAD_SALES_CNSLT_CNT,"
        + "a.mitcount as MIT_CNT,a.rdadjustedheadcount as RD_ADJSTD_HDCT,a.actualheadcount as ACTL_HDCT,a.underage as UNDERAGE,"
        + "a.overage as OVRAGE " #",a.cdcindicator as CDC_IND_CD "
		+ "from storeheadcount a")
                  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(StoreHeadCountOutputArg)

               
spark.stop()

                   