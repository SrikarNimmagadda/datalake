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

StorePayrollRefineInp = sys.argv[1]
StorePayrollArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("StorePayrollDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfStorePayroll  = spark.read.parquet(StorePayrollRefineInp)

dfStorePayroll.registerTempTable("storepayroll")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.storenumber as STORE_NUM,a.companycd AS CO_CD,a.report_date as RPT_DT,"
        + "a.regularhours as REG_HRS,a.overtimehours as OT_HRS,a.doubleovertimehours as DBL_OT_HRS,"
        + "a.totalhours as TTL_HRS,a.storegreeterscount as STORE_GRTERS_CNT,a.operationalasmcount as OPRAL_ASM_CNT,"
        + "a.smdcount as SMD_CNT,a.totalpayrollamount as TTL_PAYRL_AMT,"
        + "a.grossprofitperhour as GRS_PRFT_PER_HRS,a.payrollpercentofgrossproft as PAYRL_PCT_OF_GRS_PROFT,"
        + "a.totalftecount as TTL_FTE_CNT_PAYRL,a.fulltimeequivalent as FULL_TM_EQUIV_PAYRL,a.parttimeequivalent as PTTM_EQUIV_PAYRL,"
        + "a.districtleadsalesconsultantcount as DSTRC_LEAD_SALES_CNSLT_CNT_PAYRL,a.bam1count as BAM_1_CNT,"
        + "a.bam2count as BAM_2_CNT,a.storemanagercount as SM_CNT,a.mitcount as MIT_CNT "
        #+ "a.totalgrossprofit as TTL_GRS_PRFT, a.cdcindicator as CDC_IND_CD "
        + "from storepayroll a")

                 
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(StorePayrollArg)

               
spark.stop()

                   