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

ReceivingInvHistoryRefineInp = sys.argv[1]
ReceivingInvHistoryOutputArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("dfReceivingInvHistoryDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfReceivingInvHistory  = spark.read.parquet(ReceivingInvHistoryRefineInp)

dfReceivingInvHistory.registerTempTable("receiving")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.companycd as CO_CD,a.receivingid as RCV_ID,a.productsku as PROD_SKU,"
    + "a.productserialnumber as PROD_SN,a.receivingidbystore as RCV_ID_BY_STORE,a.report_date as RPT_DT,"
    + "a.receiveddate as RCV_DT,a.sourcepurchaseorderid as SRC_PO_ID,a.sourcesystemname as SRC_SYS_NM,"
    + "a.storenumber as STORE_NUM,a.referencenumber as REF_NBR,a.vendorsku as VNDR_SKU,a.quantityreceived as QTY_RCV,"
    + "a.unitcost as UNT_CST,a.totalcost as TTL_CST,a.reconciledindicator as RCNCL_IND,a.reconcileddate as RCNCL_DT,"
    + "a.correctionindicator as CRCT_IND,a.correctionreasoncode as CRCT_RSN_CD,a.receivingcomments as RCV_CMNT,"
    + "a.vendorterms as VNDR_TERMS,a.employeeid as SRC_EMP_ID,a.accountnumber as ACCT_NBR "
    + "from receiving a")
                  
                    
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1).\
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(ReceivingInvHistoryOutputArg)

               
spark.stop()

                   