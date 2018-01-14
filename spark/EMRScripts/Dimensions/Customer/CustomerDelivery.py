#This module for Company Delivery#############

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

SpringCommCust = sys.argv[1]
CompanyOutputArg = sys.argv[2]
CompanyFileTime = sys.argv[3]

spark = SparkSession.builder.\
    appName("CompanyDelivery").getOrCreate()

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfSpringComm = spark.read.parquet(SpringCommCust)

dfSpringComm.registerTempTable("springcomm")


#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfSpringComm = spark.sql("select a.sourceidentifier AS SRC_ID,a.companycd AS CO_CD,a.sourcesystemname as SRC_SYS_NM,"
                         + "a.firstname AS FIRST_NM,a.lastname AS LAST_NM,"
                         + "a.companyname AS COMP_NM,"
                         + "a.city AS CTY,a.stateprovince AS ST_PRV,a.zippostalcode AS ZIP_PSTL,a.country AS CNTRY,"
                         + "a.multilevelpriceidentifier AS MULTI_PRC_ID,a.billingaccountnumber AS BILL_ACCTNG_NUM,"
                         + "a.totalactivations AS TOT_ACTVNG,a.datecreatedatsource AS DT_CRT_AT_SRC,a.customertype AS CUST_TYP_NM,"
                         + "a.vipcustomerindicator AS VIP_CUST_IND,"
                         + "a.cdcindicator as CDC_IND_CD"
                         + " from springcomm a")


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(CompanyOutputArg)


spark.stop()
