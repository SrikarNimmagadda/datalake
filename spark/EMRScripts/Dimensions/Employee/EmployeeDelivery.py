#This module for Employee Delivery#############

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

EmpRefineInp = sys.argv[1]
EmpOutputArg = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
    appName("EmployeeDelivery").getOrCreate()

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfSpringComm = spark.read.parquet(EmpRefineInp)

dfSpringComm.registerTempTable("spring")


#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfSpringComm = spark.sql("select a.companycd as CO_CD,a.sourceemployeeid as SRC_EMP_ID,a.sourcesystemname as SRC_SYS_NM,"
                         + "a.workdayid as WRKDAY_ID,a.name as NME,"
                         + "a.gender as GNDR,a.workemail as WRK_EMAIL,"
                         + "a.status as STAT_IND,a.parttimeindicator as PTTM_IND,a.language as LANG,a.title as JOB_TITLE,"
                         + "a.jobrole as JOB_ROLE,a.city as CTY,a.stateprovision as ST_PROVN,a.country as CNTRY,"
                         + "a.startdate as START_DT,a.terminationdate as TRMNTN_DT,a.terminationnotes as TRMNTN_NT,"
                         + "a.commissiongroup as CMSN_GRP,a.commissiontype AS COMP_TYP,a.employeemanagername as EMP_MGR_ID,"
                         + "a.cdcindicator as CDC_IND_CD from Spring a")


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(EmpOutputArg)


spark.stop()
