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

EmpCNCTrainingRefineInp = sys.argv[1]
EmpCNCTrainingArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("EmployeeCNCDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfEmpCNCTraining  = spark.read.parquet(EmpCNCTrainingRefineInp)

dfEmpCNCTraining.registerTempTable("employeecnc")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.companycd AS CO_CD,a.reportdate as RPT_DT,a.sourceemployeeid as SRC_EMP_ID, "
        + "a.jobtitle as JOB_TITLE,a.passfailindicator as PASS_FAIL_IND,a.compliancecompletion as CMPLY_CMPLT,"
        + "a.ongoingtrainingcompletion as ONGO_TRAING_CMPLT,a.mitcompletion as MIT_CMPLT,"
        + "a.incompletetrainingcount as NCMPL_TRAING_CNT,a.xmid as XMID,a.storenumber as STORE_NUM,"
        + "a.cdcindicator as CDC_IND_CD "
        + "from employeecnc a")

                 
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpCNCTrainingArg)

               
spark.stop()

                   