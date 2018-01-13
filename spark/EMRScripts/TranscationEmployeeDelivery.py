from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col

# Passing argument for reading the file

TransEmployeeInp = sys.argv[1]
TransEmpOPArg = sys.argv[2] 
FileTime = sys.argv[3]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()
        
        
        
#########################################################################################################
#                                 Reading the source data file                                         #
#########################################################################################################

dfEmployee  = spark.read.parquet(TransEmployeeInp)
                   
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################


dfEmployee.registerTempTable("employee")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfSpringComm=spark.sql("select distinct a.reportdate as RPT_DT,a.companycd as CO_CD,a.sourceemployeeid as SRC_EMP_ID,"
                     + "a.adjustmenttype as ADJMNT_TYP,a.adjustmentamount as ADJMNT_AMT,a.cdcindicator as CDC_IND_CD from employee a ")
            

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(TransEmpOPArg)

#dfSpringComm.coalesce(1).select("*"). \
#write.parquet(TransEmpOPArg + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeTranscationDelivery' + FileTime);
                
spark.stop()
