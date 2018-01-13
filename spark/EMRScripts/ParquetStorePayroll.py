from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField,FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

StorePayrollInp = sys.argv[1]
StorePayrollOp = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("StorePayroll").getOrCreate()
        
       
StoreSchema = StructType() .\
                        add("RowLabels", StringType(), True). \
                        add("SumofREG", StringType(), True). \
                        add("SumofOT", StringType(), True). \
                        add("SumofDOT", StringType(), True). \
                        add("SumofTotal", StringType(), True). \
                        add("SumofFTE", StringType(), True). \
                        add("SumofFTEC", StringType(), True). \
                        add("SumofPTEC", StringType(), True). \
                        add("SumofGreeter", StringType(), True). \
                        add("SumofDLSC", StringType(), True). \
                        add("SumofBAM1", StringType(), True). \
                        add("SumofBam2", StringType(), True). \
                        add("SumofOperationalASM", StringType(), True). \
                        add("SumofStoreManager", StringType(), True). \
                        add("SumofSMD", StringType(), True). \
                        add("SumofMIT", StringType(), True).\
                        add("SumofHeadcount", StringType(), True).\
                        add("SumofPayroll", StringType(), True). \
                        add("LocationGP", StringType(), True). \
                        add("GPPerHour", StringType(), True).\
                        add("Payroll%ofGP", StringType(), True)                       
                 
                    
dfStorePayroll = spark.read.format("com.crealytics.spark.excel").\
                option("location", StorePayrollInp).\
                option("sheetName", "Sales Reporting").\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(StoreSchema). \
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")                        


                
#########################################################################################################
# Reading the source data files #
#########################################################################################################

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

#dfStoreHeadCount.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(StoreHeadCountOp)

dfStorePayroll.coalesce(1).select("*"). \
    write.parquet(StorePayrollOp + '/' + todayyear + '/' + todaymonth + '/' + 'ParquetStorePayroll' + FileTime);
    
spark.stop()  