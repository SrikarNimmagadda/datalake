#This module will parquet ReceivingInvoice and PurchaseOrder#############

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

ATTSalesActualsInp = sys.argv[1]
output = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("ParquetATTSalesactuals").getOrCreate()
        
dfATTShippingDetail = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(ATTSalesActualsInp)

              
                
#########################################################################################################
# Reading the source data files #
#########################################################################################################

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

'''
dfReceivingInvoice.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(output)
        
dfPurchaseOrder.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(output)
'''
dfATTShippingDetail.coalesce(1).select("*"). \
    write.parquet(output + '/' + todayyear + '/' + todaymonth + '/' + 'ParquetATTSalesactuals' + FileTime);
   
    
spark.stop()  