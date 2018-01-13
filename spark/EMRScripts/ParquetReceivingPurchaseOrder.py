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

ReceivingInvoiceInp = sys.argv[1]
PurchaseOrderInp = sys.argv[2]
output = sys.argv[3]
FileTime = sys.argv[4]

spark = SparkSession.builder.\
        appName("ParquetReceivingPurchaseOrder").getOrCreate()
        
dfReceivingInvoice = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(ReceivingInvoiceInp)

dfPurchaseOrder = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(PurchaseOrderInp)
                   
                
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
dfReceivingInvoice.coalesce(1).select("*"). \
    write.parquet(output + '/' + todayyear + '/' + todaymonth + '/' + 'ParquetReceivingInvoice' + FileTime);
    
dfPurchaseOrder.coalesce(1).select("*"). \
    write.parquet(output + '/' + todayyear + '/' + todaymonth + '/' + 'ParquetPurchaseOrder' + FileTime);
    
spark.stop()  