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

spark = SparkSession.builder.\
        appName("DimStoreInventory").getOrCreate()

StoreInventoryIn = sys.argv[1]
StoreInvOut = sys.argv[2]
FileTime = sys.argv[3]
        
#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################
 
				             
												
dfStoreInventory = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(StoreInventoryIn)

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

dfStoreInventory.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(StoreInvOut + '/' + todayyear + '/' + todaymonth + '/' + 'SpringCommunications_Inventory' + FileTime);


spark.stop()