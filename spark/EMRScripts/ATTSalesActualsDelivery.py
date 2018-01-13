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

ATTSalesActualRefineInp = sys.argv[1]
ATTSalesActualOutputArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("ATTSalesActualDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfATTSalesActual  = spark.read.parquet(ATTSalesActualRefineInp)

dfATTSalesActual.registerTempTable("attsalesactual")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.storenumber as STORE_NUM,a.dealercode as DLR_CD,a.companycd AS CO_CD,"
        + "a.report_date as RPT_DT,a.kpi as KPI_NM,a.actualvalue as ACTL_VAL,a.projectedvalue as PROJ_VAL "
        + "from attsalesactual a")
                  
                 
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(ATTSalesActualOutputArg)

               
spark.stop()

                   