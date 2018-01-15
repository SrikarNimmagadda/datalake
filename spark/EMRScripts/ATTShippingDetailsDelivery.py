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

ATTShippingDetailsRefineInp = sys.argv[1]
ATTShippingDetailsOutputArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("ATTShippingDetailsDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfATTShippingDetails  = spark.read.parquet(ATTShippingDetailsRefineInp)

dfATTShippingDetails.registerTempTable("attshipping")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.report_date as RPT_DT,a.ponumber as PO_NBR,a.productsku as PROD_SKU,a.companycd AS CO_CD,"
            + "a.sourcesystemname as SRC_SYS_NM,"
            + "a.storenumber as STORE_NUM,a.ordernumber as ORD_NBR,a.actualshipdate as ACTL_SHIP_DT,"
            + "a.unitprice as UNT_PRC,a.extendedprice as EXTN_PRC,a.quantityordered as QTY_ORD,a.quantityshipped as QTY_SHIPPED,"
            + "a.imei as IMEI,a.trackingnumber as TRKING_NBR,a.productcategoryid as CAT_ID,a.attinvoicenumber as ATT_INVOICE_NBR,"
            + "a.cdcindicator as CDC_IND_CD from attshipping a")
                  
                    
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(ATTShippingDetailsOutputArg)

               
spark.stop()

                   