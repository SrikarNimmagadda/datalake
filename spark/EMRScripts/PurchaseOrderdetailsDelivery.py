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

PurchaseOrderDetailsRefineInp = sys.argv[1]
PurchaseOrderDetailsOutputArg = sys.argv[2] 
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("PurchaseOrderDetailsDelivery").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfPurchaseOrder  = spark.read.parquet(PurchaseOrderDetailsRefineInp)

dfPurchaseOrder.registerTempTable("purchaseorder")
                

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfOutput = spark.sql("select distinct a.productsku as PROD_SKU,a.companycd as CO_CD,a.sourcepurchaseorderid as SRC_PO_ID,"
    + "a.sourcesystemname as SRC_SYS_NM,a.purchaseorderidentifier as PO_ID,a.businessdate as BUS_DT,"
    + "a.storenumberorderedfor as STORE_NUM_ORD_FOR,a.employeeid as SRC_EMP_ID_ORD_BY,a.pocompleteindicator as PO_COMPLETE_IND, "
    + "a.unitcost as UNT_CST,a.vendorname as DFLT_VNDR_NME,a.productcategoryid as CAT_ID,a.vendorsku as PRI_VNDR_SKU,"
    + "a.quantityordered as QTY_ORD,a.quantityreceived as QTY_RCV,a.totalquantityreceived as TTL_QTY_RCV,"
    + "a.totalcostreceived as TTL_CST_RCV,a.orderetadate as ORD_ETA_DT,a.partiallyreceivedindicator as PTL_RCV_IND,"
    + "a.fullyreceivedindicator as FULL_RCV_IND,a.ordercomments as ORD_CMNT,a.orderentryid as ORD_ENTRY_ID,"
    + "a.orderentryidbystore as ORD_ENTRY_ID_BY_STORE,a.vendorterms as VNDR_TERMS,a.cogsaccountnumber as COGS_ACCT_NBR,"
    + "a.vendoridentifier as VNDR_ID from purchaseorder a")
                  
                    
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(PurchaseOrderDetailsOutputArg)

               
spark.stop()

                   