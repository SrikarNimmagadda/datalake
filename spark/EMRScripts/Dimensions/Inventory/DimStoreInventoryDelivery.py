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


spark = SparkSession.builder.\
    appName("DimStoreInventory").getOrCreate()

StoreInventoryParquetInput = sys.argv[1]
DimStoreInventoryDeliveryOutput = sys.argv[2]


#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

StoreInventory_DF = spark.read.parquet(
    StoreInventoryParquetInput).registerTempTable("StoreInventoryTT")


Final_DF = spark.sql("select report_date as RPT_DT, store_number as STORE_NUM,company_cd as CO_CD, "
                     + "product_sku as PROD_SKU,bin_status as BIN_STAT,serial_number as SN, "
                     + "vendor_name as VNDR_NM,vendor_part_number as VNDR_PRT_NBR, "
                     + "used_indicator as USED_IND,quantity as QTY,unit_cost as UNT_CST, "
                     + "total_cost as TTL_CST,store_number_warehouse as STORE_NBR_WHSE,inventory_item_age as INV_ITM_AGE "
                     + "from StoreInventoryTT")

Final_DF.coalesce(1).select("*"). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(DimStoreInventoryDeliveryOutput)

spark.stop()
