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

StoreInventoryParquetInput = sys.argv[1]
DimStoreInventoryDeliveryOutput = sys.argv[2]


#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

StoreInventory_DF = spark.read.parquet(StoreInventoryParquetInput).registerTempTable("StoreInventoryTT")
				   

Final_DF =  spark.sql("select report_date as RPT_DT, Store_Number as STORE_NUM,company_cd as CO_CD, "
					+ "Product_SKU as PROD_SKU,Bin_Status as BIN_STAT,Serial_Number as SN, "
					+ "Vendor_Name as VNDR_NM,Vendor_Part_Number as VNDR_PRT_NBR, "
					+ "Used_Indicator as USED_IND,Quantity as QTY,Unit_Cost as UNT_CST, "
					+ "Total_Cost as TTL_CST,Store_Number_Warehouse as STORE_NBR_WHSE,Inventory_Item_Age as INV_ITM_AGE "
					+"from StoreInventoryTT")

Final_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimStoreInventoryDeliveryOutput);
						  
spark.stop()