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

StoreInventoryInput = sys.argv[1]
StoreRefined = sys.argv[2]
DimStoreInventoryRefinedOutput = sys.argv[3]
FileTime = sys.argv[4]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################


StoreInventory_DF = spark.read.parquet(StoreInventoryInput)


split_col = split(StoreInventory_DF['StoreName'], ' ')

StoreInventory_DF = StoreInventory_DF.withColumn('store_num', split_col.getItem(0))
StoreInventory_DF = StoreInventory_DF.withColumn('loc_name', split_col.getItem(1))

StoreInventory_DF.registerTempTable("StoreInventoryTT")

StoreRefined_DF = spark.read.parquet(StoreRefined).registerTempTable("StoreRefinedTT")

Final_Joined_DF = spark.sql("select ' ' as report_date, a.store_num as store_number, '4' as company_cd, a.ProductIdentifier as product_sku, "
                            + "a.BinStatus as bin_status, a.SerialNumber as serial_number, a.VendorName as vendor_name, a.VendorPartNumber as vendor_part_number, "
                            + "a.ProductName as product_name, a.loc_name as location_name, a.StoreTypeName as store_type, b.Market as spring_market, "
                            + "b.Region as spring_region, b.District as spring_district, a.IsUsed as used_indicator, a.Quantity as quantity, a.UnitCost as unit_Cost, "
                            + "a.TotalCost as total_cost, a.ManufacturerPartNumber as manufacturer_part_number, a.WarehouseLocation as store_number_warehouse, "
                            + "a.CategoryPath as category_id, a.CategoryPath as category_path, a.DiscontinuedDate as discontinued_date, "
                            + "case when a.NoSale = 'TRUE' then '1' when a.NoSale = 'FALSE' then '0' else ' ' end as no_sale_indicator, "
                            + "case when a.DoNotOrder = 'TRUE' then '1' when a.DoNotOrder = 'FALSE' then '0' else ' ' end as do_not_order_indicator, "
                            + "case when a.SpecialOrder = 'TRUE' then '1' when a.SpecialOrder = 'FALSE' then '0'  else ' ' end as special_order_indicator, "
                            + "a.DateEOL as end_of_life_date, "
                            + "case when a.WriteOff = 'TRUE' then '1' when a.WriteOff = 'FALSE' then '0' else ' ' end as write_off_indicator, "
                            + "a.RefundPeriodLength as refund_period_length, a.BarCode as barCode, ' ' as inventory_item_age "
                            + "from StoreInventoryTT a "
                            + "inner join StoreRefinedTT b "
                            + "on a.store_num = b.StoreNumber")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimStoreInventoryRefinedOutput +
                                                            '/' + todayyear + '/' + todaymonth + '/' + 'DimStoreInventoryRefined' + FileTime)

spark.stop()
