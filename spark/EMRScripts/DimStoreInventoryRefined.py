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

Final_Joined_DF = spark.sql("select ' ' as report_date, a.store_num as Store_Number, '4' as company_cd, a.ProductIdentifier as Product_SKU, "
						  + "a.BinStatus as Bin_Status, a.SerialNumber as Serial_Number, a.VendorName as Vendor_Name, a.VendorPartNumber as Vendor_Part_Number, "
                          + "a.ProductName as Product_Name, a.loc_name as Location_Name, a.StoreTypeName as Store_Type, b.Market as spring_market, "
						  + "b.Region as spring_region, b.District as spring_district, a.IsUsed as Used_Indicator, a.Quantity as Quantity, a.UnitCost as Unit_Cost, "
						  + "a.TotalCost as Total_Cost, a.ManufacturerPartNumber as Manufacturer_Part_Number, a.WarehouseLocation as Store_Number_Warehouse, "
						  + "a.CategoryPath as Category_Id, a.CategoryPath as Category_Path, a.DiscontinuedDate as Discontinued_Date, "
						  + "case when a.NoSale = 'TRUE' then '1' when a.NoSale = 'FALSE' then '0' else ' ' end as No_Sale_Indicator, "
                          + "case when a.DoNotOrder = 'TRUE' then '1' when a.DoNotOrder = 'FALSE' then '0' else ' ' end as Do_Not_Order_Indicator, "
                          + "case when a.SpecialOrder = 'TRUE' then '1' when a.SpecialOrder = 'FALSE' then '0'  else ' ' end as Special_Order_Indicator, "
                          + "a.DateEOL as End_Of_Life_Date, "
						  + "case when a.WriteOff = 'TRUE' then '1' when a.WriteOff = 'FALSE' then '0' else ' ' end as Write_Off_Indicator, "
						  + "a.RefundPeriodLength as Refund_Period_Length, a.BarCode as BarCode, ' ' as Inventory_Item_Age "
                          + "from StoreInventoryTT a "
						  + "inner join StoreRefinedTT b "
						  + "on a.store_num = b.StoreNumber")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimStoreInventoryRefinedOutput + '/' + todayyear + '/' + todaymonth + '/' + 'DimStoreInventoryRefined' + FileTime)
						  
spark.stop()