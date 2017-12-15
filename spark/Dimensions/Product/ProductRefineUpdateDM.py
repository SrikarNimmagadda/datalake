#This module for Product Refine#############

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

SpringCommCust = sys.argv[1]
CustomerInp = sys.argv[2]
ProductIdenInp = sys.argv[3]
CompanyOutputArg = sys.argv[4]
CompanyFileTime = sys.argv[5]

spark = SparkSession.builder.\
    appName("CompanyRefinary").getOrCreate()

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfSpringComm = spark.read.parquet(SpringCommCust)
dfProductIden = spark.read.parquet(ProductIdenInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    load(CustomerInp)

dfSpringComm = dfSpringComm.withColumnRenamed("ProductSKU", "productsku").\
    withColumnRenamed("ProductName", "productname").\
    withColumnRenamed("ProductLabel", "productlabel").\
    withColumnRenamed("DefaultCost", "defaultcost").\
    withColumnRenamed("AverageCOS", "averagecost").\
    withColumnRenamed("UnitCost", "unitcost").\
    withColumnRenamed("MostRecentCost", "mostrecentcost").\
    withColumnRenamed("ProductLibraryName", "productlibraryname").\
    withColumnRenamed("Manufacturer", "manufacturer").\
    withColumnRenamed("ManufacturerPartNumber", "manufacturerpartnumber").\
    withColumnRenamed("PricingType", "pricingtype").\
    withColumnRenamed("DefaultRetailPrice", "defaultretailprice").\
    withColumnRenamed("DefaultMargin", "defaultmargin").\
    withColumnRenamed("FloorPrice", "floorprice").\
    withColumnRenamed("PAWFloorPrice", "pawfloorprice").\
    withColumnRenamed("DefaultMinQty", "defaultminimumquantity").\
    withColumnRenamed("DefaultMaxQty", "defaultmaximumquantity").\
    withColumnRenamed("LockMinMax", "lockminmax").\
    withColumnRenamed("NoSale", "nosaleflag").\
    withColumnRenamed("RMADays", "rmadays").\
    withColumnRenamed("InvoiceComments", "defaultinvoicecomments").\
    withColumnRenamed("SerialNumberLength", "serialnumberlength").\
    withColumnRenamed("Discountable", "discountable").\
    withColumnRenamed("DefaultDiscontinuedDate", "defaultdiscontinueddate").\
    withColumnRenamed("DateCreated", "datecreatedatsource").\
    withColumnRenamed("Enabled", "enabled").\
    withColumnRenamed("EcommerceItem", "ecommerceitem").\
    withColumnRenamed("WarehouseLocation", "warehouselocation").\
    withColumnRenamed("DefaultVendorName", "defaultvendorname").\
    withColumnRenamed("PrimaryVendorSKU", "primaryvendorsku").\
    withColumnRenamed("CostofGoodsSoldAccount", "costaccount").\
    withColumnRenamed("SalesRevenueAccount", "revenueaccount").\
    withColumnRenamed("InventoryAccount", "inventoryaccount").\
    withColumnRenamed("InventoryCorrectionsAccount", "inventorycorrectionsaccount").\
    withColumnRenamed("WarrantyDescription", "warrantydescription").\
    withColumnRenamed("RMANumberRequired", "rmanumberrequired").\
    withColumnRenamed("WarrantyLengthUnits", "warrantylengthunits").\
    withColumnRenamed("WarrantyLengthValue", "warrantylengthvalue").\
    withColumnRenamed("CommissionDetailsLocked", "commissiondetailslocked").\
    withColumnRenamed("ShowOnInvoice", "showoninvoice").\
    withColumnRenamed("Refundable", "refundable").\
    withColumnRenamed("RefundPeriodLength", "refundperiodlength").\
    withColumnRenamed("RefundToUsed", "refundtoused").\
    withColumnRenamed("TriggerServiceRequestOnSale", "triggerservicerequestonsale").\
    withColumnRenamed("ServiceRequestType", "servicerequesttype").\
    withColumnRenamed("MultiLevelPriceDetailsLocked", "multilevelpricedetailslocked").\
    withColumnRenamed("BackOrderDate", "backorderdate").\
    withColumnRenamed("StoreInStoreSKU", "storeinstoresku").\
    withColumnRenamed("StoreInStorePrice", "storeinstoreprice").\
    withColumnRenamed("DefaultDoNotOrder", "defaultdonotorder").\
    withColumnRenamed("DefaultSpecialOrder", "defaultspecialorder").\
    withColumnRenamed("DefaultDateEOL", "defaultdateeol").\
    withColumnRenamed("DefaultWriteOff", "defaultwriteoff").\
    withColumnRenamed("NoAutoTaxes", "noautotaxes")
#withColumnRenamed("TaxApplicationType", "taxapplicationtype")


dfSpringComm.registerTempTable("springcomm")
dfCustomer.registerTempTable("customer")
dfProductIden.registerTempTable("productiden")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfCustomer = spark.sql(
    "select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfSpringComm = spark.sql("select b.companycode as companycd,a.productsku as productsku,a.productname,a.productlabel,c.CategoryNumber as categoryid,a.defaultcost,"
                         + "'RQ4' as sourcesystemname,"
                         + "a.averagecost,a.unitcost,a.mostrecentcost,a.manufacturer,a.manufacturerpartnumber,"
                         + "a.pricingtype,a.defaultretailprice,a.defaultmargin,a.floorprice,a.pawfloorprice,a.defaultminimumquantity,"
                         + "a.defaultmaximumquantity,a.lockminmax,a.nosaleflag,a.rmadays,a.defaultinvoicecomments,"
                         + "'' as serialnumberautogenerateflag,a.serialnumberlength,"
                         + "a.discountable,'' as allowpriceincrease,"
                         + "a.defaultdiscontinueddate,a.datecreatedatsource,a.enabled as productactiveindicator,a.ecommerceitem,a.warehouselocation,"
                         + "a.defaultvendorname,a.primaryvendorsku,a.costaccount,a.revenueaccount,a.inventoryaccount,a.inventorycorrectionsaccount,"
                         + "a.warrantydescription,a.rmanumberrequired,a.warrantylengthunits,a.warrantylengthvalue,a.commissiondetailslocked,"
                         + "a.showoninvoice,a.refundable,a.refundperiodlength,a.refundtoused,a.triggerservicerequestonsale,a.servicerequesttype,a.multilevelpricedetailslocked,"
                         + "a.backorderdate,a.storeinstoresku,a.storeinstoreprice,a.defaultdonotorder,a.defaultspecialorder,a.defaultdateeol,"
                         + "a.defaultwriteoff,a.noautotaxes, 'I' as cdcindicator,"
                         + "case when discountable == 'TRUE' then '1' else '0' end as discountableindicator,"
                         + "'' as allowpriceincreaseindicator from springcomm a cross join customer1 b"
                         + " inner join productiden c "
                         + "on a.productsku = c.ID")

dfSpringComm.printSchema()

# dfdiscntind = dfSpringCommtemp.withColumn('discountableindicatortemp',sf.when((dfSpringComm.discountable == 'TRUE'),1).otherwise(0)).\
# drop(dfSpringCommtemp.discountableindicator).\
# select(col('discountableIndicatortemp').alias('discountableindicator'))
#dfSpringComm = dfSpringCommtemp.unionAll(dfdiscntind)

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

# dfSpringComm.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(CompanyOutputArg)

dfSpringComm.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' +
                  todaymonth + '/' + 'PRODUCT' + CompanyFileTime)

spark.stop()
