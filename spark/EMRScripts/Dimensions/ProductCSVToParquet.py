#!/usr/bin/python
#######################################################################################################################
# Author                : Harikesh R
# Date Created          : 12/01/2018
# Purpose               : To convert the csv file to parquet and move it to Discovery layer for Product
# Version               : 1.1
# Revision History      :
# Revised Date        : 22/01/2018
# Revision COmments   : Included Coupons file load
# Version           : 1.2
# Revision Comments : Included CDC logic
########################################################################################################################

import sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark import SparkConf, SparkContext
import boto3

OutputPath = sys.argv[1]
InputFilePath = sys.argv[2]

Config = SparkConf().setAppName("Product_CSVToParquet")
SparkCtx = SparkContext(conf=Config)
spark = SparkSession.builder.config(conf=Config). \
    getOrCreate()

Log4jLogger = SparkCtx._jvm.org.apache.log4j
logger = Log4jLogger.LogManager.getLogger('prod_csv_to_parquet')

s3 = boto3.resource('s3')
fileHasDataFlag = 0
bkt = InputFilePath
my_bucket = s3.Bucket(name=bkt)


def checkFileIsCsv(prefixPath):
    fileValid = 0
    ext = ''
    pfx = prefixPath
    partitionName = my_bucket.objects.filter(Prefix=pfx)
    for obj in partitionName:
        filepat = obj.key.split('/')[2]
        if filepat != '':
            ext = filepat.split('.')[1]
            logger.info("Extension of the file is : ")
            logger.info(ext)
        if ext == 'csv':
            fileValid = 1
        else:
            fileValid = 0
    return fileValid


prodFileCheck = checkFileIsCsv("Product/Working")
prodIdenFileCheck = checkFileIsCsv("ProductIdentifier/Working")
couponFileCheck = checkFileIsCsv("Coupons/Working")

if prodFileCheck == 1 and prodIdenFileCheck == 1 and couponFileCheck == 1:
    logger.info("All the 3 files are in csv format, proceeding with the transformation logic")

    productColumns = ['ProductSKU', 'ProductName', 'ProductLabel', 'DefaultCost', 'AverageCOS', 'UnitCost',
                      'MostRecentCost', 'ProductLibraryName', 'Manufacturer', 'ManufacturerPartNumber', 'CategoryName',
                      'PricingType', 'DefaultRetailPrice', 'DefaultMargin', 'FloorPrice', 'PAWFloorPrice',
                      'DefaultMinQty', 'DefaultMaxQty', 'LockMinMax', 'NoSale', 'RMADays', 'InvoiceComments',
                      'Serialized', 'AutoGenerateSerialNumbers', 'SerialNumberLength', 'Discountable',
                      'AllowPriceIncrease', 'DefaultDiscontinuedDate', 'DateCreated', 'Enabled', 'EcommerceItem',
                      'WarehouseLocation', 'DefaultVendorName', 'PrimaryVendorSKU', 'CostofGoodsSoldAccount',
                      'SalesRevenueAccount', 'InventoryAccount', 'InventoryCorrectionsAccount', 'WarrantyWebLink',
                      'WarrantyDescription', 'RMANumberRequired', 'WarrantyLengthUnits', 'WarrantyLengthValue',
                      'CommissionDetailsLocked', 'ShowOnInvoice', 'Refundable', 'RefundPeriodLength', 'RefundToUsed',
                      'TriggerServiceRequestOnSale', 'ServiceRequestType', 'MultiLevelPriceDetailsLocked',
                      'BackOrderDate', 'StoreInStoreSKU', 'StoreInStorePrice', 'DefaultDoNotOrder',
                      'DefaultSpecialOrder', 'DefaultDateEOL', 'DefaultWriteOff', 'NoAutoTaxes', 'TaxApplicationType',
                      'SetCostToRefundPrice']
    dfProduct = spark.read.format("com.databricks.spark.csv"). \
        option("header", "true"). \
        option("treatEmptyValuesAsNulls", "true"). \
        option("inferSchema", "true"). \
        option("parserLib", "univocity"). \
        option("multiLine", "true"). \
        load('s3://' + InputFilePath + '/Product/Working').toDF(*productColumns)
    dfProductIden = spark.read.format("com.databricks.spark.csv"). \
        option("header", "true"). \
        option("treatEmptyValuesAsNulls", "true"). \
        option("inferSchema", "true"). \
        option("parserLib", "univocity"). \
        option("multiLine", "true"). \
        load('s3://' + InputFilePath + '/ProductIdentifier/Working')

    dfCoupons = spark.read.format("com.databricks.spark.csv"). \
        option("header", "true"). \
        option("treatEmptyValuesAsNulls", "true"). \
        option("inferSchema", "true"). \
        load('s3://' + InputFilePath + '/Coupons/Working')

    dfProductCnt = dfProduct.count()
    dfProductIdenCnt = dfProductIden.count()

    if dfProductCnt > 1 and dfProductIdenCnt > 1:
        logger.info("The product and product identifier files have data")
        fileHasDataFlag = 1
    else:
        logger.info("The product OR product identifier files do not have data")
        fileHasDataFlag = 0

    if fileHasDataFlag == 1:
        logger.info("Csv file loaded into dataframe properly")

        dfProductWithDupsRem = dfProduct.dropDuplicates(['ProductSKU']).registerTempTable("ProdTempTable")
        dfProductIdenWithDupsRem = dfProductIden.dropDuplicates(['ID']).registerTempTable("ProdIdenTempTable")
        dfCouponsWithDupsRem = dfCoupons.dropDuplicates(['CouponSKU']).registerTempTable("CouponsTempTable")

        dfProductFinal = spark.sql(
            "select ProductSKU,ProductName,ProductLabel,DefaultCost,AverageCOS,UnitCost,MostRecentCost,"
            + "ProductLibraryName,Manufacturer,ManufacturerPartNumber,CategoryName,PricingType,"
            + "DefaultRetailPrice,DefaultMargin,FloorPrice,PAWFloorPrice,DefaultMinQty,DefaultMaxQty,"
            + "LockMinMax,NoSale,RMADays,InvoiceComments,Serialized,AutoGenerateSerialNumbers,"
            + "SerialNumberLength,Discountable,AllowPriceIncrease,DefaultDiscontinuedDate,DateCreated,"
            + "Enabled,EcommerceItem,WarehouseLocation,DefaultVendorName,PrimaryVendorSKU,"
            + "CostofGoodsSoldAccount,SalesRevenueAccount,InventoryAccount,InventoryCorrectionsAccount,"
            + "WarrantyWebLink,WarrantyDescription,RMANumberRequired,WarrantyLengthUnits,"
            + "WarrantyLengthValue,CommissionDetailsLocked,ShowOnInvoice,Refundable,RefundPeriodLength,"
            + "RefundToUsed,TriggerServiceRequestOnSale,ServiceRequestType,MultiLevelPriceDetailsLocked,"
            + "BackOrderDate,StoreInStoreSKU,StoreInStorePrice,DefaultDoNotOrder,DefaultSpecialOrder,"
            + "DefaultDateEOL,DefaultWriteOff,NoAutoTaxes,TaxApplicationType,SetCostToRefundPrice,"
            + "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
            + " from ProdTempTable "
            + " where ProductSKU != ''"
        )

        dfProductIdenFinal = spark.sql("select ID,Description,CategoryNumber,ProductEnabled, "
                                       "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                       "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdIdenTempTable "
                                       "where ID is not null")

        dfCouponsFinal = spark.sql("select CouponID,CouponSKU,CouponName,CouponLabel,EnabledStatus, "
                                   "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                   "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from CouponsTempTable where "
                                   "CouponSKU is not null")

        todayyear = datetime.now().strftime('%Y')
        todaymonth = datetime.now().strftime('%m')

        dfProductFinal.coalesce(1).select('ProductSKU', 'ProductName', 'ProductLabel', 'DefaultCost', 'AverageCOS',
                                          'UnitCost', 'MostRecentCost', 'ProductLibraryName', 'Manufacturer',
                                          'ManufacturerPartNumber', 'CategoryName', 'PricingType', 'DefaultRetailPrice',
                                          'DefaultMargin', 'FloorPrice', 'PAWFloorPrice', 'DefaultMinQty',
                                          'DefaultMaxQty',
                                          'LockMinMax', 'NoSale', 'RMADays', 'InvoiceComments', 'Serialized',
                                          'AutoGenerateSerialNumbers', 'SerialNumberLength', 'Discountable',
                                          'AllowPriceIncrease', 'DefaultDiscontinuedDate', 'DateCreated', 'Enabled',
                                          'EcommerceItem', 'WarehouseLocation', 'DefaultVendorName', 'PrimaryVendorSKU',
                                          'CostofGoodsSoldAccount', 'SalesRevenueAccount', 'InventoryAccount',
                                          'InventoryCorrectionsAccount', 'WarrantyWebLink', 'WarrantyDescription',
                                          'RMANumberRequired', 'WarrantyLengthUnits', 'WarrantyLengthValue',
                                          'CommissionDetailsLocked', 'ShowOnInvoice', 'Refundable',
                                          'RefundPeriodLength',
                                          'RefundToUsed', 'TriggerServiceRequestOnSale', 'ServiceRequestType',
                                          'MultiLevelPriceDetailsLocked', 'BackOrderDate', 'StoreInStoreSKU',
                                          'StoreInStorePrice', 'DefaultDoNotOrder', 'DefaultSpecialOrder',
                                          'DefaultDateEOL',
                                          'DefaultWriteOff', 'NoAutoTaxes', 'TaxApplicationType',
                                          'SetCostToRefundPrice'). \
            write.mode("overwrite"). \
            parquet(OutputPath + '/' + 'Product' + '/' + 'Working')

        dfProductIdenFinal.coalesce(1).select('ID', 'Description', 'CategoryNumber', 'ProductEnabled'). \
            write.mode("overwrite"). \
            parquet(OutputPath + '/' + 'ProductIdentifier' + '/' + 'Working')

        dfCouponsFinal.coalesce(1).select('CouponID', 'CouponSKU', 'CouponName', 'CouponLabel', 'EnabledStatus'). \
            write.mode("overwrite"). \
            parquet(OutputPath + '/' + 'Coupons' + '/' + 'Working')

        dfProductFinal.coalesce(1). \
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet').save(OutputPath + '/' + 'Product')

        dfProductIdenFinal.coalesce(1). \
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet').save(OutputPath + '/' + 'ProductIdentifier')

        dfCouponsFinal.coalesce(1). \
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet').save(OutputPath + '/' + 'Coupons')

    else:
        logger.error("ERROR : Loading csv file into dataframe")

else:
    logger.error("Raw files are not in csv format, terminating the script")

spark.stop()
