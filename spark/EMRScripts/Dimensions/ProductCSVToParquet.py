import sys
from pyspark.sql import SparkSession
import boto3


class ProductCSVToParquet(object):

    def __init__(self):

        self.outputPath = sys.argv[1]
        self.inputFilePath = sys.argv[2]

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.fileHasDataFlag = 0
        self.bkt = self.inputFilePath
        self.my_bucket = self.s3.Bucket(name=self.bkt)

    def checkFileIsCsv(self, prefixPath):
        fileValid = 0
        ext = ''
        pfx = prefixPath
        partitionName = self.my_bucket.objects.filter(Prefix=pfx)
        for obj in partitionName:
            filepat = obj.key.split('/')[2]
            if filepat != '':
                ext = filepat.split('.')[1]
                self.log.info("Extension of the file is : ")
                self.log.info(ext)
            if ext == 'csv':
                fileValid = 1
            else:
                fileValid = 0
        return fileValid

    def loadParquet(self):
        prodFileCheck = self.checkFileIsCsv("Product/Working")
        prodIdenFileCheck = self.checkFileIsCsv("ProductIdentifier/Working")
        couponFileCheck = self.checkFileIsCsv("Coupons/Working")

        if prodFileCheck == 1 and prodIdenFileCheck == 1 and couponFileCheck == 1:
            self.log.info("All the 3 files are in csv format, proceeding with the transformation logic")

            productColumns = ['SkuID', 'ProductSku', 'CategoryNumber', 'Enabled', 'Discountable', 'PricingType', 'DefaultMargin',
                              'DefaultRetailPrice', 'GLAccountID1', 'GLAccountID2', 'GLAccountID3', 'GLAccountID4', 'ShowOnInvoice',
                              'Refundable', 'FloorPrice', 'MaxPrice', 'SerialNumberLength', 'NoSale', 'ProductName', 'ProductLabel',
                              'RefundPeriodLength', 'CarrierPrice', 'MultiLevelPriceDetailsLocked', 'DefaultSalePrice',
                              'DefaultMinQty', 'LockMinMax', 'DefaultVendorID', 'NoAutoTaxes', 'DefaultVendorName',
                              'PrimaryVendorSku', 'ManufacturerName', 'ManufacturerPartNumber', 'Serialized',
                              'DaysUntilStockBalance', 'RMANumberRequired', 'WarehouseLocation', 'BarCode', 'MinimumValueRequired',
                              'MinimumValue', 'AllowMultiple', 'Type', 'DefaultCost', 'AverageCOS', 'UnitCost', 'MostRecentCost',
                              'ProductLibraryName', 'CategoryName', 'PAWFloorPrice', 'DefaultMaxQty', 'RMADays', 'InvoiceComments',
                              'AutoGenerateSerialNumbers', 'AllowPriceIncrease', 'DefaultDiscontinuedDate', 'DateCreated',
                              'EcommerceItem', 'CostofGoodsSoldAccount', 'SalesRevenueAccount', 'InventoryAccount',
                              'InventoryCorrectionsAccount', 'WarrantyWebLink', 'WarrantyDescription', 'WarrantyLengthUnits',
                              'WarrantyLengthValue', 'CommissionDetailsLocked', 'RefundToUsed', 'TriggerServiceRequestOnSale',
                              'ServiceRequestType', 'BackOrderDate', 'StoreInStoreSKU', 'StoreInStorePrice', 'DefaultDoNotOrder',
                              'DefaultSpecialOrder', 'DefaultDateEOL', 'DefaultWriteOff', 'TaxApplicationType',
                              'SetCostToRefundPrice']
            dfProduct = self.sparkSession.read.format("com.databricks.spark.csv"). \
                option("header", "true"). \
                option("treatEmptyValuesAsNulls", "true"). \
                option("inferSchema", "true"). \
                option("parserLib", "univocity"). \
                option("multiLine", "true"). \
                load('s3://' + self.inputFilePath + '/Product/Working').toDF(*productColumns)
            dfProductIden = self.sparkSession.read.format("com.databricks.spark.csv"). \
                option("header", "true"). \
                option("treatEmptyValuesAsNulls", "true"). \
                option("inferSchema", "true"). \
                option("parserLib", "univocity"). \
                option("multiLine", "true"). \
                load('s3://' + self.inputFilePath + '/ProductIdentifier/Working')

            dfCoupons = self.sparkSession.read.format("com.databricks.spark.csv"). \
                option("header", "true"). \
                option("treatEmptyValuesAsNulls", "true"). \
                option("inferSchema", "true"). \
                load('s3://' + self.inputFilePath + '/Coupons/Working')

            dfProductCnt = dfProduct.count()
            dfProductIdenCnt = dfProductIden.count()

            if dfProductCnt > 1 and dfProductIdenCnt > 1:
                self.log.info("The product and product identifier files have data")
                fileHasDataFlag = 1
            else:
                self.log.info("The product OR product identifier files do not have data")
                fileHasDataFlag = 0

            if fileHasDataFlag == 1:
                self.log.info("Csv file loaded into dataframe properly")

                dfProduct.dropDuplicates(['ProductSKU']).registerTempTable("ProdTempTable")
                dfProductIden.dropDuplicates(['ID']).registerTempTable("ProdIdenTempTable")
                dfCoupons.dropDuplicates(['CouponSKU']).registerTempTable("CouponsTempTable")

                dfProductFinal = self.sparkSession.sql(
                    "select ProductSKU,ProductName,ProductLabel,DefaultCost,AverageCOS,UnitCost,MostRecentCost,"
                    "ProductLibraryName,ManufacturerName,ManufacturerPartNumber,CategoryName,PricingType,"
                    "DefaultRetailPrice,DefaultMargin,FloorPrice,PAWFloorPrice,DefaultMinQty,DefaultMaxQty,"
                    "LockMinMax,NoSale,RMADays,InvoiceComments,Serialized,AutoGenerateSerialNumbers,"
                    "SerialNumberLength,Discountable,AllowPriceIncrease,DefaultDiscontinuedDate,DateCreated,"
                    "Enabled,EcommerceItem,WarehouseLocation,DefaultVendorName,PrimaryVendorSKU,"
                    "CostofGoodsSoldAccount,SalesRevenueAccount,InventoryAccount,InventoryCorrectionsAccount,"
                    "WarrantyWebLink,WarrantyDescription,RMANumberRequired,WarrantyLengthUnits,"
                    "WarrantyLengthValue,CommissionDetailsLocked,ShowOnInvoice,Refundable,RefundPeriodLength,"
                    "RefundToUsed,TriggerServiceRequestOnSale,ServiceRequestType,MultiLevelPriceDetailsLocked,"
                    "BackOrderDate,StoreInStoreSKU,StoreInStorePrice,DefaultDoNotOrder,DefaultSpecialOrder,"
                    "DefaultDateEOL,DefaultWriteOff,NoAutoTaxes,TaxApplicationType,SetCostToRefundPrice,"
                    "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
                    " from ProdTempTable "
                    " where ProductSKU != ''"
                )

                dfProductIdenFinal = self.sparkSession.sql("select ID,Description,CategoryNumber,ProductEnabled, "
                                                           "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                                           "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdIdenTempTable "
                                                           "where ID is not null")

                dfCouponsFinal = self.sparkSession.sql("select CouponID,CouponSKU,CouponName,CouponLabel,EnabledStatus, "
                                                       "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                                       "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from CouponsTempTable where "
                                                       "CouponSKU is not null")

                dfProductFinal.coalesce(1).select('ProductSKU', 'ProductName', 'ProductLabel', 'DefaultCost', 'AverageCOS',
                                                  'UnitCost', 'MostRecentCost', 'ProductLibraryName', 'ManufacturerName',
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
                    parquet(self.outputPath + '/' + 'Product' + '/' + 'Working')

                dfProductIdenFinal.coalesce(1).select('ID', 'Description', 'CategoryNumber', 'ProductEnabled'). \
                    write.mode("overwrite"). \
                    parquet(self.outputPath + '/' + 'ProductIdentifier' + '/' + 'Working')

                dfCouponsFinal.coalesce(1).select('CouponID', 'CouponSKU', 'CouponName', 'CouponLabel', 'EnabledStatus'). \
                    write.mode("overwrite"). \
                    parquet(self.outputPath + '/' + 'Coupons' + '/' + 'Working')

                dfProductFinal.coalesce(1). \
                    write.mode('append').partitionBy('year', 'month'). \
                    format('parquet').save(self.outputPath + '/' + 'Product')

                dfProductIdenFinal.coalesce(1). \
                    write.mode('append').partitionBy('year', 'month'). \
                    format('parquet').save(self.outputPath + '/' + 'ProductIdentifier')

                dfCouponsFinal.coalesce(1). \
                    write.mode('append').partitionBy('year', 'month'). \
                    format('parquet').save(self.outputPath + '/' + 'Coupons')

            else:
                self.log.error("ERROR : Loading csv file into dataframe")

        else:
            self.log.error("Raw files are not in csv format, terminating the script")

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCSVToParquet().loadParquet()
