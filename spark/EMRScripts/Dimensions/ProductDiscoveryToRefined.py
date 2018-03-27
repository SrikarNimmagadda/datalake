from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import hash as hash_
import boto3


class ProductDiscoveryToRefined(object):

    def __init__(self):

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.discoveryWorkingPath = sys.argv[1]
        self.refinedWorkingPath = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/refined'

        self.productRQ4Name = self.discoveryWorkingPath[self.discoveryWorkingPath.index('tb'):].split("/")[1]
        self.workingName = self.discoveryWorkingPath[self.discoveryWorkingPath.index('tb'):].split("/")[2]
        self.couponsName = 'Coupons'
        self.productIdentifierName = 'ProductIdentifier'

        self.discoveryBucket = self.discoveryWorkingPath[self.discoveryWorkingPath.index('tb'):].split("/")[0]
        self.refinedBucket = self.refinedWorkingPath[self.refinedWorkingPath.index('tb'):].split("/")[0]

        self.discoveryProductWorkingPath = self.discoveryWorkingPath
        self.discoveryCouponsWorkingPath = 's3://' + self.discoveryBucket + '/' + self.couponsName + '/' + self.workingName
        self.discoveryProductIdentifierWorkingPath = 's3://' + self.discoveryBucket + '/' + self.productIdentifierName + '/' + self.workingName

        self.productRefinedWorkingPath = 's3://' + self.refinedBucket + '/' + self.productRQ4Name + '/' + self.workingName
        self.productRefinedPartitonPath = 's3://' + self.refinedBucket + '/' + self.productRQ4Name

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.log.info("prefixPath is " + prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        allValuesDict = {}
        reqValuesDict = {}
        for obj in partitionName:
            allValuesDict[obj.key] = obj.last_modified
        for k, v in allValuesDict.items():
            if 'part-0000' in k:
                reqValuesDict[k] = v
        revSortedFiles = sorted(reqValuesDict, key=reqValuesDict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        dfProduct = self.sparkSession.read.parquet(self.discoveryProductWorkingPath).drop_duplicates()
        self.sparkSession.read.parquet(self.discoveryProductIdentifierWorkingPath).drop_duplicates().registerTempTable("productIdentifier")
        self.sparkSession.read.parquet(self.discoveryCouponsWorkingPath).drop_duplicates().registerTempTable("coupons")

        dfProduct.withColumnRenamed("ProductSKU", "productsku"). \
            withColumnRenamed("ProductName", "productname"). \
            withColumnRenamed("ProductLabel", "productlabel"). \
            withColumnRenamed("DefaultCost", "defaultcost"). \
            withColumnRenamed("AverageCOS", "averagecost"). \
            withColumnRenamed("UnitCost", "unitcost"). \
            withColumnRenamed("MostRecentCost", "mostrecentcost"). \
            withColumnRenamed("ProductLibraryName", "productlibraryname"). \
            withColumnRenamed("Manufacturer", "manufacturer"). \
            withColumnRenamed("ManufacturerPartNumber", "manufacturerpartnumber"). \
            withColumnRenamed("PricingType", "pricingtype"). \
            withColumnRenamed("DefaultRetailPrice", "defaultretailprice"). \
            withColumnRenamed("DefaultMargin", "defaultmargin"). \
            withColumnRenamed("FloorPrice", "floorprice"). \
            withColumnRenamed("PAWFloorPrice", "pawfloorprice"). \
            withColumnRenamed("DefaultMinQty", "defaultminimumquantity"). \
            withColumnRenamed("DefaultMaxQty", "defaultmaximumquantity"). \
            withColumnRenamed("LockMinMax", "lockminmax"). \
            withColumnRenamed("NoSale", "nosaleflag"). \
            withColumnRenamed("RMADays", "rmadays"). \
            withColumnRenamed("InvoiceComments", "defaultinvoicecomments"). \
            withColumnRenamed("Serialized", "serialized"). \
            withColumnRenamed("SerialNumberLength", "serialnumberlength"). \
            withColumnRenamed("Discountable", "discountable"). \
            withColumnRenamed("AllowPriceIncrease", "allowpriceincrease"). \
            withColumnRenamed("DefaultDiscontinuedDate", "defaultdiscontinueddate"). \
            withColumnRenamed("DateCreated", "datecreatedatsource"). \
            withColumnRenamed("Enabled", "enabled"). \
            withColumnRenamed("EcommerceItem", "ecommerceitem"). \
            withColumnRenamed("WarehouseLocation", "warehouselocation"). \
            withColumnRenamed("DefaultVendorName", "defaultvendorname"). \
            withColumnRenamed("PrimaryVendorSKU", "primaryvendorsku"). \
            withColumnRenamed("CostofGoodsSoldAccount", "costaccount"). \
            withColumnRenamed("SalesRevenueAccount", "revenueaccount"). \
            withColumnRenamed("InventoryAccount", "inventoryaccount"). \
            withColumnRenamed("InventoryCorrectionsAccount", "inventorycorrectionsaccount"). \
            withColumnRenamed("WarrantyDescription", "warrantydescription"). \
            withColumnRenamed("RMANumberRequired", "rmanumberrequired"). \
            withColumnRenamed("WarrantyLengthUnits", "warrantylengthunits"). \
            withColumnRenamed("WarrantyLengthValue", "warrantylengthvalue"). \
            withColumnRenamed("CommissionDetailsLocked", "commissiondetailslocked"). \
            withColumnRenamed("ShowOnInvoice", "showoninvoice"). \
            withColumnRenamed("Refundable", "refundable"). \
            withColumnRenamed("RefundPeriodLength", "refundperiodlength"). \
            withColumnRenamed("RefundToUsed", "refundtoused"). \
            withColumnRenamed("TriggerServiceRequestOnSale", "triggerservicerequestonsale"). \
            withColumnRenamed("ServiceRequestType", "servicerequesttype"). \
            withColumnRenamed("MultiLevelPriceDetailsLocked", "multilevelpricedetailslocked"). \
            withColumnRenamed("BackOrderDate", "backorderdate"). \
            withColumnRenamed("StoreInStoreSKU", "storeinstoresku"). \
            withColumnRenamed("StoreInStorePrice", "storeinstoreprice"). \
            withColumnRenamed("DefaultDoNotOrder", "defaultdonotorder"). \
            withColumnRenamed("DefaultSpecialOrder", "defaultspecialorder"). \
            withColumnRenamed("DefaultDateEOL", "defaultdateeol"). \
            withColumnRenamed("DefaultWriteOff", "defaultwriteoff"). \
            withColumnRenamed("NoAutoTaxes", "noautotaxes"). \
            withColumnRenamed("TaxApplicationType", "taxapplicationtype").registerTempTable("product")

        ###########################################################################################################
        #                                Exception Handling

        prodBadRecsDF = self.sparkSession.sql("select * from product  where productsku is null")

        prodIdenBadRecsDF = self.sparkSession.sql("select * from productIdentifier where ID is null")

        couponsBadRecsDF = self.sparkSession.sql("select * from coupons where CouponSKU is null")

        if prodBadRecsDF.count() > 0:
            prodBadRecsDF.coalesce(1). \
                write.format("com.databricks.spark.csv"). \
                option("header", "true").mode("append").save(self.dataProcessingErrorPath + '/' + self.productRQ4Name)

        if prodIdenBadRecsDF.count() > 0:
            prodIdenBadRecsDF.coalesce(1). \
                write.format("com.databricks.spark.csv"). \
                option("header", "true").mode("append").save(self.dataProcessingErrorPath + '/' + self.productIdentifierName)

        if couponsBadRecsDF.count() > 0:
            couponsBadRecsDF.coalesce(1). \
                write.format("com.databricks.spark.csv"). \
                option("header", "true").mode("append").save(self.dataProcessingErrorPath + '/' + self.couponsName)

        #########################################################################################################
        #                                 Spark Transformation begins here                                      #
        #########################################################################################################

        SourceDataDFTmp = self.sparkSession.sql("select a.productsku,'4' as companycd ,a.productname,a.productlabel,"
                                                "cast(b.CategoryNumber as string) as categoryid,"
                                                "a.defaultcost,a.averagecost,a.unitcost,a.mostrecentcost,a.manufacturer,"
                                                "a.manufacturerpartnumber,"
                                                "a.pricingtype,a.defaultretailprice,a.defaultmargin,a.floorprice,a.pawfloorprice,"
                                                "a.defaultminimumquantity,"
                                                "a.defaultmaximumquantity,"
                                                "CASE WHEN a.lockminmax = 'TRUE' THEN 1 ELSE 0 END as lockminmax,"
                                                "CASE WHEN a.nosaleflag = 'TRUE' THEN 1 ELSE 0 END as nosaleflag,"
                                                "a.rmadays,a.defaultinvoicecomments,"
                                                "CASE WHEN a.serialized = TRUE THEN 1 ELSE 0 END as serializedproductindicator,"
                                                "a.serialnumberlength,"
                                                "CASE WHEN a.discountable = TRUE THEN 1 ELSE 0 END as discountable,"
                                                "a.defaultdiscontinueddate,a.datecreatedatsource,"
                                                "CASE WHEN a.enabled = TRUE THEN 1 ELSE 0 END as productactiveindicator,"
                                                "CASE WHEN a.ecommerceitem = 'TRUE' THEN 1 ELSE 0 END as ecommerceitem,"
                                                "a.warehouselocation,"
                                                "a.defaultvendorname,a.primaryvendorsku,a.costaccount,a.revenueaccount,"
                                                "a.inventoryaccount,a.inventorycorrectionsaccount,"
                                                "a.warrantydescription,"
                                                "CASE WHEN a.rmanumberrequired = 'TRUE' THEN 1 ELSE 0 END as rmanumberrequired,"
                                                "a.warrantylengthunits,a.warrantylengthvalue,"
                                                "CASE WHEN a.commissiondetailslocked = 'TRUE' THEN 1 ELSE 0 END as "
                                                "commissiondetailslocked,"
                                                "CASE WHEN a.showoninvoice = 'TRUE' THEN 1 ELSE 0 END as showoninvoice,"
                                                "CASE WHEN a.refundable = 'TRUE' THEN 1 ELSE 0 END as refundable,"
                                                "a.refundperiodlength,"
                                                "CASE WHEN a.refundtoused = 'TRUE' THEN 1 ELSE 0 END as refundtoused,"
                                                "CASE WHEN a.triggerservicerequestonsale = 'TRUE' THEN 1 ELSE 0 END as "
                                                "triggerservicerequestonsale,"
                                                "a.servicerequesttype,"
                                                "CASE WHEN a.multilevelpricedetailslocked = 'TRUE' THEN 1 ELSE 0 END as "
                                                "multilevelpricedetailslocked,"
                                                "a.backorderdate,a.storeinstoresku,a.storeinstoreprice,"
                                                "CASE WHEN a.defaultdonotorder = 'TRUE' THEN 1 ELSE 0 END as defaultdonotorder,"
                                                "CASE WHEN a.defaultspecialorder = 'TRUE' THEN 1 ELSE 0 END as defaultspecialorder,"
                                                "a.defaultdateeol,"
                                                "CASE WHEN a.defaultwriteoff = TRUE THEN 1 ELSE 0 END as defaultwriteoff,"
                                                "CASE WHEN a.noautotaxes = 'TRUE' THEN 1 ELSE 0 END as noautotaxes, "
                                                "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as YEAR,"
                                                "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as MONTH "
                                                "from product a LEFT OUTER JOIN productIdentifier b "
                                                "on a.productsku = b.ID "
                                                "    UNION ALL "
                                                "select c.CouponSKU as productsku,'4' as companycd ,c.CouponName as productname,"
                                                "c.CouponLabel as productlabel,"
                                                "20,"
                                                "NULL,NULL,NULL,NULL,NULL,NULL,"
                                                "NULL,NULL,NULL,NULL,NULL,NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,NULL,"
                                                "NULL,NULL,"
                                                "NULL,"
                                                "NULL,NULL,"
                                                "CASE WHEN c.EnabledStatus = 'Enabled' THEN 1 ELSE 0 END as productactiveindicator,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,NULL,NULL,NULL,NULL,NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,NULL,NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL,"
                                                "NULL, "
                                                "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as YEAR,"
                                                "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as MONTH "
                                                "from coupons c")

        # CDC Logic for PROD

        refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)
        productPrevRefinedPath = self.findLastModifiedFile(refinedBucketNode, self.productRQ4Name, self.refinedBucket)
        SourceDataDF = SourceDataDFTmp.withColumn("hash_key",
                                                  hash_("productsku", "companycd", "productname", "productlabel", "categoryid",
                                                        "defaultcost", "averagecost",
                                                        "unitcost", "mostrecentcost", "manufacturer", "manufacturerpartnumber",
                                                        "pricingtype", "defaultretailprice",
                                                        "defaultmargin", "floorprice", "pawfloorprice",
                                                        "defaultminimumquantity", "defaultmaximumquantity", "lockminmax",
                                                        "nosaleflag", "rmadays", "defaultinvoicecomments",
                                                        "serializedproductindicator", "serialnumberlength",
                                                        "discountable", "defaultdiscontinueddate", "datecreatedatsource",
                                                        "productactiveindicator", "ecommerceitem",
                                                        "warehouselocation", "defaultvendorname", "primaryvendorsku",
                                                        "costaccount", "revenueaccount",
                                                        "inventoryaccount", "inventorycorrectionsaccount",
                                                        "warrantydescription", "rmanumberrequired",
                                                        "warrantylengthunits", "warrantylengthvalue", "commissiondetailslocked",
                                                        "showoninvoice", "refundable",
                                                        "refundperiodlength", "refundtoused", "triggerservicerequestonsale",
                                                        "servicerequesttype",
                                                        "multilevelpricedetailslocked", "backorderdate", "storeinstoresku",
                                                        "storeinstoreprice",
                                                        "defaultdonotorder", "defaultspecialorder", "defaultdateeol",
                                                        "defaultwriteoff", "noautotaxes"))

        SourceDataDF.registerTempTable("CurrentSourceTempTable")

        if productPrevRefinedPath != '':

            self.sparkSession.read.parquet(productPrevRefinedPath).registerTempTable("PreviousDataRefinedTable")

            # selects target data with no change
            self.sparkSession.sql("SELECT a.productsku,a.companycd,a.productname,a.productlabel,a.categoryid,a.defaultcost,"
                                  "a.averagecost,a.unitcost,a.mostrecentcost,a.manufacturer,a.manufacturerpartnumber,a.pricingtype,"
                                  "a.defaultretailprice,a.defaultmargin,a.floorprice,a.pawfloorprice,a.defaultminimumquantity,"
                                  "a.defaultmaximumquantity,a.lockminmax,a.nosaleflag,a.rmadays,a.defaultinvoicecomments,"
                                  "a.serializedproductindicator,a.serialnumberlength,a.discountable,a.defaultdiscontinueddate,"
                                  "a.datecreatedatsource,a.productactiveindicator,a.ecommerceitem,a.warehouselocation,"
                                  "a.defaultvendorname,a.primaryvendorsku,a.costaccount,a.revenueaccount,a.inventoryaccount,"
                                  "a.inventorycorrectionsaccount,a.warrantydescription,a.rmanumberrequired,a.warrantylengthunits,"
                                  "a.warrantylengthvalue,a.commissiondetailslocked,a.showoninvoice,a.refundable,a.refundperiodlength,"
                                  "a.refundtoused,a.triggerservicerequestonsale,a.servicerequesttype,a.multilevelpricedetailslocked,"
                                  "a.backorderdate,a.storeinstoresku,a.storeinstoreprice,a.defaultdonotorder,a.defaultspecialorder,"
                                  "a.defaultdateeol,a.defaultwriteoff,a.noautotaxes,a.hash_key,"
                                  "b.year,b.month"
                                  " from PreviousDataRefinedTable a LEFT OUTER JOIN CurrentSourceTempTable b "
                                  " on a.productsku = b.productsku "
                                  " where a.hash_key = b.hash_key").registerTempTable("target_no_change_data")

            # selects source data which got updated
            dfProdUpdated = self.sparkSession.sql("SELECT a.productsku,a.companycd,a.productname,a.productlabel,a.categoryid,"
                                                  "a.defaultcost,"
                                                  "a.averagecost,a.unitcost,a.mostrecentcost,a.manufacturer,a.manufacturerpartnumber,"
                                                  "a.pricingtype,"
                                                  "a.defaultretailprice,a.defaultmargin,a.floorprice,a.pawfloorprice,"
                                                  "a.defaultminimumquantity,"
                                                  "a.defaultmaximumquantity,a.lockminmax,a.nosaleflag,a.rmadays,a.defaultinvoicecomments,"
                                                  "a.serializedproductindicator,a.serialnumberlength,a.discountable,"
                                                  "a.defaultdiscontinueddate,"
                                                  "a.datecreatedatsource,a.productactiveindicator,a.ecommerceitem,a.warehouselocation,"
                                                  "a.defaultvendorname,a.primaryvendorsku,a.costaccount,a.revenueaccount,"
                                                  "a.inventoryaccount,"
                                                  "a.inventorycorrectionsaccount,a.warrantydescription,a.rmanumberrequired,"
                                                  "a.warrantylengthunits,"
                                                  "a.warrantylengthvalue,a.commissiondetailslocked,a.showoninvoice,a.refundable,"
                                                  "a.refundperiodlength,"
                                                  "a.refundtoused,a.triggerservicerequestonsale,a.servicerequesttype,"
                                                  "a.multilevelpricedetailslocked,"
                                                  "a.backorderdate,a.storeinstoresku,a.storeinstoreprice,a.defaultdonotorder,"
                                                  "a.defaultspecialorder,"
                                                  "a.defaultdateeol,a.defaultwriteoff,a.noautotaxes,a.hash_key,"
                                                  "a.year,a.month"
                                                  " FROM CurrentSourceTempTable a LEFT OUTER JOIN PreviousDataRefinedTable b"
                                                  " on a.productsku = b.productsku "
                                                  " where a.hash_key <> b.hash_key")

            rowCountUpdateRecords = dfProdUpdated.count()
            dfProdUpdated.registerTempTable("src_updated_data")

            # selects new records from source
            dfProdNew = self.sparkSession.sql("SELECT a.productsku,a.companycd,a.productname,a.productlabel,a.categoryid,a.defaultcost,"
                                              "a.averagecost,a.unitcost,a.mostrecentcost,a.manufacturer,a.manufacturerpartnumber,"
                                              "a.pricingtype,"
                                              "a.defaultretailprice,a.defaultmargin,a.floorprice,a.pawfloorprice,"
                                              "a.defaultminimumquantity,"
                                              "a.defaultmaximumquantity,a.lockminmax,a.nosaleflag,a.rmadays,a.defaultinvoicecomments,"
                                              "a.serializedproductindicator,a.serialnumberlength,a.discountable,a.defaultdiscontinueddate,"
                                              "a.datecreatedatsource,a.productactiveindicator,a.ecommerceitem,a.warehouselocation,"
                                              "a.defaultvendorname,a.primaryvendorsku,a.costaccount,a.revenueaccount,a.inventoryaccount,"
                                              "a.inventorycorrectionsaccount,a.warrantydescription,a.rmanumberrequired,"
                                              "a.warrantylengthunits,"
                                              "a.warrantylengthvalue,a.commissiondetailslocked,a.showoninvoice,a.refundable,"
                                              "a.refundperiodlength,"
                                              "a.refundtoused,a.triggerservicerequestonsale,a.servicerequesttype,"
                                              "a.multilevelpricedetailslocked,"
                                              "a.backorderdate,a.storeinstoresku,a.storeinstoreprice,a.defaultdonotorder,"
                                              "a.defaultspecialorder,"
                                              "a.defaultdateeol,a.defaultwriteoff,a.noautotaxes,a.hash_key,"
                                              "a.year,a.month"
                                              " FROM CurrentSourceTempTable a LEFT OUTER JOIN PreviousDataRefinedTable b"
                                              " on a.productsku = b.productsku "
                                              " where b.productsku is null")

            rowCountNewRecords = dfProdNew.count()
            dfProdNew.registerTempTable("src_new_data")

            self.log.info('Updated prod skus are')
            dfProdUpdatedPrint = self.sparkSession.sql("select productsku from src_updated_data")
            self.log.info(dfProdUpdatedPrint.show(100, truncate=False))

            self.log.info('New added prod skus are')
            dfProdNewPrint = self.sparkSession.sql("select productsku from src_new_data")
            self.log.info(dfProdNewPrint.show(100, truncate=False))

            # union all extracted records
            final_cdc_data = self.sparkSession.sql(" SELECT productsku,companycd,productname,productlabel,categoryid,defaultcost,"
                                                   "averagecost,unitcost,mostrecentcost,manufacturer,manufacturerpartnumber,pricingtype,"
                                                   "defaultretailprice,defaultmargin,floorprice,pawfloorprice,defaultminimumquantity,"
                                                   "defaultmaximumquantity,lockminmax,nosaleflag,rmadays,defaultinvoicecomments,"
                                                   "serializedproductindicator,serialnumberlength,discountable,defaultdiscontinueddate,"
                                                   "datecreatedatsource,productactiveindicator,ecommerceitem,warehouselocation,"
                                                   "defaultvendorname,primaryvendorsku,costaccount,revenueaccount,inventoryaccount,"
                                                   "inventorycorrectionsaccount,warrantydescription,rmanumberrequired,warrantylengthunits,"
                                                   "warrantylengthvalue,commissiondetailslocked,showoninvoice,refundable,"
                                                   "refundperiodlength,refundtoused,triggerservicerequestonsale,servicerequesttype,"
                                                   "multilevelpricedetailslocked,backorderdate,storeinstoresku,storeinstoreprice,"
                                                   "defaultdonotorder,defaultspecialorder,defaultdateeol,defaultwriteoff,noautotaxes,"
                                                   "hash_key,year,month FROM target_no_change_data UNION ALL SELECT productsku,companycd,"
                                                   "productname,productlabel,categoryid,defaultcost,averagecost,unitcost,mostrecentcost,"
                                                   "manufacturer,manufacturerpartnumber,pricingtype,defaultretailprice,defaultmargin,"
                                                   "floorprice,pawfloorprice,defaultminimumquantity,defaultmaximumquantity,lockminmax,"
                                                   "nosaleflag,rmadays,defaultinvoicecomments,serializedproductindicator,"
                                                   "serialnumberlength,discountable,defaultdiscontinueddate,datecreatedatsource,"
                                                   "productactiveindicator,ecommerceitem,warehouselocation,defaultvendorname,"
                                                   "primaryvendorsku,costaccount,revenueaccount,inventoryaccount,"
                                                   "inventorycorrectionsaccount,warrantydescription,rmanumberrequired,warrantylengthunits,"
                                                   "warrantylengthvalue,commissiondetailslocked,showoninvoice,refundable,"
                                                   "refundperiodlength,refundtoused,triggerservicerequestonsale,servicerequesttype,"
                                                   "multilevelpricedetailslocked,backorderdate,storeinstoresku,storeinstoreprice,"
                                                   "defaultdonotorder,defaultspecialorder,defaultdateeol,defaultwriteoff,noautotaxes,"
                                                   "hash_key,year,month FROM src_updated_data UNION ALL SELECT productsku,companycd,"
                                                   "productname,productlabel,categoryid,defaultcost,averagecost,unitcost,mostrecentcost,"
                                                   "manufacturer,manufacturerpartnumber,pricingtype,defaultretailprice,defaultmargin,"
                                                   "floorprice,pawfloorprice,defaultminimumquantity,defaultmaximumquantity,lockminmax,"
                                                   "nosaleflag,rmadays,defaultinvoicecomments,serializedproductindicator,"
                                                   "serialnumberlength,discountable,defaultdiscontinueddate,datecreatedatsource,"
                                                   "productactiveindicator,ecommerceitem,warehouselocation,defaultvendorname,"
                                                   "primaryvendorsku,costaccount,revenueaccount,inventoryaccount,"
                                                   "inventorycorrectionsaccount,warrantydescription,rmanumberrequired,warrantylengthunits,"
                                                   "warrantylengthvalue,commissiondetailslocked,showoninvoice,refundable,"
                                                   "refundperiodlength,refundtoused,triggerservicerequestonsale,servicerequesttype,"
                                                   "multilevelpricedetailslocked,backorderdate,storeinstoresku,storeinstoreprice,"
                                                   "defaultdonotorder,defaultspecialorder,defaultdateeol,defaultwriteoff,noautotaxes,"
                                                   "hash_key,year,month FROM src_new_data")

            # Write final CDC data to output path
            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Changes noticed in the source file, creating a new file in the Refined layer partition")

                final_cdc_data.coalesce(1).select('productsku', 'companycd', 'productname', 'productlabel', 'categoryid',
                                                  'defaultcost', 'averagecost', 'unitcost', 'mostrecentcost', 'manufacturer',
                                                  'manufacturerpartnumber', 'pricingtype', 'defaultretailprice',
                                                  'defaultmargin', 'floorprice', 'pawfloorprice', 'defaultminimumquantity',
                                                  'defaultmaximumquantity', 'lockminmax', 'nosaleflag', 'rmadays',
                                                  'defaultinvoicecomments', 'serializedproductindicator', 'serialnumberlength',
                                                  'discountable', 'defaultdiscontinueddate', 'datecreatedatsource',
                                                  'productactiveindicator', 'ecommerceitem', 'warehouselocation',
                                                  'defaultvendorname', 'primaryvendorsku', 'costaccount', 'revenueaccount',
                                                  'inventoryaccount', 'inventorycorrectionsaccount', 'warrantydescription',
                                                  'rmanumberrequired', 'warrantylengthunits', 'warrantylengthvalue',
                                                  'commissiondetailslocked', 'showoninvoice', 'refundable',
                                                  'refundperiodlength', 'refundtoused', 'triggerservicerequestonsale',
                                                  'servicerequesttype', 'multilevelpricedetailslocked', 'backorderdate',
                                                  'storeinstoresku', 'storeinstoreprice', 'defaultdonotorder',
                                                  'defaultspecialorder', 'defaultdateeol', 'defaultwriteoff', 'noautotaxes',
                                                  'hash_key'). \
                    write.mode("overwrite"). \
                    format('parquet'). \
                    save(self.refinedWorkingPath)

                final_cdc_data.coalesce(1).select('productsku', 'companycd', 'productname', 'productlabel', 'categoryid',
                                                  'defaultcost', 'averagecost', 'unitcost', 'mostrecentcost', 'manufacturer',
                                                  'manufacturerpartnumber', 'pricingtype', 'defaultretailprice',
                                                  'defaultmargin', 'floorprice', 'pawfloorprice', 'defaultminimumquantity',
                                                  'defaultmaximumquantity', 'lockminmax', 'nosaleflag', 'rmadays',
                                                  'defaultinvoicecomments', 'serializedproductindicator', 'serialnumberlength',
                                                  'discountable', 'defaultdiscontinueddate', 'datecreatedatsource',
                                                  'productactiveindicator', 'ecommerceitem', 'warehouselocation',
                                                  'defaultvendorname', 'primaryvendorsku', 'costaccount', 'revenueaccount',
                                                  'inventoryaccount', 'inventorycorrectionsaccount', 'warrantydescription',
                                                  'rmanumberrequired', 'warrantylengthunits', 'warrantylengthvalue',
                                                  'commissiondetailslocked', 'showoninvoice', 'refundable',
                                                  'refundperiodlength', 'refundtoused', 'triggerservicerequestonsale',
                                                  'servicerequesttype', 'multilevelpricedetailslocked', 'backorderdate',
                                                  'storeinstoresku', 'storeinstoreprice', 'defaultdonotorder',
                                                  'defaultspecialorder', 'defaultdateeol', 'defaultwriteoff', 'noautotaxes',
                                                  'hash_key', 'year', 'month').write.mode("append"). \
                    partitionBy('year', 'month'). \
                    format('parquet'). \
                    save(self.productRefinedPartitonPath)
            else:
                self.log.info("No changes in the source file, not creating any files in the Refined layer")

        else:
            self.log.info("No files found in refined layer, writing the source data to refined")

            SourceDataDF.coalesce(1). \
                select('productsku', 'companycd', 'productname', 'productlabel', 'categoryid', 'defaultcost', 'averagecost',
                       'unitcost', 'mostrecentcost', 'manufacturer', 'manufacturerpartnumber', 'pricingtype',
                       'defaultretailprice', 'defaultmargin', 'floorprice', 'pawfloorprice', 'defaultminimumquantity',
                       'defaultmaximumquantity', 'lockminmax', 'nosaleflag', 'rmadays', 'defaultinvoicecomments',
                       'serializedproductindicator', 'serialnumberlength', 'discountable', 'defaultdiscontinueddate',
                       'datecreatedatsource', 'productactiveindicator', 'ecommerceitem', 'warehouselocation',
                       'defaultvendorname', 'primaryvendorsku', 'costaccount', 'revenueaccount', 'inventoryaccount',
                       'inventorycorrectionsaccount', 'warrantydescription', 'rmanumberrequired', 'warrantylengthunits',
                       'warrantylengthvalue', 'commissiondetailslocked', 'showoninvoice', 'refundable', 'refundperiodlength',
                       'refundtoused', 'triggerservicerequestonsale', 'servicerequesttype', 'multilevelpricedetailslocked',
                       'backorderdate', 'storeinstoresku', 'storeinstoreprice', 'defaultdonotorder', 'defaultspecialorder',
                       'defaultdateeol', 'defaultwriteoff', 'noautotaxes', 'hash_key'). \
                write.mode("overwrite"). \
                format('parquet'). \
                save(self.refinedWorkingPath)

            SourceDataDF.coalesce(1).select('productsku', 'companycd', 'productname', 'productlabel', 'categoryid',
                                            'defaultcost', 'averagecost', 'unitcost', 'mostrecentcost', 'manufacturer',
                                            'manufacturerpartnumber', 'pricingtype', 'defaultretailprice', 'defaultmargin',
                                            'floorprice', 'pawfloorprice', 'defaultminimumquantity', 'defaultmaximumquantity',
                                            'lockminmax', 'nosaleflag', 'rmadays', 'defaultinvoicecomments',
                                            'serializedproductindicator', 'serialnumberlength', 'discountable',
                                            'defaultdiscontinueddate', 'datecreatedatsource', 'productactiveindicator',
                                            'ecommerceitem', 'warehouselocation', 'defaultvendorname', 'primaryvendorsku',
                                            'costaccount', 'revenueaccount', 'inventoryaccount', 'inventorycorrectionsaccount',
                                            'warrantydescription', 'rmanumberrequired', 'warrantylengthunits',
                                            'warrantylengthvalue', 'commissiondetailslocked', 'showoninvoice', 'refundable',
                                            'refundperiodlength', 'refundtoused', 'triggerservicerequestonsale',
                                            'servicerequesttype', 'multilevelpricedetailslocked', 'backorderdate',
                                            'storeinstoresku', 'storeinstoreprice', 'defaultdonotorder', 'defaultspecialorder',
                                            'defaultdateeol', 'defaultwriteoff', 'noautotaxes', 'hash_key', 'year',
                                            'month').write.mode('append').partitionBy('year', 'month'). \
                format('parquet'). \
                save(self.productRefinedPartitonPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductDiscoveryToRefined().loadRefined()
