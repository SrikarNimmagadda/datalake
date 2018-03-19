from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3
from urlparse import urlparse


class ProductRefinedToDelivery:
    def __init__(self):

        self.refinedProductWorkingPath = sys.argv[1]
        self.deliveryProductCurrentPath = sys.argv[2]
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.refinedBucket = self.refinedProductWorkingPath[self.refinedProductWorkingPath.index('tb'):].split("/")[0]
        self.prefixProductPath = self.refinedProductWorkingPath[self.refinedProductWorkingPath.index('tb'):].split("/")[1]
        self.deliveryBucket = self.deliveryProductCurrentPath[self.deliveryProductCurrentPath.index('tb'):].split("/")[0]
        self.deliveryName = self.deliveryProductCurrentPath[self.deliveryProductCurrentPath.index('tb'):].split("/")[1]

        self.deliveryProductPreviousPath = 's3://' + self.deliveryBucket + '/' + self.deliveryName + '/Previous'

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.prodColumns = "PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST," \
                           "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY," \
                           "NO_SALE_IND,RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND," \
                           "ECOM_ITM_IND,WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT," \
                           "INV_CRCT_ACCT,RMA_NBR_RQMT_IND,WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND," \
                           "SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,REFND_PRD_LEN,REFND_TO_USED_IND," \
                           "SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,STORE_INSTORE_SKU,STORE_INSTORE_PRC," \
                           "DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND," \
                           "CDC_IND_CD"

    def makeZeroByteFile(self, destinationPath, fileName):

        newBucketWithPath = urlparse(destinationPath)
        newBucket = newBucketWithPath.netloc
        newBucketNode = self.s3.Bucket(name=newBucket)
        newPath = newBucketWithPath.path.lstrip('/')
        objs = newBucketNode.objects.filter(Prefix=newPath)
        for s3Object in objs:
            s3Object.delete()

        newFile = open(fileName, 'w')
        newFile.close()
        self.client.upload_file(fileName, newBucket, newPath + '/' + fileName)

    def findLastModifiedFile(self, bucketNode, prefixType, bucket, currentOrPrev=1):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
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
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastUpdatedFilePath)

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

        lastUpdatedProdFile = self.findLastModifiedFile(refinedBucketNode, self.prefixProductPath, self.refinedBucket)

        lastPrevUpdatedProdFile = self.findLastModifiedFile(refinedBucketNode, self.prefixProductPath, self.refinedBucket, 0)

        if lastUpdatedProdFile != '':
            self.log.info("Last modified file name is : ")
            self.log.info(lastUpdatedProdFile)

            self.sparkSession.read.parquet(lastUpdatedProdFile). \
                registerTempTable("CurrentProdTableTmp")

            self.sparkSession.sql(
                "select a.productsku AS PROD_SKU,a.companycd AS CO_CD,"
                "a.productname AS PROD_NME,a.productlabel AS PROD_LBL,a.categoryid AS CAT_ID,"
                "a.defaultcost AS DFLT_COST,"
                "a.averagecost AS AVERAGE_CST,a.unitcost AS UNT_CST,a.mostrecentcost AS MST_RCNT_CST,"
                "a.manufacturername AS MFR,"
                "a.manufacturerpartnumber AS MFR_PRT_NBR,"
                "a.pricingtype AS PRC_TYP,a.defaultretailprice AS DFLT_RTL_PRC,a.defaultmargin AS DFLT_MRGN,"
                "a.floorprice AS FLOOR_PRC,"
                "a.defaultminimumquantity AS DFLT_MIN_QTY,"
                "a.defaultmaximumquantity AS DFLT_MAX_QTY,a.nosaleflag AS NO_SALE_IND,a.rmadays AS RMA_DAY,"
                "a.defaultinvoicecomments AS DFLT_INV_CMNT,"
                "a.discountable AS DSCNT_IND,"
                "a.defaultdiscontinueddate AS DFLT_DSCNT_DT,a.datecreatedatsource AS DT_CRT_AT_SRC,"
                "a.productactiveindicator AS PROD_ACT_IND,"
                "a.ecommerceitem AS ECOM_ITM_IND,a.warehouselocation as WHSE_LOC,"
                "a.defaultvendorname AS DFLT_VNDR_NME,a.primaryvendorsku AS PRI_VNDR_SKU,a.costaccount as CST_ACCT,"
                "a.revenueaccount AS RVNU_ACCT,a.inventoryaccount AS INV_ACCT,a.inventorycorrectionsaccount AS "
                "INV_CRCT_ACCT,"
                "a.rmanumberrequired AS RMA_NBR_RQMT_IND,a.warrantylengthunits AS WRNTY_LEN_UNTS,"
                "a.warrantylengthvalue AS WRNTY_LEN_VAL,"
                "a.commissiondetailslocked AS CMSN_DTL_LCK_IND,"
                "a.showoninvoice AS SHOW_ON_INV_IND,a.serializedproductindicator as SRIALIZED_PROD_IND,a.refundable "
                "AS REFND_IND,"
                "a.refundperiodlength AS REFND_PRD_LEN,"
                "a.refundtoused AS REFND_TO_USED_IND,a.servicerequesttype AS SRVC_RQST_TYP,"
                "a.multilevelpricedetailslocked AS MLTI_LVL_PRC_DTL_LCK_IND,a.backorderdate AS BAK_ORD_DT,"
                "a.storeinstoresku as STORE_INSTORE_SKU,"
                "a.storeinstoreprice as STORE_INSTORE_PRC,"
                "a.defaultdonotorder AS DFLT_DO_NOT_ORD_IND,a.defaultspecialorder AS DFLT_SPCL_ORD_IND,"
                "a.defaultdateeol AS DFLT_DT_EOL,a.defaultwriteoff AS DFLT_WRT_OFF_IND,"
                "a.noautotaxes AS NO_AUTO_TAXES_IND"
                " from CurrentProdTableTmp a"
            ).registerTempTable("CurrentProdTableTmpRenamed")

            self.sparkSession.sql(
                "select PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,"
                "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,"
                "RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,"
                "WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,"
                "WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,"
                "REFND_PRD_LEN,REFND_TO_USED_IND,SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,"
                "STORE_INSTORE_SKU,STORE_INSTORE_PRC,DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,"
                "DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND,"
                "hash(PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,"
                "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,"
                "RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,"
                "WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,"
                "WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,"
                "REFND_PRD_LEN,REFND_TO_USED_IND,SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,"
                "STORE_INSTORE_SKU,STORE_INSTORE_PRC,DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,"
                "DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND) as hash_key "
                "from CurrentProdTableTmpRenamed a"
            ).registerTempTable("CurrentProdTable")

        currTableCount = self.sparkSession.sql("select count(*) from CurrentProdTable").show()
        self.log.info("Current Table count is : ")
        self.log.info(currTableCount)

        if lastPrevUpdatedProdFile != '':
            self.log.info("Second Last modified file name is : " + lastPrevUpdatedProdFile)

            self.sparkSession.read.parquet(lastPrevUpdatedProdFile).registerTempTable("PrevProdTableTmp")

            self.sparkSession.sql(
                "select a.productsku AS PROD_SKU,a.companycd AS CO_CD,"
                "a.productname AS PROD_NME,a.productlabel AS PROD_LBL,a.categoryid AS CAT_ID,"
                "a.defaultcost AS DFLT_COST,"
                "a.averagecost AS AVERAGE_CST,a.unitcost AS UNT_CST,a.mostrecentcost AS MST_RCNT_CST,"
                "a.manufacturername AS MFR,"
                "a.manufacturerpartnumber AS MFR_PRT_NBR,"
                "a.pricingtype AS PRC_TYP,a.defaultretailprice AS DFLT_RTL_PRC,a.defaultmargin AS DFLT_MRGN,"
                "a.floorprice AS FLOOR_PRC,"
                "a.defaultminimumquantity AS DFLT_MIN_QTY,"
                "a.defaultmaximumquantity AS DFLT_MAX_QTY,a.nosaleflag AS NO_SALE_IND,a.rmadays AS RMA_DAY,"
                "a.defaultinvoicecomments AS DFLT_INV_CMNT,"
                "a.discountable AS DSCNT_IND,"
                "a.defaultdiscontinueddate AS DFLT_DSCNT_DT,a.datecreatedatsource AS DT_CRT_AT_SRC,"
                "a.productactiveindicator AS PROD_ACT_IND,"
                "a.ecommerceitem AS ECOM_ITM_IND,a.warehouselocation as WHSE_LOC,"
                "a.defaultvendorname AS DFLT_VNDR_NME,a.primaryvendorsku AS PRI_VNDR_SKU,a.costaccount as CST_ACCT,"
                "a.revenueaccount AS RVNU_ACCT,a.inventoryaccount AS INV_ACCT,a.inventorycorrectionsaccount AS "
                "INV_CRCT_ACCT,"
                "a.rmanumberrequired AS RMA_NBR_RQMT_IND,a.warrantylengthunits AS WRNTY_LEN_UNTS,"
                "a.warrantylengthvalue AS WRNTY_LEN_VAL,"
                "a.commissiondetailslocked AS CMSN_DTL_LCK_IND,"
                "a.showoninvoice AS SHOW_ON_INV_IND,a.serializedproductindicator as SRIALIZED_PROD_IND,a.refundable "
                "AS REFND_IND,"
                "a.refundperiodlength AS REFND_PRD_LEN,"
                "a.refundtoused AS REFND_TO_USED_IND,a.servicerequesttype AS SRVC_RQST_TYP,"
                "a.multilevelpricedetailslocked AS MLTI_LVL_PRC_DTL_LCK_IND,a.backorderdate AS BAK_ORD_DT,"
                "a.storeinstoresku as STORE_INSTORE_SKU,"
                "a.storeinstoreprice as STORE_INSTORE_PRC,"
                "a.defaultdonotorder AS DFLT_DO_NOT_ORD_IND,a.defaultspecialorder AS DFLT_SPCL_ORD_IND,"
                "a.defaultdateeol AS DFLT_DT_EOL,a.defaultwriteoff AS DFLT_WRT_OFF_IND,"
                "a.noautotaxes AS NO_AUTO_TAXES_IND"
                " from PrevProdTableTmp a"
            ).registerTempTable("PrevProdTableTmpRenamed")

            self.sparkSession.sql(
                "select PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,"
                "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,"
                "RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,"
                "WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,"
                "WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,"
                "REFND_PRD_LEN,REFND_TO_USED_IND,SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,"
                "STORE_INSTORE_SKU,STORE_INSTORE_PRC,DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,"
                "DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND,"
                "hash(PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,"
                "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,"
                "RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,"
                "WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,"
                "WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,"
                "REFND_PRD_LEN,REFND_TO_USED_IND,SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,"
                "STORE_INSTORE_SKU,STORE_INSTORE_PRC,DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,"
                "DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND) as hash_key "
                "from PrevProdTableTmpRenamed a"
            ).registerTempTable("PrevProdTable")

            prevTableCount = self.sparkSession.sql("select count(*) from PrevProdTable").show()
            self.log.info("Previous Table count is : ")
            self.log.info(prevTableCount)
        else:
            self.log.info("There is only 1 file, hence not computing the second latest file name")

        if lastUpdatedProdFile != '' and lastPrevUpdatedProdFile != '':
            self.log.info('Current and Previous refined data files are found. So processing for delivery layer starts')

            dfProdUpdated = self.sparkSession.sql(
                "select a.PROD_SKU,a.CO_CD,a.PROD_NME,a.PROD_LBL,a.CAT_ID,a.DFLT_COST,a.AVERAGE_CST,a.UNT_CST,"
                "a.MST_RCNT_CST,a.MFR,a.MFR_PRT_NBR,a.PRC_TYP,a.DFLT_RTL_PRC,a.DFLT_MRGN,a.FLOOR_PRC,"
                "a.DFLT_MIN_QTY,a.DFLT_MAX_QTY,a.NO_SALE_IND,a.RMA_DAY,a.DFLT_INV_CMNT,a.DSCNT_IND,"
                "a.DFLT_DSCNT_DT,a.DT_CRT_AT_SRC,a.PROD_ACT_IND,a.ECOM_ITM_IND,a.WHSE_LOC,a.DFLT_VNDR_NME,"
                "a.PRI_VNDR_SKU,a.CST_ACCT,a.RVNU_ACCT,a.INV_ACCT,a.INV_CRCT_ACCT,a.RMA_NBR_RQMT_IND,"
                "a.WRNTY_LEN_UNTS,a.WRNTY_LEN_VAL,a.CMSN_DTL_LCK_IND,a.SHOW_ON_INV_IND,a.SRIALIZED_PROD_IND,"
                "a.REFND_IND,a.REFND_PRD_LEN,a.REFND_TO_USED_IND,a.SRVC_RQST_TYP,a.MLTI_LVL_PRC_DTL_LCK_IND,"
                "a.BAK_ORD_DT,a.STORE_INSTORE_SKU,a.STORE_INSTORE_PRC,a.DFLT_DO_NOT_ORD_IND,a.DFLT_SPCL_ORD_IND,"
                "a.DFLT_DT_EOL,a.DFLT_WRT_OFF_IND,a.NO_AUTO_TAXES_IND,'C' as CDC_IND_CD"
                " from CurrentProdTable a LEFT OUTER JOIN PrevProdTable b "
                " on a.PROD_SKU = b.PROD_SKU where a.hash_key <> b.hash_key")

            rowCountUpdateRecords = dfProdUpdated.count()

            self.log.info("Number of update records are :" + str(rowCountUpdateRecords))

            dfProdUpdated.registerTempTable("prod_updated_data")

            dfProdNew = self.sparkSession.sql(
                "select a.PROD_SKU,a.CO_CD,a.PROD_NME,a.PROD_LBL,a.CAT_ID,a.DFLT_COST,a.AVERAGE_CST,a.UNT_CST,"
                "a.MST_RCNT_CST,a.MFR,a.MFR_PRT_NBR,a.PRC_TYP,a.DFLT_RTL_PRC,a.DFLT_MRGN,a.FLOOR_PRC,"
                "a.DFLT_MIN_QTY,a.DFLT_MAX_QTY,a.NO_SALE_IND,a.RMA_DAY,a.DFLT_INV_CMNT,a.DSCNT_IND,"
                "a.DFLT_DSCNT_DT,a.DT_CRT_AT_SRC,a.PROD_ACT_IND,a.ECOM_ITM_IND,a.WHSE_LOC,a.DFLT_VNDR_NME,"
                "a.PRI_VNDR_SKU,a.CST_ACCT,a.RVNU_ACCT,a.INV_ACCT,a.INV_CRCT_ACCT,a.RMA_NBR_RQMT_IND,"
                "a.WRNTY_LEN_UNTS,a.WRNTY_LEN_VAL,a.CMSN_DTL_LCK_IND,a.SHOW_ON_INV_IND,a.SRIALIZED_PROD_IND,"
                "a.REFND_IND,a.REFND_PRD_LEN,a.REFND_TO_USED_IND,a.SRVC_RQST_TYP,a.MLTI_LVL_PRC_DTL_LCK_IND,"
                "a.BAK_ORD_DT,a.STORE_INSTORE_SKU,a.STORE_INSTORE_PRC,a.DFLT_DO_NOT_ORD_IND,a.DFLT_SPCL_ORD_IND,"
                "a.DFLT_DT_EOL,a.DFLT_WRT_OFF_IND,a.NO_AUTO_TAXES_IND,'I' as CDC_IND_CD"
                " from CurrentProdTable a LEFT OUTER JOIN PrevProdTable b "
                " on a.PROD_SKU = b.PROD_SKU where b.PROD_SKU IS NULL")

            rowCountNewRecords = dfProdNew.count()

            self.log.info("Number of insert records are : ")
            self.log.info(rowCountNewRecords)

            dfProdNew.registerTempTable("prod_new_data")

            dfProdCatDelta = self.sparkSession.sql(
                "select " + self.prodColumns + " from prod_updated_data union all select " + self.prodColumns +
                " from prod_new_data")

            self.log.info('Updated prod skus are')
            dfProdUpdatedPrint = self.sparkSession.sql("select PROD_SKU from prod_updated_data")
            self.log.info(dfProdUpdatedPrint.show(100, truncate=False))

            self.log.info('New added prod skus are')
            dfProdNewPrint = self.sparkSession.sql("select PROD_SKU from prod_new_data")
            self.log.info(dfProdNewPrint.show(100, truncate=False))

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                dfProdCatDelta.coalesce(1).write.mode("overwrite").csv(self.deliveryProductCurrentPath, header=True)
                dfProdCatDelta.coalesce(1).write.mode("append").csv(self.deliveryProductPreviousPath, header=True)
            else:
                self.makeZeroByteFile(self.deliveryProductCurrentPath, 'wt_prod.csv')
                self.log.info("The prev and current files are same. No delta file will be generated in delivery bucket.")

        elif lastUpdatedProdFile != '' and lastPrevUpdatedProdFile == '':
            self.log.info("This is the first transformation call, So keeping the refined file in delivery bucket.")

            dfProdCurr = self.sparkSession.sql(
                "select PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,MFR,"
                "MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,RMA_DAY,"
                "DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,WHSE_LOC,DFLT_VNDR_NME,"
                "PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,WRNTY_LEN_UNTS,WRNTY_LEN_VAL,"
                "CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,REFND_PRD_LEN,REFND_TO_USED_IND,"
                "SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,STORE_INSTORE_SKU,STORE_INSTORE_PRC,"
                "DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND,'I' as CDC_IND_CD"
                " from CurrentProdTable")

            self.log.info("Current Path is : " + self.deliveryProductCurrentPath)
            self.log.info("Previous Path is : " + self.deliveryProductPreviousPath)

            dfProdCurr.coalesce(1).write.mode("overwrite").csv(self.deliveryProductCurrentPath, header=True)
            dfProdCurr.coalesce(1).write.mode("append").csv(self.deliveryProductPreviousPath, header=True)

        else:
            self.log.error("ERROR : This should not be printed, Please check the logs")

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductRefinedToDelivery().loadDelivery()
