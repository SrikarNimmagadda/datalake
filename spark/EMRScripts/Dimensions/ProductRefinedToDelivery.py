from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark import SparkConf, SparkContext
import boto3


class ProductRefinedToDelivery:
    def __init__(self):

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb-us'):]

        self.prodCurrentPath = 's3://' + self.deliveryBucket + '/Current'
        self.prodPreviousPath = 's3://' + self.deliveryBucket + '/Previous'

        self.prodColumns = "PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST," \
                           "MFR,MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY," \
                           "NO_SALE_IND,RMA_DAY,DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND," \
                           "ECOM_ITM_IND,WHSE_LOC,DFLT_VNDR_NME,PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT," \
                           "INV_CRCT_ACCT,RMA_NBR_RQMT_IND,WRNTY_LEN_UNTS,WRNTY_LEN_VAL,CMSN_DTL_LCK_IND," \
                           "SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,REFND_PRD_LEN,REFND_TO_USED_IND," \
                           "SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,STORE_INSTORE_SKU,STORE_INSTORE_PRC," \
                           "DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND," \
                           "CDC_IND_CD"

    def findLastModifiedFile(self, bucketNode, prefixPath, bucket, currentOrPrev=1):

        partitionName = bucketNode.objects.filter(Prefix=prefixPath)

        all_values_dict = {}
        req_values_dict = {}

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.items():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        lastUpdatedFilePath = ''
        lastPreviousRefinedPath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        Config = SparkConf().setAppName("ProductDelivery")
        SparkCtx = SparkContext(conf=Config)
        spark = SparkSession.builder.config(conf=Config).getOrCreate()

        Log4jLogger = SparkCtx._jvm.org.apache.log4j

        logger = Log4jLogger.LogManager.getLogger('prod_loadDelivery')

        s3 = boto3.resource('s3')

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)
        todayyear = datetime.now().strftime('%Y')
        datetime.now().strftime('%m')

        prodPrefixPath = "Product/year=" + todayyear
        lastUpdatedProdFile = self.findLastModifiedFile(
            refinedBucketNode, prodPrefixPath, self.refinedBucket)

        lastPrevUpdatedProdFile = self.findLastModifiedFile(
            refinedBucketNode, prodPrefixPath, self.refinedBucket, 0)

        if lastUpdatedProdFile != '':
            logger.info("Last modified file name is : ")
            logger.info(lastUpdatedProdFile)

            spark.read.parquet(lastUpdatedProdFile). \
                registerTempTable("CurrentProdTableTmp")

            spark.sql(
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

            spark.sql(
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

        currTableCount = spark.sql("select count(*) from CurrentProdTable").show()
        logger.info("Current Table count is : ")
        logger.info(currTableCount)

        if lastPrevUpdatedProdFile != '':
            logger.info("Second Last modified file name is : " + lastPrevUpdatedProdFile)

            spark.read.parquet(lastPrevUpdatedProdFile).registerTempTable("PrevProdTableTmp")

            spark.sql(
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

            spark.sql(
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

            prevTableCount = spark.sql("select count(*) from PrevProdTable").show()
            logger.info("Previous Table count is : ")
            logger.info(prevTableCount)
        else:
            logger.info("There is only 1 file, hence not computing the second latest file name")

        if lastUpdatedProdFile != '' and lastPrevUpdatedProdFile != '':
            logger.info('Current and Previous refined data files are found. So processing for delivery layer starts')

            dfProdUpdated = spark.sql(
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

            logger.info("Number of update records are :" + str(rowCountUpdateRecords))

            dfProdUpdated.registerTempTable("prod_updated_data")

            dfProdNew = spark.sql(
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

            logger.info("Number of insert records are : ")
            logger.info(rowCountNewRecords)

            dfProdNew.registerTempTable("prod_new_data")

            dfProdCatDelta = spark.sql(
                "select " + self.prodColumns + " from prod_updated_data union all select " + self.prodColumns +
                " from prod_new_data")

            logger.info('Updated prod skus are')
            dfProdUpdatedPrint = spark.sql("select PROD_SKU from prod_updated_data")
            logger.info(dfProdUpdatedPrint.show(100, truncate=False))

            logger.info('New added prod skus are')
            dfProdNewPrint = spark.sql("select PROD_SKU from prod_new_data")
            logger.info(dfProdNewPrint.show(100, truncate=False))

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                logger.info("Updated file has arrived..")
                dfProdCatDelta.coalesce(1).write.mode("overwrite").csv(self.prodCurrentPath, header=True)
                dfProdCatDelta.coalesce(1).write.mode("append").csv(self.prodPreviousPath, header=True)
            else:
                logger.info("The prev and current files are same. No delta file will be generated in delivery bucket.")

        elif lastUpdatedProdFile != '' and lastPrevUpdatedProdFile == '':
            logger.info("This is the first transformation call, So keeping the refined file in delivery bucket.")

            dfProdCurr = spark.sql(
                "select PROD_SKU,CO_CD,PROD_NME,PROD_LBL,CAT_ID,DFLT_COST,AVERAGE_CST,UNT_CST,MST_RCNT_CST,MFR,"
                "MFR_PRT_NBR,PRC_TYP,DFLT_RTL_PRC,DFLT_MRGN,FLOOR_PRC,DFLT_MIN_QTY,DFLT_MAX_QTY,NO_SALE_IND,RMA_DAY,"
                "DFLT_INV_CMNT,DSCNT_IND,DFLT_DSCNT_DT,DT_CRT_AT_SRC,PROD_ACT_IND,ECOM_ITM_IND,WHSE_LOC,DFLT_VNDR_NME,"
                "PRI_VNDR_SKU,CST_ACCT,RVNU_ACCT,INV_ACCT,INV_CRCT_ACCT,RMA_NBR_RQMT_IND,WRNTY_LEN_UNTS,WRNTY_LEN_VAL,"
                "CMSN_DTL_LCK_IND,SHOW_ON_INV_IND,SRIALIZED_PROD_IND,REFND_IND,REFND_PRD_LEN,REFND_TO_USED_IND,"
                "SRVC_RQST_TYP,MLTI_LVL_PRC_DTL_LCK_IND,BAK_ORD_DT,STORE_INSTORE_SKU,STORE_INSTORE_PRC,"
                "DFLT_DO_NOT_ORD_IND,DFLT_SPCL_ORD_IND,DFLT_DT_EOL,DFLT_WRT_OFF_IND,NO_AUTO_TAXES_IND,'I' as CDC_IND_CD"
                " from CurrentProdTable")

            logger.info("Current Path is : " + self.prodCurrentPath)
            logger.info("Previous Path is : " + self.prodPreviousPath)

            dfProdCurr.coalesce(1).write.mode("overwrite").csv(self.prodCurrentPath, header=True)
            dfProdCurr.coalesce(1).write.mode("append").csv(self.prodPreviousPath, header=True)

        else:
            logger.error("ERROR : This should not be printed, Please check the logs")

        spark.stop()


if __name__ == "__main__":
    ProductRefinedToDelivery().loadDelivery()
