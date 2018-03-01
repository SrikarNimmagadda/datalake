import sys
from datetime import datetime
import boto3
from pyspark.sql import SparkSession


class DimStoreHierDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.refinedBucketWorking = sys.argv[1]
        self.storeHierCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]

        self.deliveryBucket = self.storeHierCurrentPath[self.storeHierCurrentPath.index('tb'):].split("/")[0]
        self.storeHierDeliveryName = self.storeHierCurrentPath[self.storeHierCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.storeHierCurrentPath[self.storeHierCurrentPath.index('tb'):].split("/")[2]

        self.prefixStorePath = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.prefixAttDealerPath = 'ATTDealerCode'

        self.storeHierPrevPath = "s3://" + self.deliveryBucket + '/' + self.storeHierDeliveryName + '/Previous'
        self.storeHierCurrentPath = "s3://" + self.deliveryBucket + '/' + self.storeHierDeliveryName + '/' + \
                                    self.currentName

        self.storeHierColumnsAlias = "a.hier_id,a.hier_nm,a.lvl_1_cd,a.lvl_1_nm,a.lvl_2_cd,a.lvl_2_nm,a.lvl_3_cd," \
                                     "a.lvl_3_nm,a.lvl_4_cd,a.lvl_4_nm,a.lvl_5_cd,a.lvl_5_nm,a.lvl_6_cd,a.lvl_6_nm"
        self.storeHierColumns = "hier_id,hier_nm,lvl_1_cd,lvl_1_nm,lvl_2_cd,lvl_2_nm,lvl_3_cd,lvl_3_nm,lvl_4_cd," \
                                "lvl_4_nm,lvl_5_cd,lvl_5_nm,lvl_6_cd,lvl_6_nm"

    def findLastModifiedFile(self, bucketNode, prefixType, bucket, currentOrPrev=1):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
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

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        lastUpdatedAttDealerCodeFile = self.findLastModifiedFile(refinedBucketNode, self.prefixAttDealerPath,
                                                                 self.refinedBucket)

        lastPrevUpdatedAttDealerCodeFile = self.findLastModifiedFile(refinedBucketNode, self.prefixAttDealerPath,
                                                                     self.refinedBucket, 0)

        self.sparkSession.read.parquet(lastUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code")

        lastUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStorePath, self.refinedBucket)

        lastPrevUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStorePath,
                                                             self.refinedBucket, 0)
        if lastPrevUpdatedStoreFile != '' and lastPrevUpdatedAttDealerCodeFile != '':

            self.sparkSession.read.parquet(lastUpdatedStoreFile).registerTempTable("store_refine_curr")
            self.sparkSession.read.parquet(lastUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code_curr")

            dfStoreHierCurr = self.sparkSession.sql("select concat(a.ATTRegion,a.ATTMarket) as hier_id,'ATT Hierarchy'"
                                                    + " as hier_nm,a.ATTRegion as lvl_1_cd,a.ATTRegion as lvl_1_nm, "
                                                    + "a.ATTMarketCode as lvl_2_cd, a.ATTMarket as lvl_2_nm, '' as "
                                                    + "lvl_3_cd, '' as lvl_3_nm,'' as lvl_4_cd, '' as lvl_4_nm, "
                                                    + "'' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm, "
                                                    + "'I' as cdc_ind_cd from att_dealer_code_curr a where"
                                                    + " a.ATTRegion != '' and a.ATTMarket != '' union select "
                                                    + "concat(b.SpringMarket,b.SpringRegion,b.SpringDistrict) as "
                                                    + "hier_id,'Spring Hierarchy' as hier_nm,b.SpringMarket as lvl_1_cd"
                                                    + ",b.SpringMarket as lvl_1_nm, b.SpringRegion as lvl_2_cd, "
                                                    + "b.SpringRegion as lvl_2_nm, b.SpringDistrict as lvl_3_cd, "
                                                    + "b.SpringDistrict as lvl_3_nm, '' as lvl_4_cd, '' as lvl_4_nm, "
                                                    + "'' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm,"
                                                    + " 'I' as cdc_ind_cd from store_refine_curr b where "
                                                    + "b.SpringMarket != '' and b.SpringRegion != '' and "
                                                    + "b.SpringDistrict != ''")

            self.sparkSession.read.parquet(lastPrevUpdatedStoreFile).registerTempTable("store_refine_prev")
            self.sparkSession.read.parquet(lastPrevUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code_prev")

            dfStoreHierPrev = self.sparkSession.sql("select concat(a.ATTRegion,a.ATTMarket) as hier_id,'ATT Hierarchy'"
                                                    + " as hier_nm,a.ATTRegion as lvl_1_cd,a.ATTRegion as lvl_1_nm, "
                                                    + "a.ATTMarketCode as lvl_2_cd, a.ATTMarket as lvl_2_nm, '' as "
                                                    + "lvl_3_cd, '' as lvl_3_nm,'' as lvl_4_cd, '' as lvl_4_nm, "
                                                    + "'' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm, "
                                                    + "'I' as cdc_ind_cd from att_dealer_code_prev a where "
                                                    + "a.ATTRegion != '' and a.ATTMarket != '' union select "
                                                    + "concat(b.SpringMarket,b.SpringRegion,b.SpringDistrict) as "
                                                    + "hier_id,'Spring Hierarchy' as hier_nm,b.SpringMarket as lvl_1_cd"
                                                    + ",b.SpringMarket as lvl_1_nm, b.SpringRegion as lvl_2_cd,"
                                                    + " b.SpringRegion as lvl_2_nm, b.SpringDistrict as lvl_3_cd, "
                                                    + "b.SpringDistrict as lvl_3_nm, '' as lvl_4_cd, '' as lvl_4_nm, "
                                                    + "'' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm, "
                                                    + "'I' as cdc_ind_cd from store_refine_prev b where b.SpringMarket"
                                                    + " != '' and b.SpringRegion != '' and b.SpringDistrict != ''")

            dfStoreHierCurr.subtract(dfStoreHierPrev).registerTempTable("store_hier_delta")
            dfStoreHierPrev.registerTempTable("store_hier_prev")
            dfStoreHierNew = self.sparkSession.sql(
                "select " + self.storeHierColumnsAlias + ",'I' as cdc_ind_cd from store_hier_delta a left join "
                                                         + "store_hier_prev b on a.hier_id = b.hier_id where"
                                                         + " b.hier_id is null")
            dfStoreHierUpdated = self.sparkSession.sql(
                "select " + self.storeHierColumnsAlias + ",'C' as cdc_ind_cd from store_hier_delta a left join "
                                                         + "store_hier_prev b on a.hier_id = b.hier_id where "
                                                         + "b.hier_id is not null")

            rowCountUpdateRecords = dfStoreHierUpdated.count()

            dfStoreHierUpdated.registerTempTable("store_hier_updated_data")

            rowCountNewRecords = dfStoreHierNew.count()

            dfStoreHierNew.registerTempTable("store_hier_new_data")

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                dfStoreHierDelta = self.sparkSession.sql(
                    "select " + self.storeHierColumns + ",cdc_ind_cd from store_hier_updated_data union all select " +
                    self.storeHierColumns + ",cdc_ind_cd from store_hier_new_data")

                dfStoreHierDelta.coalesce(1).write.mode("overwrite").csv(self.storeHierCurrentPath, header=True)
                dfStoreHierDelta.coalesce(1).write.mode("append").csv(self.storeHierPrevPath, header=True)
            else:
                self.log.info(" The prev and current are same. So no delta file will be generated in refined bucket.")
        else:
            self.log.info(" This is the first transaformation call, So keeping the file in delivery bucket.")
            self.sparkSession.read.parquet(lastUpdatedStoreFile).registerTempTable("store_refine_curr")
            self.sparkSession.read.parquet(lastUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code_curr")

            dfStoreHierCurr = self.sparkSession.sql("select concat(a.ATTRegion,a.ATTMarket) as hier_id,'ATT Hierarchy'"
                                                    + " as hier_nm,a.ATTRegion as lvl_1_cd,a.ATTRegion as lvl_1_nm,"
                                                    + " a.ATTMarketCode as lvl_2_cd, a.ATTMarket as lvl_2_nm, "
                                                    + "'' as lvl_3_cd, '' as lvl_3_nm,'' as lvl_4_cd, '' as lvl_4_nm, "
                                                    + "'' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm, "
                                                    + "'I' as cdc_ind_cd from att_dealer_code_curr a where "
                                                    + "a.ATTRegion != '' and a.ATTMarket != '' union select "
                                                    + "concat(b.SpringMarket,b.SpringRegion,b.SpringDistrict) as "
                                                    + "hier_id,'Spring Hierarchy' as hier_nm,b.SpringMarket as lvl_1_cd"
                                                    + ",b.SpringMarket as lvl_1_nm, b.SpringRegion as lvl_2_cd,"
                                                    + " b.SpringRegion as lvl_2_nm, b.SpringDistrict as lvl_3_cd,"
                                                    + " b.SpringDistrict as lvl_3_nm, '' as lvl_4_cd, '' as lvl_4_nm,"
                                                    + " '' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm,"
                                                    + " 'I' as cdc_ind_cd from store_refine_curr b where b.SpringMarket"
                                                    + " != '' and b.SpringRegion != '' and b.SpringDistrict != ''")
            dfStoreHierCurr.coalesce(1).write.mode("overwrite").csv(self.storeHierCurrentPath, header=True)
            dfStoreHierCurr.coalesce(1).write.mode("append").csv(self.storeHierPrevPath, header=True)
            self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreHierDelivery().loadDelivery()
