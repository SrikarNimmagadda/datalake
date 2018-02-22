from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, lit


class StoreDealerCodeAssociationDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.refinedBucketWorking = sys.argv[1]
        self.storeDealerAssocCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]

        self.deliveryBucket = self.storeDealerAssocCurrentPath[
                              self.storeDealerAssocCurrentPath.index('tb'):].split("/")[0]
        self.storeDealerAssocDeliveryName = self.storeDealerAssocCurrentPath[
                                            self.storeDealerAssocCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.storeDealerAssocCurrentPath[self.storeDealerAssocCurrentPath.index('tb'):].split("/")[2]

        self.prefixStoreDealerAssocRefineParttionPath = self.refinedBucketWorking[
                                                        self.refinedBucketWorking.index('tb'):].split("/")[1]

        self.storeDealerAssocCurrentPath = 's3://' + self.deliveryBucket + '/' + self.storeDealerAssocDeliveryName \
                                           + '/' + self.currentName
        self.storeDealerAssocPreviousPath = 's3://' + self.deliveryBucket + '/' + self.storeDealerAssocDeliveryName \
                                            + '/Previous'

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

        storeAssocPrefixPath = self.prefixStoreDealerAssocRefineParttionPath
        lastUpdatedStoreAssocFile = self.findLastModifiedFile(refinedBucketNode, storeAssocPrefixPath,
                                                              self.refinedBucket)
        self.sparkSession.read.parquet(lastUpdatedStoreAssocFile).filter(col("DealerCode") != '').\
            registerTempTable("storeAss")
        dfStoreDealerAssocCurr = self.sparkSession.sql(
            "select cast(a.StoreNumber as integer) as STORE_NUM,a.DealerCode as DLR_CD,a.CompanyCode as "
            + "CO_CD,a.AssociationType as ASSOC_TYP, a.AssociationStatus as ASSOC_STAT from storeAss a "
            + "where AssociationType != '' or AssociationStatus != '' ")

        lastPrevUpdatedStoreAssocFile = self.findLastModifiedFile(refinedBucketNode, storeAssocPrefixPath,
                                                                  self.refinedBucket, 0)
        if lastPrevUpdatedStoreAssocFile != '':
            self.sparkSession.read.parquet(lastPrevUpdatedStoreAssocFile).filter(col("DealerCode") != '').\
                registerTempTable("storeAss1")
            dfStoreDealerAssocPrev = self.sparkSession.sql("select cast(a.StoreNumber as integer) as STORE_NUM, "
                                                           "a.DealerCode as DLR_CD,a.CompanyCode as CO_CD, "
                                                           "a.AssociationType as ASSOC_TYP, a.AssociationStatus as "
                                                           "ASSOC_STAT from storeAss1 a where AssociationType != '' or "
                                                           "AssociationStatus != '' ")

            dfStoreDealerAssocCurr.subtract(dfStoreDealerAssocPrev).registerTempTable("store_assoc_delta")
            dfStoreDealerAssocPrev.registerTempTable("store_assoc_prev")

            dfStoreAssocNew = self.sparkSession.sql(
                "select a.STORE_NUM,a.DLR_CD,a.CO_CD,a.ASSOC_TYP,a.ASSOC_STAT,'I' as cdc_ind_cd from "
                + "store_assoc_delta a left join store_assoc_prev b on a.DLR_CD = b.DLR_CD where b.DLR_CD is null")
            rowCountNewRecords = dfStoreAssocNew.count()

            dfStoreAssocUpdated = self.sparkSession.sql(
                "select a.STORE_NUM,a.DLR_CD,a.CO_CD,a.ASSOC_TYP,a.ASSOC_STAT,'C' as cdc_ind_cd from "
                + "store_assoc_delta a left join store_assoc_prev b on a.DLR_CD = b.DLR_CD where b.DLR_CD is not null")
            rowCountUpdateRecords = dfStoreAssocUpdated.count()

            dfStoreAssocUpdated.registerTempTable("store_assoc_updated_data")

            dfStoreAssocNew.registerTempTable("store_assoc_new_data")

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                dfStoreAssocDelta = self.sparkSession.sql(
                    "select STORE_NUM,DLR_CD,CO_CD,ASSOC_TYP,ASSOC_STAT,cdc_ind_cd from store_assoc_updated_data union"
                    + " all select STORE_NUM,DLR_CD,CO_CD,ASSOC_TYP,ASSOC_STAT,cdc_ind_cd from store_assoc_new_data")

                dfStoreAssocDelta.coalesce(1).write.mode("overwrite").csv(self.storeDealerAssocCurrentPath, header=True)
                dfStoreAssocDelta.coalesce(1).write.mode("append").csv(self.storeDealerAssocPreviousPath, header=True)
            else:
                self.log.info("The prev and current files are same.No delta file will be generated in refined bucket.")
        else:
            self.log.info(" This is the first transaformation call, So keeping the file in delivery bucket.")
            dfStoreDealerAssocCurr.coalesce(1).withColumn("cdc_ind_cd", lit('I')).write.mode("overwrite").\
                csv(self.storeDealerAssocCurrentPath, header=True)
            dfStoreDealerAssocCurr.coalesce(1).withColumn("cdc_ind_cd", lit('I')).write.mode("append").\
                csv(self.storeDealerAssocPreviousPath, header=True)
            self.sparkSession.stop()


if __name__ == "__main__":
    StoreDealerCodeAssociationDelivery().loadDelivery()
