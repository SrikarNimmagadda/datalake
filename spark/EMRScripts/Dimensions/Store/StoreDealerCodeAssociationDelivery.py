from pyspark.sql import SparkSession
import sys
import boto3
import datetime
from pyspark.sql.functions import col, hash


class StoreDealerCodeAssociationDelivery(object):

    def __init__(self):
        self.storeAssociationInput = sys.argv[1]
        self.storeAssociationOutput = sys.argv[2]

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb-us'):]

        self.prefixStoreDealerAssocRefineParttionPath = 'StoreDealerAssociation/year='

    def findLastModifiedFile(self, bucketNode, prefixPath, bucket, currentOrPrev=1):
        print("prefixPath is ", prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        all_values_dict = {}
        req_values_dict = {}
        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified
        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v
        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        print("Number of part files is : ", numFiles)
        lastUpdatedFilePath = ''
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            print("Last Modified file in s3 format is : ", lastUpdatedFilePath)

        elif numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + self.bucket + "/" + secondLastModifiedFileName
            print("Last Modified file in s3 format is : ", lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):
        spark = SparkSession.builder.appName("StoreDealerCodeAssociationDelivery").getOrCreate()

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        storeAssocPrefixPath = self.prefixStoreDealerAssocRefineParttionPath + datetime.now().strftime('%Y')
        lastUpdatedStoreAssocFile = self.findLastModifiedFile(refinedBucketNode, storeAssocPrefixPath, self.refinedBucket)
        spark.read.parquet(lastUpdatedStoreAssocFile).filter(col("DealerCode") != '').registerTempTable("storeAss")
        dfStoreDealerAssocCurr = spark.sql(
            "select cast(a.StoreNumber as integer) as STORE_NUM,a.DealerCode as DLR_CD,a.CompanyCode as "
            + "CO_CD,a.AssociationType as ASSOC_TYP, a.AssociationStatus as ASSOC_STAT from storeAss a "
            + "where AssociationType != '' or AssociationStatus != '' ")

        dfStoreDealerAssocCurr.\
            withColumn("Hash_Column", hash('STORE_NUM', 'DLR_CD', 'CO_CD', 'ASSOC_TYP', 'ASSOC_STAT')).\
            registerTempTable("store_dealercode_assoc_curr")

        lastPrevUpdatedStoreAssocFile = self.findLastModifiedFile(refinedBucketNode, storeAssocPrefixPath, self.refinedBucket, 0)

        spark.read.parquet(lastPrevUpdatedStoreAssocFile).filter(col("DealerCode") != '').registerTempTable("storeAss1")
        spark.sql(
            "select cast(a.StoreNumber as integer) as STORE_NUM,a.DealerCode as DLR_CD,a.CompanyCode as CO_CD,"
            + "a.AssociationType as ASSOC_TYP, a.AssociationStatus as ASSOC_STAT from storeAss1 a "
            + "where AssociationType != '' or AssociationStatus != '' ")

        dfStoreDealerAssocCurr.\
            withColumn("Hash_Column", hash('STORE_NUM', 'DLR_CD', 'CO_CD', 'ASSOC_TYP', 'ASSOC_STAT')).\
            registerTempTable("store_dealercode_assoc_prev")

        dfStoreDealerAssocCurr.coalesce(1).select("*").write.mode("overwrite").csv(self.storeAssociationOutput, header=True)

        spark.stop()


if __name__ == "__main__":
    StoreDealerCodeAssociationDelivery().loadDelivery()
