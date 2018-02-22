from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime


class DimStoreGoalsDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.refinedBucketWorking = sys.argv[1]
        self.storeGoalsCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]

        self.deliveryBucket = self.storeGoalsCurrentPath[self.storeGoalsCurrentPath.index('tb'):].split("/")[0]
        self.storeGoalsDeliveryName = self.storeGoalsCurrentPath[self.storeGoalsCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.storeGoalsCurrentPath[self.storeGoalsCurrentPath.index('tb'):].split("/")[2]

        self.prefixStoreGoalsPath = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.storeGoalsCurrentPath = 's3://' + self.deliveryBucket + '/' + self.storeGoalsDeliveryName + '/' + \
                                     self.currentName
        self.storeGoalsPreviousPath = 's3://' + self.deliveryBucket + '/' + self.storeGoalsDeliveryName + '/Previous'

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

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
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastUpdatedFilePath)

        return lastUpdatedFilePath

    def loadDelivery(self):

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        lastUpdatedStoreGoalsRefinedFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreGoalsPath,
                                                                     self.refinedBucket)

        self.sparkSession.read.parquet(lastUpdatedStoreGoalsRefinedFile).\
            dropDuplicates(subset=['ReportDate', 'KPIName', 'StoreNumber', 'CompanyCd']).\
            registerTempTable("StoreGoalsRefined_TT")

        dfFinalStoreGoals = self.sparkSession.sql(
            "select a.ReportDate as RPT_DT, a.KPIName as KPI_NM, a.StoreNumber as STORE_NUM, a.CompanyCd as CO_CD,"
            " a.GoalValue as GOAL_VAL from StoreGoalsRefined_TT a")

        dfFinalStoreGoals.coalesce(1).write.mode("overwrite").csv(self.storeGoalsCurrentPath, header=True)
        dfFinalStoreGoals.coalesce(1).write.mode("append").csv(self.storeGoalsPreviousPath, header=True)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreGoalsDelivery().loadDelivery()
