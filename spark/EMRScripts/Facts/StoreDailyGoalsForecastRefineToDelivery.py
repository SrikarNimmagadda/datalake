from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3


class StoreDailyGoalsForecastRefineToDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.refinedBucketWorking = sys.argv[1]
        self.deliveryCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.prefixPath = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.deliveryBucket = self.deliveryCurrentPath[self.deliveryCurrentPath.index('tb'):].split("/")[0]
        self.deliveryName = self.deliveryCurrentPath[self.deliveryCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.deliveryCurrentPath[self.deliveryCurrentPath.index('tb'):].split("/")[2]
        self.deliveryPreviousPath = 's3://' + self.deliveryBucket + '/' + self.deliveryName + '/Previous'

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.log.info("prefixPath is " + prefixPath)
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
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadDelivery(self):

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        lastUpdatedStoreDailyGoalForecastFile = self.findLastModifiedFile(refinedBucketNode, self.prefixPath,
                                                                          self.refinedBucket)

        self.sparkSession.read.parquet(lastUpdatedStoreDailyGoalForecastFile).\
            registerTempTable("StoreDailyGoalForecast")

        dfStoreDailyGoalForecast = self.sparkSession.sql("select date as RPT_DT, daypercenttoforecast as DAY_PCT_FCST "
                                                         "from StoreDailyGoalForecast")

        dfStoreDailyGoalForecast.coalesce(1).write.mode("overwrite").csv(self.deliveryCurrentPath, header=True)
        dfStoreDailyGoalForecast.coalesce(1).write.mode("append").csv(self.deliveryPreviousPath, header=True)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreDailyGoalsForecastRefineToDelivery().loadDelivery()
