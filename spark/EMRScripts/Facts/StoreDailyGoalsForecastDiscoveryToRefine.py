from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import year, substring, from_unixtime, unix_timestamp


class StoreDailyGoalsForecastDiscoveryToRefine(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryWorkingPath = sys.argv[1]
        self.refineWorkingPath = sys.argv[2]
        self.discoveryBucket = self.discoveryWorkingPath[self.discoveryWorkingPath.index('tb'):].split("/")[0]
        self.tableName = self.discoveryWorkingPath[self.discoveryWorkingPath.index('tb'):].split("/")[1]
        self.refinedBucket = self.refineWorkingPath[self.refineWorkingPath.index('tb'):].split("/")[0]
        self.storeDailyGoalsForecastPartitionPath = 's3://' + self.refinedBucket + '/' + self.tableName

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

    def loadRefined(self):

        s3 = boto3.resource('s3')

        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedStoreDailyGoalsForecastFile = self.findLastModifiedFile(discoveryBucketNode,
                                                                           self.tableName,
                                                                           self.discoveryBucket)
        self.sparkSession.read.parquet(lastUpdatedStoreDailyGoalsForecastFile).registerTempTable("StoreDailyGoalForecast")
        dfStoreDailyGoalForecast = self.sparkSession.sql("select date, case when daypercenttoforecast rlike '%' then cast(cast(regexp_replace(daypercenttoforecast,'%','') as float)/100 as decimal(8,4)) else daypercenttoforecast end as daypercenttoforecast from StoreDailyGoalForecast")

        dfStoreDailyGoalForecast.coalesce(1).write.mode("overwrite").parquet(
            self.refineWorkingPath)

        dfStoreDailyGoalForecast.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp())))\
            .withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.storeDailyGoalsForecastPartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreDailyGoalsForecastDiscoveryToRefine().loadRefined()
