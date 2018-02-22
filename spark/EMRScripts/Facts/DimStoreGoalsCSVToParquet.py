from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, unix_timestamp, from_unixtime, substring
import sys
import os
import csv
import boto3
from datetime import datetime
from urlparse import urlparse


class DimStoreGoalsCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.storeGoalsPath = sys.argv[1]
        self.discoveryBucketWorking = sys.argv[2]
        self.rawBucket = self.storeGoalsPath[self.storeGoalsPath.index('tb'):].split("/")[0]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.storeGoalsName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]

        self.storeGoalFilePartitionPath = 's3://' + self.discoveryBucket + '/' + self.storeGoalsName
        self.storeGoalFileWorkingPath = 's3://' + self.discoveryBucket + '/' + self.storeGoalsName + '/' + \
                                        self.workingName

    def searchFile(self, strS3url):

        s3 = boto3.resource('s3')
        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        filePath = ''
        fileName = ''
        file = ''
        for s3Object in objs:
            path, filename = os.path.split(s3Object.key)
            filePath = path
            fileName = filename
            file = "s3://" + bucket + "/" + s3Object.key
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file

    def loadParquet(self):

        storeGoalsFile = self.searchFile(self.storeGoalsPath)

        dfStoreGoals = self.sparkSession.sparkContext.textFile(storeGoalsFile). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: line[1] != 'Column1' and line[1] != 'Store').\
            toDF(['Store', 'Date', 'GrossProfit', 'PremiumVideo', 'DigitalLife', 'AccGPOROpp', 'CRUOpps', 'Tablets',
                  'IntegratedProducts', 'WTR', 'GoPhone', 'Overtime', 'DTVNow', 'AccessoryAttachRate', 'Broadband',
                  'Traffic', 'BroadbandDelete', 'ApprovedFTE', 'ApprovedHC', 'GoPhoneAutoEnrollment', 'Opps',
                  'SMTraffic', 'Entertainment']).drop("BroadbandDelete")

        newformat = datetime.now().strftime('%m/%d/%Y')
        self.log.info(newformat)

        dfStoreGoals = dfStoreGoals.withColumn('ReportDate', lit(newformat))

        dfStoreGoals.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.storeGoalFileWorkingPath)

        dfStoreGoals.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(self.storeGoalFilePartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreGoalsCSVToParquet().loadParquet()
