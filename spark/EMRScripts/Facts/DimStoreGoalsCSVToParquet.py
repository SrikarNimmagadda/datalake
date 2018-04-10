from pyspark.sql import SparkSession
from pyspark.sql.functions import year, unix_timestamp, from_unixtime, substring, lit
import sys
import os
import csv
import boto3
from datetime import datetime
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class DimStoreGoalsCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.storeGoalsPath = sys.argv[1]
        self.discoveryBucketWorking = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/discovery'

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.fileFormat = ".csv"
        self.log.info('Exception Handling starts')
        self.storeGoalsFile, self.storeGoalsHeader = self.searchFile(self.storeGoalsPath, 0)
        self.storeGoalsFileName, self.storeGoalsFileExtension = os.path.splitext(os.path.basename(self.storeGoalsFile))
        if self.fileFormat not in self.storeGoalsFileExtension:
            self.log.error("StoreGoals Source file not in csv format.")
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.storeGoalsFile, self.dataProcessingErrorPath + '/' + self.storeGoalsName + datetime.now().strftime('%Y%m%d%H%M') + self.storeGoalsFileExtension)
            sys.exit()

        self.rawBucket = self.storeGoalsPath[self.storeGoalsPath.index('tb'):].split("/")[0]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.storeGoalsName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]

        self.storeGoalsFilePartitionPath = 's3://' + self.discoveryBucket + '/' + self.storeGoalsName
        self.storeGoalsFileWorkingPath = 's3://' + self.discoveryBucket + '/' + self.storeGoalsName + '/' + \
                                         self.workingName

        self.storeGoalsFile, self.storeGoalsHeader = self.searchFile(self.storeGoalsPath)
        self.log.info(self.storeGoalsFile)
        self.log.info(self.storeGoalsHeader)
        self.storeGoalsCols = [column.strip(' ') for index, column in enumerate(self.storeGoalsHeader)]
        self.storeGoalsCols = list(filter(None, self.storeGoalsCols))
        self.storeGoalsCols = [column.replace(' ', '_').replace('/', '1') for column in self.storeGoalsCols]
        self.log.info("StoreGoals Columns:" + ','.join(self.storeGoalsCols))
        self.selectQuery = "select " + ','.join(self.storeGoalsCols).strip(',') + ",ReportDate from  store_goal"

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url, isFileHeader=1):

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = self.s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        filePath = ''
        fileName = ''
        file = ''
        body = ''
        header = ''
        for s3Object in objs:
            path, filename = os.path.split(s3Object.key)
            filePath = path
            fileName = filename
            file = "s3://" + bucket + "/" + s3Object.key
            body = s3Object.get()['Body'].read()
        if isFileHeader == 0:
            return file, header
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 1:
                header = line
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def loadParquet(self):

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')
        dfStoreGoals = self.sparkSession.sparkContext.textFile(self.storeGoalsFile). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: line[1] != 'Column1' and line[1] != 'Store').toDF(self.storeGoalsCols)

        newformat = datetime.now().strftime('%m/%d/%Y')

        dfStoreGoals.withColumn('ReportDate', lit(newformat)).registerTempTable("store_goal")

        dfStoreGoalsFinal = self.sparkSession.sql(self.selectQuery)

        dfStoreGoalsFinal.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.storeGoalsFileWorkingPath)

        dfStoreGoalsFinal.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(self.storeGoalsFilePartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreGoalsCSVToParquet().loadParquet()
