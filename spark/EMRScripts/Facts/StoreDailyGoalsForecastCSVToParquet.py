from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import year, unix_timestamp, from_unixtime, substring
import os
import boto3
import csv
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
from datetime import datetime


class StoreDailyGoalsForecastCSVToParquet(object):
    def __init__(self):

        self.storeDailyGoalForecastPath = sys.argv[1]
        self.discoveryBucketWorking = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/Discovery/'
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.tableName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.storeDailyGoalForecastPartitionPath = 's3://' + self.discoveryBucket + '/' + self.tableName
        self.storeDailyGoalForecastWorkingPath = self.discoveryBucketWorking

        self.fileFormat = ".csv"
        self.storeDailyGoalsForecastFileColumnCount = 3

        self.storeDailyGoalForecastFile, self.storeDailyGoalForecastHeader = self.searchFile(self.storeDailyGoalForecastPath, 0)
        self.storeDailyGoalsForecastFileName, self.storeDailyGoalsForecastFileExtension = os.path.splitext(os.path.basename(self.storeDailyGoalForecastFile))
        if self.fileFormat not in self.storeDailyGoalsForecastFileExtension:
            self.log.error("StoreDailyGoalForecast Source file not in csv format.")
            self.copyFile(self.storeDailyGoalForecastFile, self.dataProcessingErrorPath + self.tableName + datetime.now().strftime('%Y%m%d%H%M') + self.storeDailyGoalsForecastFileExtension)
            sys.exit()

        self.storeDailyGoalForecastFile, self.storeDailyGoalForecastHeader = self.searchFile(self.storeDailyGoalForecastPath)
        self.log.info(self.storeDailyGoalForecastFile)
        self.log.info("StoreDailyGoalForecastHeader SFTP Columns:" + ','.join(self.storeDailyGoalForecastHeader))
        self.storeDailyGoalForecastCols = [column.replace(' ', '').replace('%', 'percent').lower() for column in self.storeDailyGoalForecastHeader]
        self.storeDailyGoalForecastExpectedColumns = ['Date', 'Day % to Forecast', 'Gross Profit']

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
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        if isFileHeader == 0:
            return file, header
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 0:
                header = line

        return file, header

    def isValidFormatInSource(self):

        storeDailyGoalsForecastFileName, storeDailyGoalsForecastFileExtension = os.path.splitext(os.path.basename(self.storeDailyGoalForecastFile))

        isValidStoreDailyGoalsForecastFormat = self.fileFormat in storeDailyGoalsForecastFileExtension

        if all([isValidStoreDailyGoalsForecastFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("StoreDailyGoalsForecast column count " + str(self.storeDailyGoalForecastHeader.__len__()))

        isValidStoreDailyGoalsForecastSchema = False

        storeDailyGoalsForecastColumnsMissing = [item for item in self.storeDailyGoalForecastExpectedColumns if item not in self.storeDailyGoalForecastHeader]
        if storeDailyGoalsForecastColumnsMissing.__len__() <= 0:
            isValidStoreDailyGoalsForecastSchema = True

        self.log.info("isValidStoreDailyGoalsForecastSchema " + isValidStoreDailyGoalsForecastSchema.__str__())

        if all([isValidStoreDailyGoalsForecastSchema]):
            return True

        return False

    def loadDiscovery(self):
        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("StoreDailyGoalForecast Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("StoreDailyGoalForecast Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.storeDailyGoalForecastFile, self.dataProcessingErrorPath + self.tableName + datetime.now().strftime('%Y%m%d%H%M') +
                          self.fileFormat)
            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        dfStoreDailyGoalForecast = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.storeDailyGoalForecastPath).toDF(*self.storeDailyGoalForecastCols)

        # dfStoreDailyGoalForecast.withColumnRenamed("Date", "date"). \
        #     withColumnRenamed("Day % to Forecast", "daypercentforecast"). \
        #     withColumnRenamed("Daily Forecast", "dailyforecast").registerTempTable("StoreDailyGoalForecast")

        # dfStoreDailyGoalForecastFinal = self.sparkSession.sql("select date, daypercentforecast, dailyforecast from "
        #                                                       "StoreDailyGoalForecast")

        dfStoreDailyGoalForecast.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.storeDailyGoalForecastWorkingPath)

        dfStoreDailyGoalForecast.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(
            self.storeDailyGoalForecastPartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreDailyGoalsForecastCSVToParquet().loadDiscovery()
