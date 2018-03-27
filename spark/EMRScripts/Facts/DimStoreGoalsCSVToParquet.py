from pyspark.sql import SparkSession
from pyspark.sql.functions import year, unix_timestamp, from_unixtime, substring
import sys
import os
import csv
import boto3
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

        self.storeGoalsFileColumnCount = 23
        self.storeGoalsExpectedColumns = ['Store', 'Date', 'Gross Profit', 'Premium Video', 'Digital Life', 'Acc GP/Opp', 'CRU Opps', 'Tablets', 'Integrated Products', 'WTR', 'Go Phone', 'Overtime', 'DTV Now', 'Accessory Attach Rate', 'Broadband', 'Traffic', 'Broadband', 'Approved FTE', 'Approved HC', 'GoPhone Auto Enrollment %', 'Opps', 'SM Traffic', 'Entertainment']
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.fileFormat = ".csv"

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
        self.storeGoalsCols = ['BroadbandDelete' if index in [16] else column.replace(' ', '').replace('/', 'OR').replace(
            '%', '') for index, column in enumerate(self.storeGoalsHeader)]
        self.log.info("StoreGoals Columns:" + ','.join(self.storeGoalsCols))
        self.storeGoalsCols = list(filter(None, self.storeGoalsCols))

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url):

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
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 1:
                header = line
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def isValidFormatInSource(self):

        storeGoalsFileName, storeGoalsFileExtension = os.path.splitext(os.path.basename(self.storeGoalsFile))

        isValidStoreGoalsFormat = self.fileFormat in storeGoalsFileExtension

        if all([isValidStoreGoalsFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("StoreGoals column count " + str(self.storeGoalsHeader.__len__()))

        isValidStoreGoalsSchema = False

        storeGoalsColumnsMissing = [item for item in self.storeGoalsExpectedColumns if item not in self.storeGoalsHeader]
        if storeGoalsColumnsMissing.__len__() <= 0:
            isValidStoreGoalsSchema = True

        self.log.info("isValidStoreGoalsSchema " + isValidStoreGoalsSchema.__str__())

        if all([isValidStoreGoalsSchema]):
            return True

        return False

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("StoreGoals Source file not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("StoreGoals Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.storeGoalsFile, self.dataProcessingErrorPath + '/' + self.storeGoalsName +
                          self.fileFormat)
            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')
        dfStoreGoals = self.sparkSession.sparkContext.textFile(self.storeGoalsFile). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: line[1] != 'Column1' and line[1] != 'Store').toDF(self.storeGoalsCols)

        dfStoreGoals.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.storeGoalsFileWorkingPath)

        dfStoreGoals.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(self.storeGoalsFilePartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreGoalsCSVToParquet().loadParquet()
