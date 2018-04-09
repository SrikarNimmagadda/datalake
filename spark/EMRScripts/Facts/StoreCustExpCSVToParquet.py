import sys
import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, from_unixtime, unix_timestamp, substring
from datetime import datetime
import boto3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class StoreCustExpCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.inputWorkingPath = sys.argv[1]
        self.outputWorkingPath = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/Discovery/'

        self.rawBucket = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[0]
        self.discoveryBucket = self.outputWorkingPath[self.outputWorkingPath.index('tb'):].split("/")[0]
        self.storeCustExpName = self.outputWorkingPath[self.outputWorkingPath.index('tb'):].split("/")[1]
        self.workingName = self.outputWorkingPath[self.outputWorkingPath.index('tb'):].split("/")[2]

        self.storeCustExpFilePartitionPath = 's3://' + self.discoveryBucket + '/' + self.storeCustExpName
        self.storeCustExpFileWorkingPath = 's3://' + self.discoveryBucket + '/' + self.storeCustExpName + '/' + self.workingName

        self.fileFormat = ".csv"
        self.storeCustExpFile, self.storeCustExpFileHeader = self.searchFile(self.inputWorkingPath, 0)
        self.storeCustExpFileName, self.storeCustExpFileExtension = os.path.splitext(os.path.basename(self.storeCustExpFile))
        if self.fileFormat not in self.storeCustExpFileExtension:
            self.logger.error("StoreCustExp Source file not in csv format.")
            self.copyFile(self.storeCustExpFile, self.dataProcessingErrorPath + self.storeCustExpName + datetime.now().strftime('%Y%m%d%H%M') + self.storeCustExpFileExtension)
            sys.exit()

        self.storeCustExpExpectedColumns = ['Dealer Code', 'Location', '5 Key Behaviors', 'Effective Solutioning', 'Integrated Experience']
        self.storeCustExpFile, self.storeCustExpFileHeader = self.searchFile(self.inputWorkingPath)
        self.storeCustExpColumns = list(filter(None, self.storeCustExpFileHeader))
        self.storeCustExpColumns = [column.lower().replace(' ', '_').replace('5', 'five') for column in self.storeCustExpColumns]

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
            if i == 0:
                header = line
        self.logger.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def isValidFormatInSource(self):

        isValidStoreCustExpFormat = self.fileFormat in self.storeCustExpFileExtension

        if all([isValidStoreCustExpFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.logger.info("StoreCustExp column count " + str(self.storeCustExpFileHeader.__len__()))

        isValidStoreCustExpSchema = False

        storeCustExpColumnsMissing = [item for item in self.storeCustExpExpectedColumns if item not in self.storeCustExpFileHeader]
        if storeCustExpColumnsMissing.__len__() <= 0:
            isValidStoreCustExpSchema = True

        self.logger.info("isValidStoreCustExpSchema " + isValidStoreCustExpSchema.__str__())

        if all([isValidStoreCustExpSchema]):
            return True

        return False

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.logger.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def loadParquet(self):

        self.logger.info('Exception Handling starts')
        validReportDateFormat = True

        try:
            self.newformat = datetime.strptime(str(self.storeCustExpFileName.split("_")[-1]), '%Y%m%d%H%M').strftime('%m/%d/%Y')
            self.logger.info(self.newformat)
        except ValueError:
            validReportDateFormat = False

        if not validReportDateFormat:
            self.logger.error("StoreCustExp Source file report date not in required date format.")

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.logger.error("StoreCustExp Source file not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.logger.error("StoreCustExp Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema or not validReportDateFormat:
            self.logger.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.storeCustExpFile, self.dataProcessingErrorPath + self.storeCustExpName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            return

        self.logger.info('Source format and schema validation successful.')
        self.logger.info('Reading the input parquet file')

        dfStoreCustExp = self.spark.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.inputWorkingPath).toDF(*self.storeCustExpColumns)

        dfStoreCustExp = dfStoreCustExp.withColumn('report_date', lit(self.newformat))

        dfStoreCustExp.coalesce(1).write.mode('overwrite').format('parquet').save(self.outputWorkingPath)

        dfStoreCustExp.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).write.mode('append').partitionBy('year', 'month').format('parquet').save(self.storeCustExpFilePartitionPath)

        self.spark.stop()


if __name__ == "__main__":
    StoreCustExpCSVToParquet().loadParquet()
