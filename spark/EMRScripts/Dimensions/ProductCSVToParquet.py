import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, from_unixtime, unix_timestamp, substring
import boto3
import os
import csv
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class ProductCSVToParquet(object):

    def __init__(self):

        self.productRQ4FilePath = sys.argv[1]
        self.couponsFilePath = sys.argv[2]
        self.productIdentifierFilePath = sys.argv[3]
        self.discoveryProductWorkingPath = sys.argv[4]
        self.dataProcessingErrorPath = sys.argv[5] + '/discovery'

        self.discoveryBucket = self.discoveryProductWorkingPath[self.discoveryProductWorkingPath.index('tb'):].split("/")[0]
        self.productRQ4Name = self.discoveryProductWorkingPath[self.discoveryProductWorkingPath.index('tb'):].split("/")[1]
        self.couponsName = self.couponsFilePath[self.couponsFilePath.index('tb'):].split("/")[1]
        self.productIdentifierName = self.productIdentifierFilePath[self.productIdentifierFilePath.index('tb'):].split("/")[1]
        self.workingName = self.discoveryProductWorkingPath[self.discoveryProductWorkingPath.index('tb'):].split("/")[2]
        self.productRQ4OutputWorkingPath = 's3://' + self.discoveryBucket + '/' + self.productRQ4Name + '/Working'
        self.couponsOutputWorkingPath = 's3://' + self.discoveryBucket + '/' + self.couponsName + '/Working'
        self.productIdentifierOutputPartitionPath = 's3://' + self.discoveryBucket + '/' + self.productIdentifierName
        self.productRQ4OutputPartitionPath = 's3://' + self.discoveryBucket + '/' + self.productRQ4Name
        self.couponsOutputPartitionPath = 's3://' + self.discoveryBucket + '/' + self.couponsName
        self.productIdentifierOutputWorkingPath = 's3://' + self.discoveryBucket + '/' + self.productIdentifierName + '/Working'

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.fileFormat = ".csv"

        self.productRQ4FileColumnCount = 61
        self.couponsFileColumnCount = 5
        self.productIdentifierFileColumnCount = 4

        self.productRQ4File, self.productRQ4Header = self.searchFile(self.productRQ4FilePath)
        self.log.info(self.productRQ4File)
        self.log.info("Product RQ4 Columns:" + ','.join(self.productRQ4Header))

        self.couponsFile, self.couponsHeader = self.searchFile(self.couponsFilePath)
        self.log.info(self.couponsFile)
        self.log.info("Coupons Columns:" + ','.join(self.couponsHeader))

        self.productIdentifierFile, self.productIdentifierHeader = self.searchFile(self.productIdentifierFilePath)
        self.log.info(self.productIdentifierFile)
        self.log.info("ProductIdentifier Columns:" + ','.join(self.productIdentifierHeader))

    def isValidFormatInSource(self):

        productRQ4FileName, productRQ4FileExtension = os.path.splitext(os.path.basename(self.productRQ4File))
        couponsFileName, couponsFileExtension = os.path.splitext(os.path.basename(self.couponsFile))
        productIdentifierFileName, productIdentifierFileExtension = os.path.splitext(os.path.basename(self.productIdentifierFile))

        isValidProductRQ4Format = self.fileFormat in productRQ4FileExtension
        isValidCouponsFormat = self.fileFormat in couponsFileExtension
        isValidProductIdentifierFormat = self.fileFormat in productIdentifierFileExtension

        if all([isValidProductRQ4Format, isValidCouponsFormat, isValidProductIdentifierFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("Product RQ4 column count " + str(self.productRQ4Header.__len__()))
        self.log.info("Coupons column count " + str(self.couponsHeader.__len__()))
        self.log.info("ProductIdentifier column count " + str(self.productIdentifierHeader.__len__()))

        isValidProductRQ4Schema = False

        if self.productRQ4Header.__len__() >= self.productRQ4FileColumnCount:
            isValidProductRQ4Schema = True

        isValidCouponsSchema = False

        if self.couponsHeader.__len__() >= self.couponsFileColumnCount:
            isValidCouponsSchema = True

        isValidProductIdentifierSchema = False

        if self.productIdentifierHeader.__len__() >= self.productIdentifierFileColumnCount:
            isValidProductIdentifierSchema = True

        self.log.info("isValidProductRQ4Schema " + isValidProductRQ4Schema.__str__() + "isValidCouponsSchema " +
                      isValidCouponsSchema.__str__() + "isValidProductIdentifierSchema " + isValidProductIdentifierSchema.__str__())

        if all([isValidProductRQ4Schema, isValidCouponsSchema, isValidProductIdentifierSchema]):
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
            body = s3Object.get()['Body'].read().decode('utf-8')
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 0:
                header = line

        return file, header

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("Product Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("Product Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.productRQ4File, self.dataProcessingErrorPath + '/' + self.productRQ4Name +
                          self.fileFormat)
            self.copyFile(self.couponsFile, self.dataProcessingErrorPath + '/' + self.couponsName + self.fileFormat)
            self.copyFile(self.productIdentifierFile, self.dataProcessingErrorPath + '/' + self.productIdentifierName + self.fileFormat)
            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        dfProduct = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.productRQ4FilePath).toDF(*self.productRQ4Header)
        dfProductIden = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.productIdentifierFilePath).toDF(*self.productIdentifierHeader)

        dfCoupons = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.couponsFilePath).toDF(*self.couponsHeader)

        dfProduct.coalesce(1).write.mode('overwrite').format('parquet').save(self.productRQ4OutputWorkingPath)

        dfCoupons.coalesce(1).write.mode('overwrite').format('parquet').save(self.couponsOutputWorkingPath)

        dfProductIden.coalesce(1).write.mode('overwrite').format('parquet').save(self.productIdentifierOutputWorkingPath)

        dfProduct.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.productRQ4OutputPartitionPath)

        dfCoupons.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.couponsOutputPartitionPath)

        dfProductIden.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.productIdentifierOutputPartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCSVToParquet().loadParquet()
