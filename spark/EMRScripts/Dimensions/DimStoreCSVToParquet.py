from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
import csv
import boto3
from urlparse import urlparse
from pyspark.sql.functions import unix_timestamp, year, substring, from_unixtime


class DimStoreCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.locationMasterList = sys.argv[1]
        self.baeLocation = sys.argv[2]
        self.dealerCodes = sys.argv[3]
        self.multiTrackerStore = sys.argv[4]
        self.springMobileStoreList = sys.argv[5]
        self.dtvLocationMasterPath = sys.argv[6]
        self.rawBucket = self.dtvLocationMasterPath[self.dtvLocationMasterPath.index('tb'):].split("/")[0]
        self.discoveryBucketWorking = sys.argv[7]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.storeName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.dataProcessingErrorPath = sys.argv[8]

        self.locationName = "Location"
        self.baeName = "BAE"
        self.multiTrackerName = "MultiTracker"
        self.springMobileName = "SpringMobile"
        self.dtvLocationName = "DTVLocation"
        self.dealerName = "Dealer"
        self.fileFormat = ".csv"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.locationFileColumnCount = 54
        self.baeFileColumnCount = 7
        self.dealerCodesColumnCount = 37
        self.multiTrackerColumnCount = 40
        self.springMobileColumnCount = 24
        self.dtvColumnCount = 2

        self.locationMasterListFile, self.locationHeader = self.searchFile(self.locationMasterList, self.locationName)
        self.log.info(self.locationMasterListFile)
        self.log.info("Location RQ4 Columns:" + ','.join(self.locationHeader))

        self.baeLocationFile, self.baeHeader = self.searchFile(self.baeLocation, self.baeName)
        self.log.info(self.baeLocationFile)
        self.log.info(self.baeHeader)
        self.baeCols = [column.replace(' ', '').replace('\r', '') for column in self.baeHeader]
        self.log.info("BAE Columns:" + ','.join(self.baeCols))

        self.dealerCodesFile, self.dealerHeader = self.searchFile(self.dealerCodes, self.dealerName)
        self.log.info(self.dealerCodesFile)
        self.log.info(self.dealerHeader)
        self.dealerCodesCols = [column.replace(' ', '') for column in self.dealerHeader]
        self.log.info("DealerCodes Columns:" + ','.join(self.dealerCodesCols))

        self.multiTrackerFile, self.multiTrackerHeader = self.searchFile(self.multiTrackerStore, self.multiTrackerName)
        self.log.info(self.multiTrackerFile)
        self.log.info(column.decode('utf-8') for column in self.multiTrackerHeader)
        self.multiTrackerCols = [column.replace(' ', '').replace(',', '').replace('/', '').replace('&', '_') for column
                                 in self.multiTrackerHeader]
        self.log.info("Multi Tracker Columns:" + ','.join(self.multiTrackerCols))

        self.springMobileStoreFile, self.springMobileHeader = self.searchFile(self.springMobileStoreList,
                                                                              self.springMobileName)
        self.log.info(self.springMobileStoreFile)
        self.log.info(column.decode('utf-8') for column in self.springMobileHeader)

        self.springMobileCols = [column.decode('ascii', 'ignore').replace(' ', '').replace('#', '')
                                 for index, column in enumerate(self.springMobileHeader)]

        self.log.info("Spring Mobile Columns:" + ','.join(self.springMobileCols))

        self.dTVLocationFile, self.dtvHeader = self.searchFile(self.dtvLocationMasterPath, self.dtvLocationName)
        self.log.info(self.dTVLocationFile)
        self.log.info(column.decode('utf-8') for column in self.dtvHeader)
        self.dtvCols = [column.decode('ascii', 'ignore').replace(' ', '') for column in self.dtvHeader]

        self.log.info("DTV Columns:" + ','.join(self.dtvCols))

        self.locationStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                              self.locationName
        self.baeStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + self.baeName
        self.dealerStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                            self.dealerName
        self.multiTrackerStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                  self.multiTrackerName
        self.springMobileStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                  self.springMobileName
        self.dtvLocationStorePartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                 self.dtvLocationName

        self.locationStoreWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                            self.locationName + '/' + self.workingName
        self.baeStoreWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + self.baeName + \
                                       '/' + self.workingName
        self.dealerStoreWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                          self.dealerName + '/' + self.workingName
        self.multiTrackerWorkingStoreFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                self.multiTrackerName + '/' + self.workingName
        self.springMobileStoreWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                self.springMobileName + '/' + self.workingName
        self.dtvLocationStoreWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                               self.dtvLocationName + '/' + self.workingName

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url, name):

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
        if name == self.multiTrackerName:
            for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
                if i == 2:
                    header = line
        # elif name == self.springMobileName:
        #     for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
        #         if i == 1:
        #             header = line[:-1]
        else:
            for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
                if i == 0:
                    header = line

        return file, header

    def isValidFormatInSource(self):

        locationFileName, locationFileExtension = os.path.splitext(os.path.basename(self.locationMasterListFile))
        baeFileName, baeFileExtension = os.path.splitext(os.path.basename(self.baeLocationFile))
        dealerCOdesFileName, dealerCOdesFileExtension = os.path.splitext(os.path.basename(self.dealerCodesFile))
        multiTrackerFileName, multiTrackerFileExtension = os.path.splitext(os.path.basename(self.multiTrackerFile))
        springMobileFileName, springMobileFileExtension = os.path.splitext(os.path.basename(self.springMobileStoreFile))
        dtvFileName, dtvFileExtension = os.path.splitext(os.path.basename(self.dTVLocationFile))

        isValidLocationFormat = self.fileFormat in locationFileExtension
        isValidBAEFormat = self.fileFormat in baeFileExtension
        isValidDealerCodesFormat = self.fileFormat in dealerCOdesFileExtension
        isValidMultiTrackerFormat = self.fileFormat in multiTrackerFileExtension
        isValidSpringMobileFormat = self.fileFormat in springMobileFileExtension
        isValidDTVFormat = self.fileFormat in dtvFileExtension

        if all([isValidBAEFormat, isValidDealerCodesFormat, isValidDTVFormat, isValidLocationFormat,
                isValidMultiTrackerFormat, isValidSpringMobileFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("Location column count " + str(self.locationHeader.__len__()))
        self.log.info("BAE column count " + str(self.baeHeader.__len__()))
        self.log.info("Dealer Codes column count " + str(self.dealerHeader.__len__()))
        self.log.info("Multi Tracker column count " + str(self.multiTrackerHeader.__len__()))
        self.log.info("Spring Mobile column count " + str(self.springMobileHeader.__len__()))
        self.log.info("DTV column count " + str(self.dtvHeader.__len__()))

        isValidLocationSchema = False

        if self.locationHeader.__len__() >= self.locationFileColumnCount:
            isValidLocationSchema = True

        isValidBAESchema = False

        if self.baeHeader.__len__() >= self.baeFileColumnCount:
            isValidBAESchema = True

        isValidDealerCodesSchema = False

        if self.dealerHeader.__len__() >= self.dealerCodesColumnCount:
            isValidDealerCodesSchema = True

        isValidMultiTrackerSchema = False

        if self.multiTrackerHeader.__len__() >= self.multiTrackerColumnCount:
            isValidMultiTrackerSchema = True

        isValidSpringMobileSchema = False

        if self.springMobileHeader.__len__() >= self.springMobileColumnCount:
            isValidSpringMobileSchema = True

        isValidDTVSchema = False

        if self.dtvHeader.__len__() >= self.dtvColumnCount:
            isValidDTVSchema = True

        self.log.info("isValidBAESchema " + isValidBAESchema.__str__() + "isValidDealerCodesSchema " +
                      isValidDealerCodesSchema.__str__() + "isValidDTVSchema " + isValidDTVSchema.__str__() +
                      "isValidLocationSchema " + isValidLocationSchema.__str__() + "isValidMultiTrackerSchema " +
                      isValidMultiTrackerSchema.__str__() + "isValidSpringMobileSchema " +
                      isValidSpringMobileSchema.__str__())

        if all([isValidBAESchema, isValidDealerCodesSchema, isValidDTVSchema, isValidLocationSchema,
                isValidMultiTrackerSchema, isValidSpringMobileSchema]):
            return True

        return False

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("Store Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("Store Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.locationMasterListFile, self.dataProcessingErrorPath + '/' + self.locationName +
                          self.fileFormat)
            self.copyFile(self.baeLocationFile, self.dataProcessingErrorPath + '/' + self.baeName + self.fileFormat)
            self.copyFile(self.dealerCodesFile, self.dataProcessingErrorPath + '/' + self.dealerName + self.fileFormat)
            self.copyFile(self.multiTrackerFile, self.dataProcessingErrorPath + '/' + self.multiTrackerName +
                          self.fileFormat)
            self.copyFile(self.springMobileStoreFile, self.dataProcessingErrorPath + '/' + self.springMobileName +
                          self.fileFormat)
            self.copyFile(self.dTVLocationFile, self.dataProcessingErrorPath + '/' + self.dtvLocationName +
                          self.fileFormat)
            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        dfLocationMasterList = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.locationMasterListFile).toDF(*self.locationHeader)

        dfBAELocation = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.baeLocationFile).toDF(*self.baeCols)

        dfDealerCodes = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.dealerCodesFile).toDF(*self.dealerCodesCols)

        dfMultiTrackerStore = self.sparkSession.sparkContext.textFile(self.multiTrackerFile). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: ''.join(line).strip() != '' and
                                line[1] != 'Formula Link' and line[2] != 'Spring Mobile Multi-Tracker').\
            toDF(self.multiTrackerCols)

        dfSpringMobileStoreList = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.springMobileStoreFile).toDF(*self.springMobileCols)

        # dfSpringMobileStoreList = self.sparkSession.sparkContext.textFile(self.springMobileStoreFile).\
        #     mapPartitions(lambda partition: csv.
        #                   reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
        #     filter(lambda line: line[0] not in {'Spring Mobile - AT&T', 'Store #'}).\
        #     toDF(self.springMobileCols)

        dfDTVLocation = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("encoding", "UTF-8"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.dTVLocationFile).toDF(*self.dtvCols)

        #########################################################################################################
        # Saving the parquet source data files in working #
        #########################################################################################################

        dfLocationMasterList.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.locationStoreWorkingFilePath)

        dfBAELocation.coalesce(1).write.mode('overwrite').format('parquet').save(self.baeStoreWorkingFilePath)

        dfDealerCodes.coalesce(1).write.mode('overwrite').format('parquet').save(self.dealerStoreWorkingFilePath)

        dfMultiTrackerStore.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.multiTrackerWorkingStoreFilePath)

        dfSpringMobileStoreList.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.springMobileStoreWorkingFilePath)

        dfDTVLocation.coalesce(1).write.mode('overwrite').format('parquet').save(self.dtvLocationStoreWorkingFilePath)

        #########################################################################################################
        # Saving the parquet source data files using partition #
        #########################################################################################################

        dfLocationMasterList.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.locationStorePartitionFilePath)

        dfBAELocation.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.baeStorePartitionFilePath)

        dfDealerCodes.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.dealerStorePartitionFilePath)

        dfMultiTrackerStore.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.multiTrackerStorePartitionFilePath)

        dfSpringMobileStoreList.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.springMobileStorePartitionFilePath)

        dfDTVLocation.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.dtvLocationStorePartitionFilePath)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreCSVToParquet().loadParquet()
