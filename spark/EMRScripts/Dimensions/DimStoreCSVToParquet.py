from pyspark.sql import SparkSession
import sys
import os
import csv
import boto3
from datetime import datetime
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
from pyspark.sql.functions import unix_timestamp, year, substring, from_unixtime


class DimStoreCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.locationMasterListWorkingFilePath = sys.argv[1]
        self.baeLocationWorkingFilePath = sys.argv[2]
        self.dealerCodesWorkingFilePath = sys.argv[3]
        self.multiTrackerStoreWorkingFilePath = sys.argv[4]
        self.springMobileStoreListWorkingFilePath = sys.argv[5]
        self.dtvLocationMasterWorkingFilePath = sys.argv[6]
        self.rawBucket = self.dtvLocationMasterWorkingFilePath[self.dtvLocationMasterWorkingFilePath.index('tb'):].split("/")[0]
        self.discoveryBucketWorking = sys.argv[7]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.storeName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.dataProcessingErrorPath = sys.argv[8] + '/Discovery'

        self.locationName = self.locationMasterListWorkingFilePath[self.locationMasterListWorkingFilePath.index('tb'):].split("/")[1]
        self.baeName = self.baeLocationWorkingFilePath[self.baeLocationWorkingFilePath.index('tb'):].split("/")[1]
        self.multiTrackerName = self.multiTrackerStoreWorkingFilePath[self.multiTrackerStoreWorkingFilePath.index('tb'):].split("/")[1]
        self.springMobileName = self.springMobileStoreListWorkingFilePath[self.springMobileStoreListWorkingFilePath.index('tb'):].split("/")[1]
        self.dtvLocationName = self.dtvLocationMasterWorkingFilePath[self.dtvLocationMasterWorkingFilePath.index('tb'):].split("/")[1]
        self.dealerName = self.dealerCodesWorkingFilePath[self.dealerCodesWorkingFilePath.index('tb'):].split("/")[1]
        self.fileFormat = ".csv"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.locationMasterListFile, self.locationHeader = self.searchFile(self.locationMasterListWorkingFilePath, self.locationName, 0)
        self.baeLocationFile, self.baeHeader = self.searchFile(self.baeLocationWorkingFilePath, self.baeName, 0)
        self.dealerCodesFile, self.dealerHeader = self.searchFile(self.dealerCodesWorkingFilePath, self.dealerName, 0)
        self.multiTrackerFile, self.multiTrackerHeader = self.searchFile(self.multiTrackerStoreWorkingFilePath, self.multiTrackerName, 0)
        self.springMobileStoreFile, self.springMobileHeader = self.searchFile(self.springMobileStoreListWorkingFilePath, self.springMobileName, 0)
        self.dTVLocationFile, self.dtvHeader = self.searchFile(self.dtvLocationMasterWorkingFilePath, self.dtvLocationName, 0)
        locationFileName, locationFileExtension = os.path.splitext(os.path.basename(self.locationMasterListFile))
        baeFileName, baeFileExtension = os.path.splitext(os.path.basename(self.baeLocationFile))
        dealerCOdesFileName, dealerCOdesFileExtension = os.path.splitext(os.path.basename(self.dealerCodesFile))
        multiTrackerFileName, multiTrackerFileExtension = os.path.splitext(os.path.basename(self.multiTrackerFile))
        springMobileFileName, springMobileFileExtension = os.path.splitext(os.path.basename(self.springMobileStoreFile))
        dtvFileName, dtvFileExtension = os.path.splitext(os.path.basename(self.dTVLocationFile))

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("Store Source files not in csv format.")
            self.copyFile(self.locationMasterListFile, self.dataProcessingErrorPath + '/' + self.locationName + datetime.now().strftime('%Y%m%d%H%M') + locationFileExtension)
            self.copyFile(self.baeLocationFile, self.dataProcessingErrorPath + '/' + self.baeName + datetime.now().strftime('%Y%m%d%H%M') + baeFileExtension)
            self.copyFile(self.dealerCodesFile, self.dataProcessingErrorPath + '/' + self.dealerName + datetime.now().strftime('%Y%m%d%H%M') + dealerCOdesFileExtension)
            self.copyFile(self.multiTrackerFile, self.dataProcessingErrorPath + '/' + self.multiTrackerName + datetime.now().strftime('%Y%m%d%H%M') + multiTrackerFileExtension)
            self.copyFile(self.springMobileStoreFile, self.dataProcessingErrorPath + '/' + self.springMobileName + datetime.now().strftime('%Y%m%d%H%M') + springMobileFileExtension)
            self.copyFile(self.dTVLocationFile, self.dataProcessingErrorPath + '/' + self.dtvLocationName + datetime.now().strftime('%Y%m%d%H%M') + dtvFileExtension)
            sys.exit()

        self.locationFileColumnCount = 54
        self.locationExpectedSourceColumns = ['StoreID', 'StoreName', 'Disabled', 'Abbreviation', 'ManagerEmployeeID', 'ManagerCommissionable', 'Address', 'City', 'StateProv', 'ZipPostal', 'Country', 'PhoneNumber', 'FaxNumber', 'DistrictName', 'RegionName', 'ChannelName', 'StoreType', 'GLCode', 'SquareFootage', 'LocationCode', 'Latitude', 'Longitude', 'AddressVerified', 'TimeZone', 'AdjustDST', 'CashPolicy', 'MaxCashDrawer', 'Serial_on_OE', 'Phone_on_OE', 'PAW_on_OE', 'Comment_on_OE', 'HideCustomerAddress', 'EmailAddress', 'GeneralLocationNotes', 'SaleInvoiceComment', 'StaffLevel', 'BankDetails', 'Taxes', 'LeaseStartDate', 'LeaseEndDate', 'Rent', 'PropertyTaxes', 'InsuranceAmount', 'OtherCharges', 'DepositTaken', 'LeaseNotes', 'InsuranceCompany', 'LandlordNotes', 'LandlordName', 'RelocationDate', 'UseLocationEmail', 'LocationEntityID', 'DateLastStatusChanged', 'UpdatedDate']
        self.baeFileColumnCount = 7
        self.baeExpectedSourceColumns = ['Store Number', 'Location', 'Market', 'Region', 'District', 'BAEWorkdayID', 'BSISWorkdayID']
        self.dealerCodesColumnCount = 37
        self.dealerCodesExpectedColumns = ['Dealer Code', 'Loc #', 'Location', 'Retail IQ', 'District', 'ATT Mkt Abbrev', 'ATT Market Name', 'Market', 'Region', 'Dispute Mkt', 'DF', 'C&C', 'WS', 'WS Expires', 'Footprint Level', 'Business Expert', 'DF Code', 'Old Code', 'Old Code 2', 'ATT Location Name', 'ATT Location ID', 'ATT Region', 'State', 'Notes', 'Notes2', 'Open Date', 'Close Date', 'DC Origin', 'Store Origin', 'Acquisition Origin', 'TB Loc', 'SMF Mapping', 'SMF Market', 'DC status', 'Sorting Rank', 'Rank Description', 'Company']
        self.multiTrackerColumnCount = 40
        self.multiTrackerExpectedColumns = ['Formula Link', 'AT&T Region', 'AT&T Market', 'Spring Region', 'Spring Market', 'Spring District', 'Loc', 'Store Name', 'Street Address', 'City, State, Zip', 'Square Feet', 'Total Monthly Rent', 'Lease Expiration', 'Average Last 12 Months Ops', 'Average Traffic Count Last 12 Months', 'Dealer Code', 'Exterior Photo', 'Interior Photo', 'Build Type', 'Store Type', 'C&C Designation', 'Remodel or Open Date', 'Signage Type', 'Pylon/Monument Panels', 'Selling Walls', 'Memorable Accessory Wall', 'Cash Wrap Expansion', 'Window Wrap Grpahics', 'Live DTV', 'Learning Tables', 'Community Table', 'Diamond Displays', 'C Fixtures', 'TIO Kiosk', 'Approved for Flex Blade', 'Cap Index Score', 'Authorized Retailer Tag Line', 'Selling Walls Notes']
        self.springMobileColumnCount = 24
        self.springMobileExpectedColumns = ['Store #', 'Store Name', 'Hyperion Store #', 'Open Date', 'Closed Date', 'Status', 'Comp', 'AcquisitionName', 'Region', 'Market', 'District', 'Region VP', 'Market Director', 'District Manager', 'RVP ID', 'MDIR ID', 'DM ID', 'Store Tier', 'SqFtRange', 'State', 'Zip', 'Attribute', 'Base', 'Classification', 'Same', 'Address', 'City', 'Sq Ft']
        self.dtvColumnCount = 2
        self.dtvExpectedColumns = ['Location', 'DTV Now Location']

        self.locationMasterListFile, self.locationHeader = self.searchFile(self.locationMasterListWorkingFilePath, self.locationName)
        self.log.info(self.locationMasterListFile)
        self.log.info("Location RQ4 Columns:" + ','.join(self.locationHeader))

        self.baeLocationFile, self.baeHeader = self.searchFile(self.baeLocationWorkingFilePath, self.baeName)
        self.log.info(self.baeLocationFile)
        self.log.info(self.baeHeader)
        self.baeCols = [column.replace(' ', '').replace('\r', '') for column in self.baeHeader]
        self.log.info("BAE Columns:" + ','.join(self.baeCols))

        self.dealerCodesFile, self.dealerHeader = self.searchFile(self.dealerCodesWorkingFilePath, self.dealerName)
        self.log.info(self.dealerCodesFile)
        self.log.info(self.dealerHeader)
        self.dealerCodesCols = [column.replace(' ', '') for column in self.dealerHeader]
        self.log.info("DealerCodes Columns:" + ','.join(self.dealerCodesCols))

        self.multiTrackerFile, self.multiTrackerHeader = self.searchFile(self.multiTrackerStoreWorkingFilePath, self.multiTrackerName)
        self.log.info(self.multiTrackerFile)
        self.log.info(column.decode('utf-8') for column in self.multiTrackerHeader)
        self.multiTrackerCols = [column.replace(' ', '').replace(',', '').replace('/', '').replace('&', '_')
                                 for column in self.multiTrackerHeader]
        # self.multiTrackerCols = self.multiTrackerCols[:-1]
        self.log.info("Multi Tracker Columns:" + ','.join(self.multiTrackerCols))

        self.springMobileStoreFile, self.springMobileHeader = self.searchFile(self.springMobileStoreListWorkingFilePath,
                                                                              self.springMobileName)
        self.log.info(self.springMobileStoreFile)
        self.log.info(column.decode('utf-8') for column in self.springMobileHeader)

        self.springMobileCols = [column.decode('ascii', 'ignore').replace(' ', '').replace('#', '').replace('/', '')
                                 for index, column in enumerate(self.springMobileHeader)]

        self.log.info("Spring Mobile Columns:" + ','.join(self.springMobileCols))

        self.dTVLocationFile, self.dtvHeader = self.searchFile(self.dtvLocationMasterWorkingFilePath, self.dtvLocationName)
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
        self.multiTrackerWorkingStoreCSVFilePath = 's3://' + self.discoveryBucket + '/' + self.storeName + '/' + \
                                                   self.multiTrackerName + '/csv'
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

    def searchFile(self, strS3url, name, isFileHeader=1):

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
        if name == self.multiTrackerName:
            for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
                if i == 2:
                    header = line
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

        # if self.locationHeader.__len__() >= self.locationFileColumnCount:
        locationColumnsMissing = [item for item in self.locationExpectedSourceColumns if item not in self.locationHeader]
        if locationColumnsMissing.__len__() <= 0:
            isValidLocationSchema = True

        isValidBAESchema = False

        # if self.baeHeader.__len__() >= self.baeFileColumnCount:
        baeColumnsMissing = [item for item in self.baeExpectedSourceColumns if item not in self.baeHeader]
        if baeColumnsMissing.__len__() <= 0:
            isValidBAESchema = True

        isValidDealerCodesSchema = False

        # if self.dealerHeader.__len__() >= self.dealerCodesColumnCount:
        dealerCodesColumnsMissing = [item for item in self.dealerCodesExpectedColumns if item not in self.dealerHeader]
        if dealerCodesColumnsMissing.__len__() <= 0:
            isValidDealerCodesSchema = True

        isValidMultiTrackerSchema = False

        # if self.multiTrackerHeader.__len__() >= self.multiTrackerColumnCount:
        multiTrackerColumnsMissing = [item for item in self.multiTrackerExpectedColumns if item not in self.multiTrackerHeader]
        if multiTrackerColumnsMissing.__len__() <= 0:
            isValidMultiTrackerSchema = True

        isValidSpringMobileSchema = False

        # if self.springMobileHeader.__len__() >= self.springMobileColumnCount:
        springMobileColumnsMissing = [item for item in self.springMobileExpectedColumns if item not in self.springMobileHeader]
        if springMobileColumnsMissing.__len__() <= 0:
            isValidSpringMobileSchema = True

        isValidDTVSchema = False

        # if self.dtvHeader.__len__() >= self.dtvColumnCount:
        dtvColumnsMissing = [item for item in self.dtvExpectedColumns if item not in self.dtvHeader]
        if dtvColumnsMissing.__len__() <= 0:
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
            self.copyFile(self.locationMasterListFile, self.dataProcessingErrorPath + '/' + self.locationName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            self.copyFile(self.baeLocationFile, self.dataProcessingErrorPath + '/' + self.baeName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            self.copyFile(self.dealerCodesFile, self.dataProcessingErrorPath + '/' + self.dealerName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            self.copyFile(self.multiTrackerFile, self.dataProcessingErrorPath + '/' + self.multiTrackerName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            self.copyFile(self.springMobileStoreFile, self.dataProcessingErrorPath + '/' + self.springMobileName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
            self.copyFile(self.dTVLocationFile, self.dataProcessingErrorPath + '/' + self.dtvLocationName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)
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

        # dfMultiTrackerStore.coalesce(1).write.mode("overwrite").csv(self.multiTrackerWorkingStoreCSVFilePath, header=True)

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
