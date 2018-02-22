from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
import csv
import boto3
from urlparse import urlparse
from pyspark.sql.functions import unix_timestamp, year, substring, from_unixtime


class DimStoreParquet(object):

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

        self.locationName = "Location"
        self.baeName = "BAE"
        self.multiTrackerName = "MultiTracker"
        self.springMobileName = "SpringMobile"
        self.dtvLocationName = "DTVLocation"
        self.dealerName = "Dealer"
        self.fileFormat = ".csv"

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

        self.log.info('Reading the input parquet file')

        locationMasterListFile = self.searchFile(self.locationMasterList)

        dfLocationMasterList = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(locationMasterListFile)

        baeLocationFile = self.searchFile(self.baeLocation)

        dfBAELocation = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(baeLocationFile)

        dfBAELocation = dfBAELocation.withColumnRenamed("Store Number", "StoreNo").\
            withColumnRenamed("BSISWorkdayID\r", "BSISWorkdayID")

        dealerCodesFile = self.searchFile(self.dealerCodes)

        dfDealerCodes = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(dealerCodesFile)

        dfDealerCodes = dfDealerCodes.withColumnRenamed("Dealer Code", "DealerCode"). \
            withColumnRenamed("Loc #", "Loc#"). \
            withColumnRenamed("Retail IQ", "RetailIQ"). \
            withColumnRenamed("ATT Mkt Abbrev", "ATTMktAbbrev"). \
            withColumnRenamed("ATT Market Name", "ATTMarketName"). \
            withColumnRenamed("Dispute Mkt", "DisputeMkt"). \
            withColumnRenamed("WS Expires", "WSExpires"). \
            withColumnRenamed("Footprint Level", "FootprintLevel"). \
            withColumnRenamed("Business Expert", "BusinessExpert"). \
            withColumnRenamed("DF Code", "DFCode"). \
            withColumnRenamed("Old 2", "Old2"). \
            withColumnRenamed("ATT Location Name", "ATTLocationName"). \
            withColumnRenamed("ATT Location ID", "ATTLocationID"). \
            withColumnRenamed("ATT Region", "ATTRegion"). \
            withColumnRenamed("Open Date", "OpenDate"). \
            withColumnRenamed("Close Date", "CloseDate"). \
            withColumnRenamed("DC Origin", "DCOrigin"). \
            withColumnRenamed("Store Origin", "StoreOrigin"). \
            withColumnRenamed("Acquisition Origin", "AcquisitionOrigin"). \
            withColumnRenamed("TB Loc", "TBLoc"). \
            withColumnRenamed("SMF Mapping", "SMFMapping"). \
            withColumnRenamed("SMF Market", "SMFMarket"). \
            withColumnRenamed("DC status", "DCstatus"). \
            withColumnRenamed("Sorting Rank", "SortingRank"). \
            withColumnRenamed("Rank Description", "RankDescription").\
            withColumnRenamed("Company\r", "Company")

        multiTrackerFile = self.searchFile(self.multiTrackerStore)

        dfMultiTrackerStore = self.sparkSession.sparkContext.textFile(multiTrackerFile). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: ''.join(line).strip() != '' and
                                line[1] != 'Formula Link' and line[2] != 'Spring Mobile Multi-Tracker').\
            toDF(['delete', 'FormulaLink', 'AT&TRegion', 'AT&TMarket', 'SpringMarket', 'Region', 'District',
                  'Loc', 'StoreName', 'StreetAddress', 'City_State_Zip',
                  'SquareFeet', 'TotalMonthlyRent', 'LeaseExpiration', 'November2017TotalOps',
                  'AverageLast12MonthsOps', 'AverageTrafficCountLast12Months', 'OctoberSMF',
                  'DealerCode', 'ExteriorPhoto', 'InteriorPhoto', 'BuildType', 'StoreType',
                  'C&CDesignation', 'RemodelorOpenDate', 'AuthorizedRetailerTagLine',
                  'PylonMonumentPanels', 'SellingWalls', 'MemorableAccessoryWall',
                  'CashWrapExpansion', 'WindowWrapGrpahics', 'LiveDTV', 'LearningTables',
                  'CommunityTable', 'DiamondDisplays', 'CFixtures', 'TIOKiosk',
                  'ApprovedforFlexBlade', 'CapIndexScore', 'SellingWallsNotes']).drop('delete')

        springMobileStoreFile = self.searchFile(self.springMobileStoreList)

        dfSpringMobileStoreList = self.sparkSession.sparkContext.textFile(springMobileStoreFile).\
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: line[0] not in {'Spring Mobile - AT&T', 'Store #'}).\
            toDF(['Store', 'StoreName', 'Address', 'City', 'State', 'Zip', 'Market', 'Region',
                  'District', 'State_delete', 'OpenDate', 'MarketVP', 'RegionDirector',
                  'DistrictManager', 'blank_column', 'Classification', 'AcquisitionName', 'StoreTier', 'SqFt',
                  'SqFtRange', 'ClosedDate', 'Status', 'Attribute', 'Base', 'Comp', 'Same']).drop('State_delete',
                                                                                                  'blank_column')

        dtvLocationSchema = StructType([StructField("Location", StringType(), True),
                                        StructField("DTVNowLocation", StringType(), True)])

        dfDTVLocationFile = self.searchFile(self.dtvLocationMasterPath)

        dfDTVLocation = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(dfDTVLocationFile, schema=dtvLocationSchema)

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
    DimStoreParquet().loadParquet()
