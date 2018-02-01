from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import csv
from pyspark.sql.functions import unix_timestamp, year, substring, from_unixtime


class LocationMasterRQ4Parquet:

    def __init__(self):

        self.locationMasterList = sys.argv[1]
        self.baeLocation = sys.argv[2]
        self.dealerCodes = sys.argv[3]
        self.multiTrackerStore = sys.argv[4]
        self.springMobileStoreList = sys.argv[5]
        self.dtvLocationMasterList = sys.argv[6]

        self.locationStoreFilePath = sys.argv[7] + '/Store/Location'
        self.baeStoreFilePath = sys.argv[7] + '/Store/BAE'
        self.dealerStoreFilePath = sys.argv[7] + '/Store/Dealer'
        self.multiTrackerStoreFilePath = sys.argv[7] + '/Store/MultiTracker'
        self.springMobileStoreFilePath = sys.argv[7] + '/Store/SpringMobile'
        self.dtvLocationStoreFilePath = sys.argv[7] + '/Store/DTVLocation'

        self.locationStoreWorkingFilePath = sys.argv[7] + '/Store/Location/Working'
        self.baeStoreWorkingFilePath = sys.argv[7] + '/Store/BAE/Working'
        self.dealerStoreWorkingFilePath = sys.argv[7] + '/Store/Dealer/Working'
        self.multiTrackerWorkingStoreFilePath = sys.argv[7] + '/Store/MultiTracker/Working'
        self.springMobileStoreWorkingFilePath = sys.argv[7] + '/Store/SpringMobile/Working'
        self.dtvLocationStoreWorkingFilePath = sys.argv[7] + '/Store/DTVLocation/Working'

    def load_parquet(self):
        spark = SparkSession.builder.appName("LocationMasterRQ4Parquet").getOrCreate()

        #########################################################################################################
        # Reading the source data files #
        #########################################################################################################

        dfLocationMasterList = spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.locationMasterList).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

        dfBAELocation = spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.baeLocation).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

        dfBAELocation = dfBAELocation.withColumnRenamed("Store Number", "StoreNo").\
            withColumnRenamed("BSISWorkdayID\r", "BSISWorkdayID")

        dfDealerCodes = spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.dealerCodes).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

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
            withColumnRenamed("Company\r", "Company").\
            withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

        dfMultiTrackerStore = spark.sparkContext.textFile(self.multiTrackerStore). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: ''.join(line).strip() != '' and
                                line[1] != 'Formula Link' and line[2] != 'Spring Mobile Multi-Tracker').\
            toDF(['delete', 'FormulaLink', 'AT&TMarket', 'SpringMarket', 'Region', 'District',
                  'Loc', 'AT&TRegion', 'StoreName', 'StreetAddress', 'City_State_Zip',
                  'SquareFeet', 'TotalMonthlyRent', 'LeaseExpiration', 'November2017TotalOps',
                  'AverageLast12MonthsOps', 'AverageTrafficCountLast12Months', 'OctoberSMF',
                  'DealerCode', 'ExteriorPhoto', 'InteriorPhoto', 'BuildType', 'StoreType',
                  'C&CDesignation', 'RemodelorOpenDate', 'AuthorizedRetailerTagLine',
                  'PylonMonumentPanels', 'SellingWalls', 'MemorableAccessoryWall',
                  'CashWrapExpansion', 'WindowWrapGrpahics', 'LiveDTV', 'LearningTables',
                  'CommunityTable', 'DiamondDisplays', 'CFixtures', 'TIOKiosk',
                  'ApprovedforFlexBlade', 'CapIndexScore', 'SellingWallsNotes']).drop('delete')
        dfMultiTrackerStore = dfMultiTrackerStore.withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

        dfSpringMobileStoreList = spark.sparkContext.textFile(self.springMobileStoreList).\
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: line[0] not in {'Spring Mobile - AT&T', 'Store #'}).\
            toDF(['Store', 'StoreName', 'Address', 'City', 'State', 'Zip', 'Market', 'Region',
                  'District', 'State_delete', 'OpenDate', 'MarketVP', 'RegionDirector',
                  'DistrictManager', 'Classification', 'AcquisitionName', 'StoreTier', 'SqFt',
                  'SqFtRange', 'ClosedDate', 'Status', 'Attribute', 'Base', 'Comp', 'Same']).drop('State_delete')
        dfSpringMobileStoreList = dfSpringMobileStoreList.withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(from_unixtime(unix_timestamp())), 6, 2))

        dtvLocationSchema = StructType([StructField("Location", StringType(), True),
                                        StructField("DTVNowLocation", StringType(), True)])
        dfDTVLocation = spark.read.format("com.crealytics.spark.excel"). \
            option("location", self.dtvLocationMasterList).\
            option("treatEmptyValuesAsNulls", "true").\
            option("useHeader", "true").\
            option("sheetName", "Sheet1").\
            load(schema=dtvLocationSchema).\
            withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

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
        # Saving the parquet source data files in partition #
        #########################################################################################################

        dfLocationMasterList.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.locationStoreFilePath)

        dfBAELocation.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.baeStoreFilePath)

        dfDealerCodes.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.dealerStoreFilePath)

        dfMultiTrackerStore.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.multiTrackerStoreFilePath)

        dfSpringMobileStoreList.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.springMobileStoreFilePath)

        dfDTVLocation.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.dtvLocationStoreFilePath)

        spark.stop()


if __name__ == "__main__":
    LocationMasterRQ4Parquet().load_parquet()
