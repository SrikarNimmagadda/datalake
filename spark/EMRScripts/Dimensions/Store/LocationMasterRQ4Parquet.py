from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import csv
from datetime import datetime


class LocationMasterRQ4Parquet:

    def __init__(self):

        self.locationMasterList = sys.argv[1]
        self.baeLocation = sys.argv[2]
        self.dealerCodes = sys.argv[3]
        self.multiTrackerStore = sys.argv[4]
        self.springMobileStoreList = sys.argv[5]
        self.dtvLocationMasterList = sys.argv[6]
        self.todayYearWithMonth = datetime.now().strftime('%Y/%m')
        self.locationStoreFilePath = sys.argv[7] + '/' + \
                                     self.todayYearWithMonth + '/' + 'location' + '/' + sys.argv[8]
        self.baeStoreFilePath = sys.argv[7] + '/' + self.todayYearWithMonth + '/' + 'bae' + '/' + sys.argv[8]
        self.dealerStoreFilePath = sys.argv[7] + '/' + \
                                   self.todayYearWithMonth + '/' + 'dealer' + '/' + sys.argv[8]
        self.multiTrackerStoreFilePath = sys.argv[7] + '/' \
                                         + self.todayYearWithMonth + '/' + 'multi_tracker' + '/' + sys.argv[8]
        self.springMobileStoreFilePath = sys.argv[7] + '/' \
                                         + self.todayYearWithMonth + '/' + 'spring_mobile' + '/' + sys.argv[8]
        self.dtvLocationStoreFilePath = sys.argv[7] + '/' \
                                         + self.todayYearWithMonth + '/' + 'dtv_location' + '/' + sys.argv[8]

    def load_parquet(self):
        spark = SparkSession.builder.appName("LocationMasterRQ4Parquet").getOrCreate()
        dfLocationMasterList = spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.locationMasterList)

        dfBAELocation= spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.baeLocation)

        dfBAELocation = dfBAELocation.withColumnRenamed("Store Number", "StoreNo").withColumnRenamed("BSISWorkdayID\r","BSISWorkdayID")

        dfDealerCodes = spark.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.dealerCodes)

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
            withColumnRenamed("Company\r","Company")

        dfMultiTrackerStore = spark.sparkContext.textFile(self.multiTrackerStore). \
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).\
            filter(lambda line: ''.join(line).strip() != '' and
                                line[1] != 'Formula Link' and line[2] != 'Spring Mobile Multi-Tracker').\
            toDF(['delete', 'FormulaLink', 'AT&TMarket', 'SpringMarket','Region', 'District',
                  'Loc', 'AT&TRegion', 'StoreName', 'StreetAddress', 'City_State_Zip',
                  'SquareFeet', 'TotalMonthlyRent', 'LeaseExpiration', 'November2017TotalOps',
                  'AverageLast12MonthsOps', 'AverageTrafficCountLast12Months', 'OctoberSMF',
                  'DealerCode', 'ExteriorPhoto', 'InteriorPhoto', 'BuildType', 'StoreType',
                  'C&CDesignation', 'RemodelorOpenDate', 'AuthorizedRetailerTagLine',
                  'PylonMonumentPanels', 'SellingWalls', 'MemorableAccessoryWall',
                  'CashWrapExpansion', 'WindowWrapGrpahics', 'LiveDTV', 'LearningTables',
                  'CommunityTable', 'DiamondDisplays', 'CFixtures', 'TIOKiosk',
                  'ApprovedforFlexBlade', 'CapIndexScore', 'SellingWallsNotes']).drop('delete')

        dfSpringMobileStoreList = spark.sparkContext.textFile(self.springMobileStoreList).\
            mapPartitions(lambda partition: csv.
                          reader([line.encode('utf-8') for line in partition],delimiter=',', quotechar='"')).\
            filter(lambda line: line[0] not in {'Spring Mobile - AT&T', 'Store #'}).\
            toDF(['Store', 'StoreName', 'Address', 'City', 'State', 'Zip', 'Market', 'Region',
                  'District', 'State_delete', 'OpenDate', 'MarketVP', 'RegionDirector',
                  'DistrictManager', 'Classification', 'AcquisitionName', 'StoreTier', 'SqFt',
                  'SqFtRange', 'ClosedDate', 'Status', 'Attribute', 'Base', 'Comp', 'Same']).drop('State_delete')

        dtvLocationSchema = StructType([StructField("Location", StringType(), True),
                                        StructField("DTVNowLocation", StringType(), True)])
        dfDTVLocation = spark.read.format("com.crealytics.spark.excel"). \
            option("location", self.dtvLocationMasterList).\
            option("treatEmptyValuesAsNulls", "true").\
            option("useHeader", "true").\
            option("sheetName", "Sheet1").\
            load(schema=dtvLocationSchema)
            #.registerTempTable("dtv_location")

        #dfDTVLocation = spark.sql("select * from dtv_location")
        #dfDTVLocation.withColumnRenamed("DTV Now Location", "DTVNowLocation")

        #########################################################################################################
        # Reading the source data files #
        #########################################################################################################

        dfLocationMasterList.coalesce(1).select("*").write.parquet(self.locationStoreFilePath)

        dfBAELocation.coalesce(1).select("*").write.parquet(self.baeStoreFilePath)

        dfDealerCodes.coalesce(1).select("*").write.parquet(self.dealerStoreFilePath)

        dfMultiTrackerStore.coalesce(1).select("*").write.parquet(self.multiTrackerStoreFilePath)

        dfSpringMobileStoreList.coalesce(1).select("*").write.parquet(self.springMobileStoreFilePath)

        dfDTVLocation.coalesce(1).select("*").write.parquet(self.dtvLocationStoreFilePath)

        spark.stop()

if __name__ == "__main__":
    LocationMasterRQ4Parquet().load_parquet()