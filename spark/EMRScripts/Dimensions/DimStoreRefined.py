from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, regexp_replace, col, when, hash, from_unixtime, \
    unix_timestamp, substring, year, lit
from pyspark.sql.types import IntegerType, DateType
import boto3
import sys
from pyspark.sql.utils import AnalysisException
from datetime import datetime


class DimStoreRefined(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryBucketWorking = sys.argv[1]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.refinedBucketWorking = sys.argv[2]
        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.storeName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[2]
        self.storeWorkingPath = 's3://' + self.refinedBucket + '/' + self.storeName + '/' + self.workingName
        self.storePartitonPath = 's3://' + self.refinedBucket + '/' + self.storeName
        self.storeCSVPath = 's3://' + self.refinedBucket + '/' + self.storeName + '/' + 'csv'
        self.dataProcessingErrorPath = sys.argv[3]

        self.locationName = "Location"
        self.baeName = "BAE"
        self.multiTrackerName = "MultiTracker"
        self.springMobileName = "SpringMobile"
        self.dtvLocationName = "DTVLocation"
        self.dealerName = "Dealer"
        self.storeRefinedName = "StoreRefined"

        self.prefixLocationDiscoveryPath = self.storeName + '/' + self.locationName
        self.prefixBaeDiscoveryPath = self.storeName + '/' + self.baeName
        self.prefixDealerDiscoveryPath = self.storeName + '/' + self.dealerName
        self.prefixSpringMobileDiscoveryPath = self.storeName + '/' + self.springMobileName
        self.prefixMultitrackerDiscoveryPath = self.storeName + '/' + self.multiTrackerName
        self.prefixDtvLocationDiscoveryPath = self.storeName + '/' + self.dtvLocationName

        self.prefixStoreRefinedPath = self.storeWorkingPath

        self.sundayTimeExp = "(([U]: Closed)|([U]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.mondayTimeExp = "(([M]: Closed)|([M]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.tuedayTimeExp = "(([T]: Closed)|([T]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.weddayTimeExp = "(([W]: Closed)|([W]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.thudayTimeExp = "(([R]: Closed)|([R]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.fridayTimeExp = "(([F]: Closed)|([F]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"
        self.satdayTimeExp = "(([S]: Closed)|([S]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):" \
                             "([0-5][0-9]) ([AP][Mm])))"

        self.storeColumns = "StoreNumber,CompanyCd,SourceStoreIdentifier,LocationName,Abbreviation,GLCode," \
                            + "StoreStatus,StoreManagerEmployeeId,ManagerCommisionableIndicator,Address,City," \
                            + "StateProvince,PostalCode,Country,Phone,Fax,StoreType,StaffLevel,SquareFootRange," \
                            + "SquareFoot,Lattitude,Longitude,Timezone,AdjustDST,CashPolicy,MaxCashDrawer," \
                            + "SerialOnOEIndicator,PhoneOnOEIndicator,PAWOnOEIndicator,CommentOnOEIndicator," \
                            + "HideCustomerAddressIndicator,EmailAddress,ConsumerLicenseNumber,SaleInvoiceComment," \
                            + "Taxes,Rent,PropertyTaxes,InsuranceAmount,OtherCharges,Deposit,LandlordName," \
                            + "UseLocationEmailIndicator,LocationType,LandlordNote,LeaseStartDate,LeaseEndDate," \
                            + "LeaseNotes,StoreOpenDate,StoreCloseDate,RelocationDate,StoreTier,MondayOpenTime," \
                            + "MondayCloseTime,TuesdayOpenTime,TuesdayCloseTime,WednesdayOpenTime,WednesdayCloseTime," \
                            + "ThursdayOpenTime,ThursdayCloseTime,FridayOpenTime,FridayCloseTime,SaturdayOpenTime," \
                            + "SaturdayCloseTime,SundayOpenTime,SundayCloseTime,AcquisitionName,BaseStoreIndicator," \
                            + "CompStoreIndicator,SameStoreIndicator,FranchiseStoreIndicator,LeaseExpiration," \
                            + "BuildTypeCode,C_CDesignation,AuthorizedRetailedTagLineStatusIndicator," \
                            + "PylonMonumentPanels,SellingWalls,MemorableAccessoryWall,CashWrapExpansion," \
                            + "WindowWrapGraphics,LiveDTV,LearningTables,CommunityTableIndicator,DiamondDisplays," \
                            + "CFixtures,TIOKioskIndicator,ApprovedforFlexBladeIndicator,CapIndexScore," \
                            + "SellingWallNotes,RemodelDate,SpringMarket,SpringRegion,SpringDistrict,DTVNowIndicator," \
                            + "BAEWorkDayId,BSISWorkDayId,SpringRegionVP,SpringMarketDirector,SpringDistrictManager"

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.log.info("prefixPath is " + prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        allValuesDict = {}
        reqValuesDict = {}
        for obj in partitionName:
            allValuesDict[obj.key] = obj.last_modified
        for k, v in allValuesDict.items():
            if 'part-0000' in k:
                reqValuesDict[k] = v
        revSortedFiles = sorted(reqValuesDict, key=reqValuesDict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        storeColumnsWithAlias = ','.join(['a.' + x for x in self.storeColumns.split(',')])
        s3 = boto3.resource('s3')
        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedLocationFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixLocationDiscoveryPath,
                                                            self.discoveryBucket)
        dfLocationMaster = self.sparkSession.read.parquet(lastUpdatedLocationFile)

        lastUpdatedBAEFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixBaeDiscoveryPath,
                                                       self.discoveryBucket)

        dfBAE = self.sparkSession.read.parquet(lastUpdatedBAEFile)

        lastUpdatedDealerFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDealerDiscoveryPath,
                                                          self.discoveryBucket)
        dfDealer = self.sparkSession.read.parquet(lastUpdatedDealerFile).withColumnRenamed("TBLoc", "StoreNo")

        lastUpdatedSpringMobileFile = self.findLastModifiedFile(discoveryBucketNode,
                                                                self.prefixSpringMobileDiscoveryPath,
                                                                self.discoveryBucket)
        dfSpringMobile = self.sparkSession.read.parquet(lastUpdatedSpringMobileFile)

        lastUpdatedMultiTrackerFile = self.findLastModifiedFile(discoveryBucketNode,
                                                                self.prefixMultitrackerDiscoveryPath,
                                                                self.discoveryBucket)
        dfRealEstate = self.sparkSession.read.parquet(lastUpdatedMultiTrackerFile)

        lastUpdatedDTVLocationFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDtvLocationDiscoveryPath,
                                                               self.discoveryBucket)
        dfDTVLocation = self.sparkSession.read.parquet(lastUpdatedDTVLocationFile)

        dfLocationMaster = dfLocationMaster.withColumn('StoreNo', split(col('StoreName'), ' ').getItem(0))

        dfLocationMaster = dfLocationMaster.withColumn('StoreNumber', regexp_replace(col('StoreNo'), '\D', ''))

        self.log.info("Exception Handling of Store Refine starts")

        dfLocationMaster.filter("StoreNumber == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.locationName + '/' + self.storeRefinedName + '/' +
                self.locationName, header=True)

        dfSpringMobile.filter("Store == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.springMobileName + '/' + self.storeRefinedName + '/' +
                self.springMobileName, header=True)
        try:
            dfSpringMobile = dfSpringMobile.withColumn('OpenDate1', dfSpringMobile['OpenDate'].cast(DateType()))
        except AnalysisException:
            self.log.error("SpringMobile OpenDate value is not in correct date format")
            # dfSpringMobile = dfSpringMobile.withColumn('OpenDate1', lit(''))
            dfSpringMobile.coalesce(1).write.mode("append").csv(
                self.dataProcessingErrorPath + '/' + self.springMobileName + '/' + self.storeRefinedName + '/' +
                self.springMobileName, header=True)
            return
        try:
            dfRealEstate = dfRealEstate.withColumn('RemodelorOpenDate1', dfRealEstate['RemodelorOpenDate'].cast(
                DateType()))
        except AnalysisException:
            self.log.error("RealEstate RemodelorOpenDate value is not in correct date format")
            # dfRealEstate = dfRealEstate.withColumn('RemodelorOpenDate1', lit(''))
            dfRealEstate.coalesce(1).write.mode("append").csv(
                self.dataProcessingErrorPath + '/' + self.multiTrackerName + '/' + self.storeRefinedName + '/' +
                self.springMobileName, header=True)
            return

        dfRealEstate.filter("Loc == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.multiTrackerName + '/' + self.storeRefinedName + '/' +
                self.multiTrackerName, header=True)

        dfDealer.filter("StoreNo == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.dealerName + '/' + self.storeRefinedName + '/' +
                self.dealerName, header=True)

        dfDTVLocation.filter("Location == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.dtvLocationName + '/' + self.storeRefinedName + '/' +
                self.dtvLocationName, header=True)

        dfBAE.filter("StoreNo == ''").coalesce(1).write.mode("append").\
            csv(self.dataProcessingErrorPath + '/' + self.baeName + '/' + self.storeRefinedName + '/' +
                self.baeName, header=True)

        self.log.info("Exception Handling of Store Refine Ends")

        dfLocationMaster = dfLocationMaster.filter("StoreNumber != ''").drop_duplicates(subset=['StoreNumber'])
        dfBAE.filter("StoreNo != ''").registerTempTable("BAE")
        dfDTVLocation.filter("Location != ''").registerTempTable("dtvLocation")
        dfDealer.filter("StoreNo != ''").registerTempTable("Dealer")
        dfSpringMobile.coalesce(1).write.mode("overwrite").csv(self.storeCSVPath + '/spring', header=True)
        dfSpringMobile.filter("Store != ''").registerTempTable("SpringMobile")
        dfRealEstate.filter("Loc != ''").withColumn("StoreNo", dfRealEstate["Loc"].cast(IntegerType())).\
            registerTempTable("RealEstate")

        dfLocationMaster = dfLocationMaster.withColumn('Location', regexp_replace(col('StoreName'), '[0-9]', ''))
        dfLocationMaster = dfLocationMaster.withColumn('SaleInvoiceCommentRe',
                                                       split(regexp_replace(
                                                           col('SaleInvoiceComment'), '\n', ' '), 'License #')
                                                       .getItem(0))
        dfLocationMaster = dfLocationMaster.withColumn('CnsmrLicNbr',
                                                       split(regexp_replace(
                                                           col('SaleInvoiceComment'), '\n', ' '), 'License #')
                                                       .getItem(1))

        dfLocationMaster = dfLocationMaster.withColumn('SunTm', regexp_extract(col("GeneralLocationNotes"),
                                                                               self.sundayTimeExp, 0)).\
            withColumn('MonTm', regexp_extract(col("GeneralLocationNotes"), self.mondayTimeExp, 0)).\
            withColumn('TueTm', regexp_extract(col("GeneralLocationNotes"), self.tuedayTimeExp, 0)).\
            withColumn('WedTm', regexp_extract(col("GeneralLocationNotes"), self.weddayTimeExp, 0)).\
            withColumn('ThuTm', regexp_extract(col("GeneralLocationNotes"), self.thudayTimeExp, 0)).\
            withColumn('FriTm', regexp_extract(col("GeneralLocationNotes"), self.fridayTimeExp, 0)).\
            withColumn('SatTm', regexp_extract(col("GeneralLocationNotes"), self.satdayTimeExp, 0))
        dfLocationMaster = dfLocationMaster.\
            withColumn('SunOpenTm', when((col("SunTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("SunTm"), 'U:', ''), '-').getItem(0))).\
            withColumn('SunCloseTm', when((col("SunTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("SunTm"), 'U:', ''), '-').getItem(1))).\
            withColumn('MonOpenTm', when((col("MonTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("MonTm"), 'M:', ''), '-').getItem(0))).\
            withColumn('MonCloseTm', when((col("MonTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("MonTm"), 'M:', ''), '-').getItem(1))).\
            withColumn('TueOpenTm', when((col("TueTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("TueTm"), 'T:', ''), '-').getItem(0))).\
            withColumn('TueCloseTm', when((col("TueTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("TueTm"), 'T:', ''), '-').getItem(1))).\
            withColumn('WedOpenTm', when((col("WedTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("WedTm"), 'W:', ''), '-').getItem(0))).\
            withColumn('WedCloseTm', when((col("WedTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("WedTm"), 'W:', ''), '-').getItem(1))).\
            withColumn('ThuOpenTm', when((col("ThuTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("ThuTm"), 'R:', ''), '-').getItem(0))).\
            withColumn('ThuCloseTm', when((col("ThuTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("ThuTm"), 'R:', ''), '-').getItem(1))).\
            withColumn('FriOpenTm', when((col("FriTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("FriTm"), 'F:', ''), '-').getItem(0))).\
            withColumn('FriCloseTm', when((col("FriTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("FriTm"), 'F:', ''), '-').getItem(1))).\
            withColumn('SatOpenTm', when((col("SatTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("SatTm"), 'S:', ''), '-').getItem(0))).\
            withColumn('SatCloseTm', when((col("SatTm").like('%Closed%')), '00:00 AM').
                       otherwise(split(regexp_replace(col("SatTm"), 'S:', ''), '-').getItem(1)))
        dfLocationMaster.registerTempTable("API")

        self.sparkSession.sql(
            "select a.StoreNumber as StoreNumber "
            ", '4' as CompanyCd"
            ", a.StoreID as SourceStoreIdentifier"
            ", a.Location as LocationName"
            ", a.Abbreviation as Abbreviation"
            ",  a.GLCode as GLCode"
            ", a.Disabled as StoreStatus"
            ", a.ManagerEmployeeID as StoreManagerEmployeeId"
            ", case when lower(a.ManagerCommissionable) = 'true' then '1' when lower(a.ManagerCommissionable) = 'false'"
            " then '0' else ' ' end as ManagerCommisionableIndicator"
            ", case when a.Address = '' then d.StreetAddress when a.Address = '' and d.StreetAddress = '' then "
            "e.Address else a.Address end as Address"
            ", case when a.City is null or a.City = '' then e.City else a.City end as City"
            ", case when a.StateProv is null or a.StateProv = '' then e.State else a.StateProv end as StateProvince"
            ", case when a.ZipPostal is null or a.ZipPostal = '' then e.Zip else a.ZipPostal end as PostalCode"
            ", a.Country as Country"
            ", cast(a.PhoneNumber AS string) as Phone"
            ", cast(a.FaxNumber as string) as Fax"
            ", a.StoreType as StoreType"
            ", a.StaffLevel as StaffLevel "
            ", e.SqFtRange as SquareFootRange "
            ",case when d.SquareFeet is null then a.SquareFootage else d.SquareFeet end as SquareFoot"
            ", cast(a.Latitude as decimal(20,10)) as Lattitude"
            ", cast(a.Longitude as decimal(20,10)) as Longitude"
            ", a.TimeZone as Timezone"
            ", case when lower(a.AdjustDST) = 'true' then '1' when lower(a.AdjustDST) = 'false' then '0' else ' ' end"
            " as AdjustDST"
            ", a.CashPolicy as CashPolicy"
            ", a.MaxCashDrawer as MaxCashDrawer"
            ", a.Serial_on_OE as SerialOnOEIndicator"
            ", a.Phone_on_OE as PhoneOnOEIndicator"
            ", a.PAW_on_OE as PAWOnOEIndicator"
            ", a.Comment_on_OE as CommentOnOEIndicator"
            ", case when lower(a.HideCustomerAddress) = 'true' then '1' when lower(a.HideCustomerAddress) = 'false' "
            "then '0' else ' ' end as HideCustomerAddressIndicator"
            ", a.EmailAddress as EmailAddress"
            ", a.CnsmrLicNbr as ConsumerLicenseNumber"
            ", a.SaleInvoiceCommentRe as SaleInvoiceComment"
            ", a.Taxes as Taxes"
            ", case when d.TotalMonthlyRent is null then a.rent else d.TotalMonthlyRent end as Rent"
            ", a.PropertyTaxes as PropertyTaxes"
            ", a.InsuranceAmount as InsuranceAmount"
            ", a.OtherCharges as OtherCharges"
            ", a.DepositTaken as Deposit"
            ", a.LandlordName as LandlordName"
            ", case when lower(a.UseLocationEmail) = 'true' then '1' when lower(a.UseLocationEmail) = 'false' then '0'"
            " else ' ' end as UseLocationEmailIndicator"
            ", d.StoreType as LocationType"
            ", a.LandlordNotes as LandlordNote"
            ", a.LeaseStartDate as LeaseStartDate"
            ", a.LeaseEndDate as LeaseEndDate"
            ", a.LeaseNotes as LeaseNotes"
            ", e.OpenDate as StoreOpenDate"
            ", e.ClosedDate as StoreCloseDate"
            ", a.RelocationDate as RelocationDate"
            ", e.StoreTier as StoreTier"
            ", a.MonOpenTm as MondayOpenTime"
            ", a.MonCloseTm as MondayCloseTime"
            ", a.TueOpenTm as TuesdayOpenTime"
            ", a.TueCloseTm as TuesdayCloseTime"
            ", a.WedOpenTm as WednesdayOpenTime"
            ", a.WedCloseTm as WednesdayCloseTime"
            ", a.ThuOpenTm as ThursdayOpenTime"
            ", a.ThuCloseTm as ThursdayCloseTime"
            ", a.FriOpenTm as FridayOpenTime"
            ", a.FriCloseTm as FridayCloseTime"
            ", a.SatOpenTm as SaturdayOpenTime"
            ", a.SatCloseTm as SaturdayCloseTime"
            ", a.SunOpenTm as SundayOpenTime"
            ", a.SunCloseTm as SundayCloseTime"
            ", e.AcquisitionName as AcquisitionName"
            ", case when lower(e.Base) = 'x' or lower(e.Base) = 'yes' or lower(e.Base) = 'true' then '1' end as "
            "BaseStoreIndicator"
            ", case when lower(e.Comp) = 'x' or lower(e.Comp) = 'yes' or lower(e.Comp) = 'true' then '1' end as "
            "CompStoreIndicator"
            ", case when lower(e.Same) = 'x' or lower(e.Same) = 'yes' or lower(e.Same) = 'true' then '1' end as "
            "SameStoreIndicator"
            ",' ' as FranchiseStoreIndicator"
            ", d.LeaseExpiration as LeaseExpiration"
            ", d.BuildType as BuildTypeCode"
            ", d.C_Cdesignation as C_CDesignation"
            ", d.AuthorizedRetailerTagLine as AuthorizedRetailedTagLineStatusIndicator"
            ", d.PylonMonumentPanels as PylonMonumentPanels"
            ", d.SellingWalls as SellingWalls "
            ", d.MemorableAccessoryWall as MemorableAccessoryWall"
            ", d.cashwrapexpansion as CashWrapExpansion"
            ", d.WindowWrapGrpahics as WindowWrapGraphics"
            ", d.LiveDTV as LiveDTV"
            ", d.LearningTables as LearningTables"
            ", d.CommunityTable as CommunityTableIndicator"
            ", d.DiamondDisplays as DiamondDisplays"
            ", d.CFixtures as CFixtures"
            ", d.TIOKiosk as TIOKioskIndicator"
            ", d.ApprovedforFlexBlade as ApprovedforFlexBladeIndicator"
            ", d.CapIndexScore as CapIndexScore"
            ", d.SellingWallsNotes as SellingWallNotes"
            ", case when unix_timestamp (d.RemodelorOpenDate1,'MM/dd/yyyy') > unix_timestamp(e.OpenDate1,'MM/dd/yyyy')"
            " then d.RemodelorOpenDate end as RemodelDate"
            ", case when a.ChannelName is null then d.SpringRegion else a.ChannelName end as SpringRegion"
            ", case when a.RegionName is null then d.SpringMarket else a.RegionName end as SpringMarket"
            ", case when a.DistrictName is null then d.SpringDistrict else a.DistrictName end as SpringDistrict"
            ", case when lower(f.DTVNowLocation) = 'x' or lower(f.DTVNowLocation) = 'yes' or lower(f.DTVNowLocation) ="
            " 'true' then '1' end as DTVNowIndicator"
            ", b.BAEWorkdayID as BAEWorkDayId"
            ", b.BSISWorkdayID as BSISWorkDayId"
            ", e.RegionVP as SpringRegionVP"
            ", e.MarketDirector as SpringMarketDirector"
            ", e.DistrictManager as SpringDistrictManager from API a "
            "left outer join BAE b on a.StoreNumber = b.StoreNo "
            "left outer join Dealer c on a.StoreNumber = c.StoreNo "
            "left outer join RealEstate d on a.StoreNumber = d.StoreNo "
            "left outer join SpringMobile e on a.StoreNumber = e.Store "
            "left outer join dtvLocation f on a.Location = f.Location"
            ).registerTempTable("store")

        dfStoreSource = self.sparkSession.sql("select " + self.storeColumns + " from store")
        self.sparkSession.sql("select " + self.storeColumns + " from store ").\
            withColumn("Hash_Column", hash("StoreNumber", "CompanyCd", "SourceStoreIdentifier", "LocationName",
                                           "Abbreviation", "GLCode", "StoreStatus", "StoreManagerEmployeeId",
                                           "ManagerCommisionableIndicator", "Address", "City", "StateProvince",
                                           "PostalCode", "Country", "Phone", "Fax", "StoreType", "StaffLevel",
                                           "SquareFootRange", "SquareFoot", "Lattitude", "Longitude", "Timezone",
                                           "AdjustDST", "CashPolicy", "MaxCashDrawer", "SerialOnOEIndicator",
                                           "PhoneOnOEIndicator", "PAWOnOEIndicator", "CommentOnOEIndicator",
                                           "HideCustomerAddressIndicator", "EmailAddress", "ConsumerLicenseNumber",
                                           "SaleInvoiceComment", "Taxes", "Rent", "PropertyTaxes", "InsuranceAmount",
                                           "OtherCharges", "Deposit", "LandlordName", "UseLocationEmailIndicator",
                                           "LocationType", "LandlordNote", "LeaseStartDate", "LeaseEndDate",
                                           "LeaseNotes", "StoreOpenDate", "StoreCloseDate", "RelocationDate",
                                           "StoreTier", "MondayOpenTime", "MondayCloseTime", "TuesdayOpenTime",
                                           "TuesdayCloseTime", "WednesdayOpenTime", "WednesdayCloseTime",
                                           "ThursdayOpenTime", "ThursdayCloseTime", "FridayOpenTime", "FridayCloseTime",
                                           "SaturdayOpenTime", "SaturdayCloseTime", "SundayOpenTime", "SundayCloseTime",
                                           "AcquisitionName", "BaseStoreIndicator", "CompStoreIndicator",
                                           "SameStoreIndicator", "FranchiseStoreIndicator", "LeaseExpiration",
                                           "BuildTypeCode", "C_CDesignation",
                                           "AuthorizedRetailedTagLineStatusIndicator", "PylonMonumentPanels",
                                           "SellingWalls", "MemorableAccessoryWall",
                                           "CashWrapExpansion", "WindowWrapGraphics", "LiveDTV", "LearningTables",
                                           "CommunityTableIndicator", "DiamondDisplays", "CFixtures",
                                           "TIOKioskIndicator", "ApprovedforFlexBladeIndicator", "CapIndexScore",
                                           "SellingWallNotes", "RemodelDate", "DTVNowIndicator", "BAEWorkDayId",
                                           "BSISWorkDayId", "SpringRegionVP", "SpringMarketDirector",
                                           "SpringDistrictManager")).registerTempTable("store_curr")

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)
        storePrevRefinedPath = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreRefinedPath,
                                                         self.refinedBucket)

        if storePrevRefinedPath != '':
            self.sparkSession.read.parquet(storePrevRefinedPath).\
                withColumn("Hash_Column", hash("StoreNumber", "CompanyCd", "SourceStoreIdentifier", "LocationName",
                                               "Abbreviation", "GLCode", "StoreStatus", "StoreManagerEmployeeId",
                                               "ManagerCommisionableIndicator", "Address", "City", "StateProvince",
                                               "PostalCode", "Country", "Phone", "Fax", "StoreType", "StaffLevel",
                                               "SquareFootRange", "SquareFoot", "Lattitude", "Longitude", "Timezone",
                                               "AdjustDST", "CashPolicy", "MaxCashDrawer", "SerialOnOEIndicator",
                                               "PhoneOnOEIndicator", "PAWOnOEIndicator", "CommentOnOEIndicator",
                                               "HideCustomerAddressIndicator", "EmailAddress", "ConsumerLicenseNumber",
                                               "SaleInvoiceComment", "Taxes", "Rent", "PropertyTaxes",
                                               "InsuranceAmount", "OtherCharges", "Deposit", "LandlordName",
                                               "UseLocationEmailIndicator", "LocationType", "LandlordNote",
                                               "LeaseStartDate", "LeaseEndDate", "LeaseNotes", "StoreOpenDate",
                                               "StoreCloseDate", "RelocationDate", "StoreTier", "MondayOpenTime",
                                               "MondayCloseTime", "TuesdayOpenTime", "TuesdayCloseTime",
                                               "WednesdayOpenTime", "WednesdayCloseTime", "ThursdayOpenTime",
                                               "ThursdayCloseTime", "FridayOpenTime", "FridayCloseTime",
                                               "SaturdayOpenTime", "SaturdayCloseTime", "SundayOpenTime",
                                               "SundayCloseTime", "AcquisitionName", "BaseStoreIndicator",
                                               "CompStoreIndicator", "SameStoreIndicator", "FranchiseStoreIndicator",
                                               "LeaseExpiration", "BuildTypeCode", "C_CDesignation",
                                               "AuthorizedRetailedTagLineStatusIndicator", "PylonMonumentPanels",
                                               "SellingWalls", "MemorableAccessoryWall", "CashWrapExpansion",
                                               "WindowWrapGraphics", "LiveDTV", "LearningTables",
                                               "CommunityTableIndicator", "DiamondDisplays", "CFixtures",
                                               "TIOKioskIndicator", "ApprovedforFlexBladeIndicator", "CapIndexScore",
                                               "SellingWallNotes", "RemodelDate", "DTVNowIndicator", "BAEWorkDayId",
                                               "BSISWorkDayId", "SpringRegionVP", "SpringMarketDirector",
                                               "SpringDistrictManager")).registerTempTable("store_prev")

            self.sparkSession.sql("select " + storeColumnsWithAlias +
                                  " from store_prev a left join store_curr b on a.StoreNumber = "
                                  "b.StoreNumber where a.Hash_Column = b.Hash_Column").\
                registerTempTable("store_no_change_data")

            dfStoreUpdated = self.sparkSession.sql("select " + storeColumnsWithAlias +
                                                   " from store_curr a left join store_prev b on a.StoreNumber = "
                                                   "b.StoreNumber where a.Hash_Column <> b.Hash_Column")
            updateRowsCount = dfStoreUpdated.count()
            dfStoreUpdated.registerTempTable("store_updated_data")

            dfStoreNew = self.sparkSession.sql("select " + storeColumnsWithAlias +
                                               " from store_curr a left join store_prev b on a.StoreNumber = "
                                               "b.StoreNumber where b.StoreNumber is null")
            newRowsCount = dfStoreNew.count()
            dfStoreNew.registerTempTable("store_new_data")

            if updateRowsCount > 0 or newRowsCount > 0:
                dfStoreWithCDC = self.sparkSession.sql("select " + self.storeColumns +
                                                       " from store_no_change_data union all select " +
                                                       self.storeColumns + " from store_updated_data union all select "
                                                       + self.storeColumns + " from store_new_data")
                self.log.info("Updated file has arrived..")
                dfStoreWithCDC.coalesce(1).write.mode("overwrite").parquet(self.storeWorkingPath)
                dfStoreWithCDC.coalesce(1).write.mode("overwrite").csv(self.storeCSVPath, header=True)
                dfStoreWithCDC.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
                    withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
                    write.mode("append").partitionBy('year', 'month').format('parquet').\
                    save(self.storePartitonPath)
            else:
                self.log.info(" The prev and current files are same. So no file will be generated in refined bucket.")
        else:
            self.log.info(" This is the first transaformation call, So keeping the file in refined bucket.")
            dfStoreSource.coalesce(1).write.mode("overwrite").parquet(self.storeWorkingPath)
            dfStoreSource.coalesce(1).write.mode("overwrite").csv(self.storeCSVPath, header=True)
            dfStoreSource.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
                withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
                write.mode('append').partitionBy('year', 'month').format('parquet').\
                save(self.storePartitonPath)
            self.sparkSession.stop()


if __name__ == "__main__":

    DimStoreRefined().loadRefined()