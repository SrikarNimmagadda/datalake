from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *

LocationMasterList = sys.argv[1]
BAELocation = sys.argv[2]
DealerCodes = sys.argv[3]
SpringMobileStoreList = sys.argv[4]
MultiTracker = sys.argv[5]
StoreTable = sys.argv[6]
StoreFileTime = sys.argv[7]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("LocationStore").getOrCreate()
        
        
        
#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################


dfLocationMaster = spark.read.parquet(LocationMasterList)
        
#dfLocationMaster = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/2017/09/location201709281007/")
		
dfBAE = spark.read.parquet(BAELocation)

#dfBAE = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/2017/09/BAE201709271440/")
		
        
dfBAE = dfBAE.withColumnRenamed("Store Number", "StoreNo").registerTempTable("BAE")
        

dfDealer = spark.read.parquet(DealerCodes)

#dfDealer = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/2017/09/Dealer201709291421/")
        
dfDealer = dfDealer.withColumnRenamed("TBLoc", "StoreNo").registerTempTable("Dealer")


dfMultiTracker = spark.read.parquet(MultiTracker).registerTempTable("RealEstate")     

#dfMultiTracker = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/2017/09/multiTracker201709291421/").registerTempTable("RealEstate") 
                      
dfSpringMobile = spark.read.parquet(SpringMobileStoreList)

#dfSpringMobile = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/2017/09/springMobile201709291421/")

dfSpringMobile = dfSpringMobile.withColumn("StoreNo", dfSpringMobile["StoreNo"].cast(IntegerType())).registerTempTable("SpringMobile")
		
        

#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

split_col = split(dfLocationMaster['StoreName'], ' ')

dfLocationMaster = dfLocationMaster.withColumn('StoreNo', split_col.getItem(0))
dfLocationMaster = dfLocationMaster.withColumn('Location', split_col.getItem(1))
dfLocationMaster.registerTempTable("LocMasterPostAppend")


finalLocationMaster_DF = spark.sql("select StoreID, StoreName, StoreNo, Location, Disabled, Abbreviation, ManagerEmployeeID, "
                    + "ManagerCommissionable, Address, City, StateProv, ZipPostal, Country, PhoneNumber, FaxNumber, "
                    + "DistrictName, RegionName, ChannelName, StoreType, GLCode, SquareFootage, LocationCode, "
                    + "Latitude, Longitude, AddressVerified, TimeZone, AdjustDST, CashPolicy, MaxCashDrawer, Serial_on_OE, "
                    + "Phone_on_OE, PAW_on_OE, Comment_on_OE, HideCustomerAddress, EmailAddress, GeneralLocationNotes, "
                    + "case when SaleInvoiceComment like 'Spring%' then SaleInvoiceComment else ' ' end as ConsumerLicenseNumber, "
                    + "case when SaleInvoiceComment not like 'Spring%' then SaleInvoiceComment else ' ' end as SaleInvoiceComment, "
                    + "StaffLevel, BankDetails, Taxes, LeaseStartDate, LeaseEndDate, Rent, PropertyTaxes, "
                    + "InsuranceAmount, OtherCharges, DepositTaken, InsuranceCompany, LandlordNotes, LandlordName, RelocationDate, "
                    + "UseLocationEmail, LocationEntityID, DateLastStatusChanged from LocMasterPostAppend  where GeneralLocationNotes != ' '")
					
#Testing					
#finalLocationMaster_DF = spark.sql("select GeneralLocationNotes from LocMasterPostAppend where GeneralLocationNotes != ' '")
					
					
split_col = split(finalLocationMaster_DF['GeneralLocationNotes'], ',')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('mon_to_fri', split_col.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('sat', split_col.getItem(1))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('sun', split_col.getItem(2))


split_col2 = split(finalLocationMaster_DF['mon_to_fri'], ' ')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('mon_to_fri2', split_col2.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('time_of_mon_fri', split_col2.getItem(1))


split_col3 = split(finalLocationMaster_DF['mon_to_fri2'], '-')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('mon_block', split_col3.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('fri_block', split_col3.getItem(1))
					
split_col4 = split(finalLocationMaster_DF['time_of_mon_fri'], '-')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekday_start_time', split_col4.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekday_end_time', split_col4.getItem(1))					


split_col5 = split(finalLocationMaster_DF['sat'], ' ')

#finalLocationMaster_DF = finalLocationMaster_DF.withColumn('empty', split_col5.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_day', split_col5.getItem(1))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_time', split_col5.getItem(2))					


split_col6 = split(finalLocationMaster_DF['weekend_time'], '-')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_start_time', split_col6.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_end_time', split_col6.getItem(1))


split_col7 = split(finalLocationMaster_DF['sun'], ' ')

#finalLocationMaster_DF = finalLocationMaster_DF.withColumn('empty2', split_col7.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_day2', split_col7.getItem(1))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_time2', split_col7.getItem(2))					


split_col8 = split(finalLocationMaster_DF['weekend_time2'], '-')

finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_start_time2', split_col8.getItem(0))
finalLocationMaster_DF = finalLocationMaster_DF.withColumn('weekend_end_time2', split_col8.getItem(1))

finalLocationMaster_DF = finalLocationMaster_DF.drop("mon_to_fri", "sat", "sun", "empty", "mon_to_fri2", "time_of_mon_fri", "empty2", "weekend_time2", "weekend_time")

finalLocationMaster_DF.registerTempTable("API")
					

joined_DF = spark.sql("select '' as report_date,a.StoreID as storeid, '4' as companycd, ' ' as source_identifier, a.StoreNo as storenumber, a.Location as locationname, " #d.FormulaLink as locationname, "
                    + "a.Disabled as storestatus, a.Abbreviation as abbreviation, a.ManagerEmployeeID as storemanageremployeeid, "
                    + "a.ManagerCommissionable as managercommissionable, d.StreetAddress as address, e.City as city, e.State as stateprovince, e.Zip as zippostal, "
					+ "a.Country as country, a.PhoneNumber as phone, a.FaxNumber as fax, a.StaffLevel as stafflevel, a.RelocationDate as relocationdate, "
                    + "d.District as district, d.Region as region, d.SpringMarket as market, a.StoreType as storetype, a.GLCode as glcode, d.SquareFeet as squarefoot, a.LocationCode as locationcode, e.AcquisitionName as acquisitionname, "
                    + "a.Latitude as latitude, a.Longitude as longitude, a.LeaseStartDate as leasestartdate, a.LeaseEndDate as leaseenddate, "
					+ "case when a.AddressVerified = 'Verified' or a.AddressVerified = 'verified' then '1' else '0' end as addressverficationstatus, "
					+ "a.TimeZone as timezone, a.AdjustDST as adjustdst, a.CashPolicy as cashpolicy, a.MaxCashDrawer as maxcashdrawer, a.Serial_on_OE as serialonoe, "
                    + "a.Phone_on_OE as phoneonoe, a.PAW_on_OE as pawonoe, a.Comment_on_OE as commentonoe, a.HideCustomerAddress as hidecustomeraddress, a.EmailAddress as emailaddress, a.Taxes as taxes, " #a.GeneralLocationNotes as generallocationnotes, "
                    + "d.TotalMonthlyRent as rent, a.PropertyTaxes as propertytaxes, a.InsuranceAmount as insuranceamount, a.InsuranceCompany as insurancecompanyname, "
					+ "a.OtherCharges as othercharges, a.DepositTaken as deposit, a.LandlordName as landlordname, d.`C&Cdesignation` as c_n_cdesignation, "
                    + "a.UseLocationEmail as uselocationemail, a.LocationEntityID as locationentityid, d.StoreType as locationtype, a.DateLastStatusChanged as datelaststatuschanged, a.LandlordNotes as landlordnotes, a.ConsumerLicenseNumber as consumerlicensenumber, "
                    + "a.SaleInvoiceComment as saleinvoicecomment, ' ' as notes, ' ' as notes2, c.OpenDate as opendate, c.CloseDate as closedate, "
                    + "e.Classification as classification, e.StoreTier as storetier, "
                    + "case when a.mon_block = 'Mon' then a.weekday_start_time else ' ' end as monday_open_time,  "
                    + "case when a.mon_block = 'Mon' then a.weekday_end_time else ' ' end as monday_close_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_start_time else ' ' end as tuesday_open_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_end_time else ' ' end as tuesday_close_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_start_time else ' ' end as wednesday_open_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_end_time else ' ' end as wednesday_close_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_start_time else ' ' end as thursday_open_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' or a.fri_block = 'Thu' then a.weekday_end_time else ' ' end as thursday_close_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' then a.weekday_start_time else ' ' end as friday_open_time, "
                    + "case when a.fri_block = 'Fri' or a.fri_block = 'Sat' then a.weekday_end_time else ' ' end as friday_close_time, "
                    + "case when a.fri_block = 'Sat' then a.weekday_start_time when a.weekend_day = 'Sat' then a.weekend_start_time else ' ' end as saturday_open_time, "
                    + "case when a.fri_block = 'Sat' then a.weekday_end_time when a.weekend_day = 'Sat' and a.weekend_start_time != 'Closed' then a.weekend_end_time else a.weekend_start_time end as saturday_close_time, "
                    + "case when a.weekend_day = 'Sun' then a.weekday_start_time when a.weekend_day2 = 'Sun' then a.weekend_start_time2 else ' ' end as sunday_open_time, "
                    + "case when a.weekend_day = 'Sun' then a.weekday_end_time when a.weekend_day2 = 'Sun' and a.weekend_start_time2 != 'Closed' then a.weekend_end_time2 else a.weekend_start_time2 end as sunday_close_time, "
					+ "' ' as storeclosed, ' ' as franchisestore, e.SqFtRange as squarefootrange, 'TestString' as attribute, 'BaseStore' as basestore, "
                    + "'Comp Store' as compstore, 'Same Store' as samestore, d.LeaseExpiration as leaseexpiration, d.BuildType as buildtype, d.RemodelorOpenDate as remodeldate, "
                    + "d.AuthorizedRetailerTagLine as authorizedretailertagline, d.`Pylon/MonumentPanels` as pylonormonumentpanels, e.DistrictManager as springdistrictmanager, "
                    + "d.SellingWalls as sellingwalls, d.MemorableAccessoryWall as memorableaccessorywall, d.cashwrapexpansion, d.WindowWrapGrpahics as windowwrapgrpahics, d.LiveDTV as dtvnowindicator, d.LearningTables as learningtables, e.MarketVP as springmarketvp, e.RegionDirector as springregiondirector, "
                    + "d.CommunityTable as communitytable, d.DiamondDisplays as diamonddisplays, d.`_C_Fixtures` as c_fixtures, d.TIOKiosk as tickiosk, d.ApprovedforFlexBlade as approvedforflexblade, d.CapIndexScore as capindexscore, d.SellingWallsNotes as sellingwallsnotes, b.BAEWorkdayID as baeworkdayid, b.BSISWorkdayID as bsisworkdayid "
                    + "from API a "
                    + "left outer join BAE b "
                    + "on a.StoreNo = b.StoreNo "
                    + "left outer join Dealer c "
                    + "on a.StoreNo = c.StoreNo "
                    + "left outer join RealEstate d "
                    + "on a.StoreNo = d.Loc "
		            + "left outer join SpringMobile e "
		            + "on a.StoreNo = e.StoreNo and a.City = e.City").registerTempTable("store")

#joined_DF1 = spark.sql("select * from store a where a.StoreNumber!='215' and a.Disabled!=1")
joined_DF1= spark.sql("select a.* from store a INNER JOIN (select storenumber, max(storestatus) as value from store group by storenumber) as b on a.storenumber=b.storenumber and a.storestatus=b.value");


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

#joined_DF1.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(StoreTable)
        
joined_DF1.coalesce(1).write.mode("overwrite").parquet(StoreTable + '/' + todayyear + '/' + todaymonth + '/' + 'StoreRefined' + StoreFileTime)


spark.stop()