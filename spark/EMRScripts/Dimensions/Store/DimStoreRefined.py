from pyspark.sql import SparkSession
from pyspark.sql.functions import split,regexp_extract,regexp_replace,col,when
import sys
from datetime import datetime
from pyspark.sql.types import *


class DimStoreRefined:

    def __init__(self):
        self.locationMasterList = sys.argv[1]
        self.baeLocation = sys.argv[2]
        self.dealerCodes = sys.argv[3]
        self.springMobileStoreList = sys.argv[4]
        self.multiTracker = sys.argv[5]
        self.dtvLocation = sys.argv[6]
        self.storeRefinedPath = sys.argv[7] + '/' + datetime.now().strftime('%Y/%m') + '/' + 'StoreRefined' + '/' + sys.argv[8]
        print('Refined path 351: ',self.storeRefinedPath)
        self.sundayTimeExp = "(([U]: Closed)|([U]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.mondayTimeExp = "(([M]: Closed)|([M]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.tuedayTimeExp = "(([T]: Closed)|([T]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.weddayTimeExp = "(([W]: Closed)|([W]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.thudayTimeExp = "(([R]: Closed)|([R]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.fridayTimeExp = "(([F]: Closed)|([F]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.satdayTimeExp = "(([S]: Closed)|([S]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"

    def loadRefined(self):
        spark = SparkSession.builder.appName("DimStoreRefined").getOrCreate()
        dfLocationMaster = spark.read.parquet(self.locationMasterList)
        spark.read.parquet(self.baeLocation).registerTempTable("BAE")
        spark.read.parquet(self.dealerCodes).withColumnRenamed("TBLoc", "StoreNo").registerTempTable("Dealer")

        dfSpringMobile = spark.read.parquet(self.springMobileStoreList)
        dfSpringMobile.withColumn("StoreNo", dfSpringMobile["Store"].cast(IntegerType())).registerTempTable("SpringMobile")

        spark.read.parquet(self.multiTracker).registerTempTable("RealEstate")
        spark.read.parquet(self.dtvLocation).registerTempTable("dtvLocation")
        dfLocationMaster = dfLocationMaster.withColumn('StoreNo', regexp_replace(col('StoreName'), '\D',''))

        dfLocationMaster = dfLocationMaster.filter("StoreNo != ''")

        dfLocationMaster = dfLocationMaster.withColumn('Location', regexp_replace(col('StoreName'), '[0-9]',''))
        dfLocationMaster = dfLocationMaster.withColumn('SaleInvoiceCommentRe', split(regexp_replace(col('SaleInvoiceComment'),'\n',' '), 'License #').getItem(0))
        dfLocationMaster = dfLocationMaster.withColumn('CnsmrLicNbr', split(regexp_replace(col('SaleInvoiceComment'),'\n',' '), 'License #').getItem(1))

        dfLocationMaster = dfLocationMaster.withColumn('SunTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.sundayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('SunOpenTm',
                                                       when((col("SunTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("SunTm"), 'U:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('SunCloseTm',
                                                       when((col("SunTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("SunTm"), 'U:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('MonTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.mondayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('MonOpenTm',
                                                       when((col("MonTm").like('%Closed%')),'00:00 AM').
                                                       otherwise(split(regexp_replace(col("MonTm"), 'M:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('MonCloseTm',
                                                       when((col("MonTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("MonTm"), 'M:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('TueTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.tuedayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('TueOpenTm',
                                                       when((col("TueTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("TueTm"), 'T:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('TueCloseTm',
                                                       when((col("TueTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("TueTm"), 'T:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('WedTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.weddayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('WedOpenTm',
                                                       when((col("WedTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("WedTm"), 'W:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('WedCloseTm',
                                                       when((col("WedTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("WedTm"), 'W:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('ThuTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.thudayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('ThuOpenTm',
                                                       when((col("ThuTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("ThuTm"), 'R:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('ThuCloseTm',
                                                       when((col("ThuTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("ThuTm"), 'R:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('FriTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.fridayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('FriOpenTm',
                                                       when((col("FriTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("FriTm"), 'F:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('FriCloseTm',
                                                       when((col("FriTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("FriTm"), 'F:', ''), '-').
                                                                 getItem(1)))

        dfLocationMaster = dfLocationMaster.withColumn('SatTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.satdayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('SatOpenTm',
                                                       when((col("SatTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("SatTm"), 'S:', ''), '-').
                                                                 getItem(0)))
        dfLocationMaster = dfLocationMaster.withColumn('SatCloseTm',
                                                       when((col("SatTm").like('%Closed%')), '00:00 AM').
                                                       otherwise(split(regexp_replace(col("SatTm"), 'S:', ''), '-').
                                                                 getItem(1)))
        dfLocationMaster.registerTempTable("API")

        dfStore = spark.sql("select a.StoreNo as store_num "
                            + ", '4' as co_cd"
                            + ", a.StoreID as src_store_id"
                            + ", a.Location as loc_nm"
                            + ", a.Abbreviation as abbr"
                            + ",  a.GLCode as gl_cd"
                            + ", a.Disabled as store_stat"
                            + ", a.ManagerEmployeeID as sm_emp_id"
                            + ", case when a.ManagerCommissionable = 'TRUE' or a.ManagerCommissionable = 'true' then '1' when a.ManagerCommissionable = 'FALSE' or a.ManagerCommissionable = 'false' then '0' else ' ' end as mgr_cmsnble_ind"
                            + ", case when a.Address is null or a.Address = '' then d.StreetAddress else a.Address end as addr"
                            + ", case when a.City is null or a.City = '' then e.City else a.City end as cty"
                            + ", case when a.StateProv is null or a.StateProv = '' then e.State else a.StateProv end as st_prv"
                            + ", case when a.ZipPostal is null or a.ZipPostal = '' then e.Zip else a.ZipPostal end as pstl_cd"
                            + ", a.Country as cntry"
                            + ", cast(a.PhoneNumber AS string) as ph"
                            + ", cast(a.FaxNumber as string) as fax"
                            + ", a.StoreType as store_typ"
                            + ", a.StaffLevel as staff_lvl"
                            + ", e.SqFtRange as s_ft_rng"
                            + ", case when d.SquareFeet is null then a.SquareFootage else d.SquareFeet end as sq_ft"
                            + ", cast(a.Latitude as decimal(20,10)) as lattd"
                            + ", cast(a.Longitude as decimal(20,10)) as lngtd"
                            + ", a.TimeZone as tz"
                            + ", case when a.AdjustDST = 'TRUE' or a.AdjustDST = 'true' then '1' when a.AdjustDST = 'FALSE' or a.AdjustDST = 'false' then '0' else ' ' end as adj_dst_ind"
                            + ", a.CashPolicy as cash_policy"
                            + ", a.MaxCashDrawer as max_cash_drawer"
                            + ", a.Serial_on_OE as Serial_on_OE"
                            + ", a.Phone_on_OE as Phone_on_OE"
                            + ", a.PAW_on_OE as PAW_on_OE"
                            + ", a.Comment_on_OE as Comment_on_OE"
                            + ", case when a.HideCustomerAddress = 'TRUE' or a.HideCustomerAddress = 'true' then '1' when a.HideCustomerAddress = 'FALSE' or a.HideCustomerAddress = 'false' then '0' else ' ' end as hide_customer_address"
                            + ", a.EmailAddress as email"
                            + ", a.CnsmrLicNbr as cnsmr_lic_nbr"
                            + ", a.SaleInvoiceCommentRe as sale_invoice_comment"
                            + ", a.Taxes as taxes"
                            + ", case when d.TotalMonthlyRent is null then a.rent else d.TotalMonthlyRent end as rnt"
                            + ", a.PropertyTaxes as property_taxes"
                            + ", a.InsuranceAmount as insurance_amount"
                            + ", a.OtherCharges as other_charges"
                            + ", a.DepositTaken as deposit"
                            + ", a.LandlordName as landlord_name"
                            + ", case when a.UseLocationEmail = 'TRUE' or a.UseLocationEmail = 'true' then '1' when a.UseLocationEmail = 'FALSE' or a.UseLocationEmail = 'false' then '0' else ' ' end as use_loc_email_ind"
                            + ", a.StoreType as loc_typ"
                            + ", a.LandlordNotes as lndlrd_nt"
                            + ", a.LeaseStartDate as lease_start_dt"
                            + ", a.LeaseEndDate as lease_end_dt"
                            + ", a.LeaseNotes as lease_notes"
                            + ", c.OpenDate as store_open_dt"
                            + ", c.CloseDate as store_close_dt"
                            + ", a.RelocationDate as reloc_dt"
                            + ", e.StoreTier as store_tier"
                            + ", a.MonOpenTm as mon_open_tm"
                            + ", a.MonCloseTm as mon_close_tm"
                            + ", a.TueOpenTm as tue_open_tm"
                            + ", a.TueCloseTm as tue_close_tm"
                            + ", a.WedOpenTm as wed_open_tm"
                            + ", a.WedCloseTm as wed_close_tm"
                            + ", a.ThuOpenTm as thu_open_tm"
                            + ", a.ThuCloseTm as thu_close_tm"
                            + ", a.FriOpenTm as fri_open_tm"
                            + ", a.FriCloseTm as fri_close_tm"
                            + ", a.SatOpenTm as sat_open_tm"
                            + ", a.SatCloseTm as sat_close_tm"
                            + ", a.SunOpenTm as sun_open_tm"
                            + ", a.SunCloseTm as sun_close_tm"
                            + ", e.AcquisitionName as acn_nm"
                            + ", case when e.Base = 'x' then '1' else '0' end as base_store_ind"
                            + ", case when e.Comp = 'x' then '1' else '0' end as comp_store_ind"
                            + ", case when e.Same = 'x' then '1' else '0' end as same_store_ind"
                            + ", ' ' as frnchs_store_ind"
                            + ", d.LeaseExpiration as lease_exp"
                            + ", d.BuildType as build_type_cd"
                            + ", d.`C&Cdesignation` as c_and_c_dsgn"
                            + ", d.AuthorizedRetailerTagLine as auth_rtl_tag_line_stat_ind"
                            + ", d.PylonMonumentPanels as pylon_monmnt_panels"
                            + ", d.SellingWalls as sell_walls"
                            + ", d.MemorableAccessoryWall as memorable_acc_wall"
                            + ", d.cashwrapexpansion as csh_wrap_expnd"
                            + ", d.WindowWrapGrpahics as wndw_wrap_grphc"
                            + ", d.LiveDTV as lve_dtv"
                            + ", d.LearningTables as lrn_tbl"
                            + ", d.CommunityTable as comnty_tbl_ind"
                            + ", d.DiamondDisplays as dmnd_dsp"
                            + ", d.CFixtures as c_fixtures"
                            + ", d.TIOKiosk as tio_ksk_ind"
                            + ", d.ApprovedforFlexBlade as aprv_for_flex_bld"
                            + ", d.CapIndexScore as cap_idx_score"
                            + ", d.SellingWallsNotes as sell_wall_nt"
                            + ", case when unix_timestamp(d.RemodelorOpenDate,'MM/dd/yyyy') > unix_timestamp(e.OpenDate,'MM/dd/yyyy') then d.RemodelorOpenDate end as remdl_dt"
                            + ", case when a.ChannelName is null then d.SpringMarket else a.ChannelName end as spring_market"
                            + ", case when a.RegionName is null then d.Region else a.RegionName end as spring_region"
                            + ", case when a.DistrictName is null then d.District else a.DistrictName end as spring_district"
                            + ", case when f.DTVNowLocation = 'x' then '0' else '1' end as dtv_now_ind"
                            + ", b.BAEWorkdayID as bae_wrkday_id"
                            + ", b.BSISWorkdayID as bsis_wrkday_id"
                            + ", e.storeTier as store_hier_id"
                            + ", e.storeTier as att_hier_id"
                            + ", 'I' as cdc_ind_cd "
                            + "from API a left outer join BAE b on a.StoreNo = b.StoreNo "
                            + "left outer join Dealer c on a.StoreNo = c.StoreNo left outer join RealEstate d "
                            + "on a.StoreNo = d.Loc left outer join SpringMobile e on a.StoreNo = e.StoreNo and "
                            + "a.City = e.City left outer join dtvLocation f on a.StoreName = f.Location").dropDuplicates(subset=['store_num']).registerTempTable("store")

        dfStoreWithMaxStoreStatus = spark.sql("select a.* from store a ")

        dfStoreWithMaxStoreStatus.coalesce(1).write.mode("overwrite").parquet(self.storeRefinedPath)
        spark.stop()

if __name__ == "__main__":
    DimStoreRefined().loadRefined()