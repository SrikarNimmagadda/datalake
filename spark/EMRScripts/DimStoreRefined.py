from pyspark.sql import SparkSession
from pyspark.sql.functions import split,udf,regexp_extract,regexp_replace,col
import sys
import re
from datetime import datetime
from pyspark.sql.types import *


class DimStoreRefined:

    def __init__(self):
        self.locationMasterList = sys.argv[1]
        self.baeLocation = sys.argv[2]
        self.dealerCodes = sys.argv[3]
        self.springMobileStoreList = sys.argv[4]
        self.multiTracker = sys.argv[5]
        self.storeRefinedPath = sys.argv[6] + '/' + datetime.now().strftime('%Y/%m') + '/' + 'StoreRefined' + '/' + sys.argv[
            7]
        print('Refined path 351: ',self.storeRefinedPath)
        self.sundayTimeExp = "(([U]: Closed) |([U]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.mondayTimeExp = "(([M]: Closed) |([M]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.tuedayTimeExp = "(([T]: Closed) |([T]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.weddayTimeExp = "(([W]: Closed) |([W]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.thudayTimeExp = "(([R]: Closed) |([R]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.fridayTimeExp = "(([F]: Closed) |([F]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.satdayTimeExp = "(([S]: Closed) |([S]: (1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])-(1[0-2]|0?[1-9]):([0-5][0-9]) ([AP][Mm])))"
        self.openTimeExp = "(1[0-2]|0?[1-9]):([0-5][0-9]) AM"
        self.closeTimeExp = "(1[0-2]|0?[1-9]):([0-5][0-9]) PM"

    def find_time(self,dayTime, time):
        if 'Closed' not in dayTime:
            str = dayTime[2:].split('-')
            if time == 'A':
                return str[0].strip()
            elif time == 'P':
                return str[1].strip()
        else:
            return "00:00 AM"

    def loadRefined(self):
        spark = SparkSession.builder.appName("DimStoreRefined").getOrCreate()
        dfLocationMaster = spark.read.parquet(self.locationMasterList)
        spark.read.parquet(self.baeLocation).registerTempTable("BAE")
        spark.read.parquet(self.dealerCodes).withColumnRenamed("TBLoc", "StoreNo").registerTempTable("Dealer")

        dfSpringMobile = spark.read.parquet(self.springMobileStoreList)
        dfSpringMobile.withColumn("StoreNo", dfSpringMobile["Store"].cast(IntegerType())).registerTempTable("SpringMobile")

        spark.read.parquet(self.multiTracker).registerTempTable("RealEstate")

        dfLocationMaster = dfLocationMaster.withColumn('StoreNo', regexp_replace(col('StoreName'), '\D',''))
        dfLocationMaster = dfLocationMaster.withColumn('Location', regexp_replace(col('StoreName'), '[0-9]',''))
        dfLocationMaster = dfLocationMaster.withColumn('CnsmrLicNbr', split(col('SaleInvoiceComment'), '#').getItem(1))

        dfLocationMaster = dfLocationMaster.withColumn('SunTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.sundayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('SunOpenTm',
                                                       regexp_extract(col("SunTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('SunCloseTm',
                                                       regexp_extract(col("SunTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('MonTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.mondayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('MonOpenTm',
                                                       regexp_extract(col("MonTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('MonCloseTm',
                                                       regexp_extract(col("MonTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('TueTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.tuedayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('TueOpenTm',
                                                       regexp_extract(col("TueTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('TueCloseTm',
                                                       regexp_extract(col("TueTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('WedTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.weddayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('WedOpenTm',
                                                       regexp_extract(col("WedTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('WedCloseTm',
                                                       regexp_extract(col("WedTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('ThuTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.thudayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('ThuOpenTm',
                                                       regexp_extract(col("ThuTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('ThuCloseTm',
                                                       regexp_extract(col("ThuTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('FriTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.fridayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('FriOpenTm',
                                                       regexp_extract(col("FriTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('FriCloseTm',
                                                       regexp_extract(col("FriTm"), self.closeTimeExp, 0))

        dfLocationMaster = dfLocationMaster.withColumn('SatTm',
                                                       regexp_extract(col("GeneralLocationNotes"), self.satdayTimeExp,
                                                                      0))
        dfLocationMaster = dfLocationMaster.withColumn('SatOpenTm',
                                                       regexp_extract(col("SatTm"), self.openTimeExp, 0))
        dfLocationMaster = dfLocationMaster.withColumn('SatCloseTm',
                                                       regexp_extract(col("SatTm"), self.closeTimeExp, 0))
        dfLocationMaster.registerTempTable("API")
        # dfStore = spark.sql(
        #     "select a.StoreNo as store_num, '4' as co_cd, a.StoreID as src_store_id, a.Location as loc_name, a.Abbreviation as abbr,  a.GLCode as gl_cd, a.Disabled as store_stat, a.ManagerEmployeeID as sm_emp_id, case when a.ManagerCommissionable = 'TRUE' or a.ManagerCommissionable = 'true' then '1' when a.ManagerCommissionable = 'FALSE' or a.ManagerCommissionable = 'false' then '0' else ' ' end as mgr_cmsnble_ind, d.StreetAddress as addr, e.City as cty, e.State as st_prv, e.Zip as pstl_cd, a.Country as cntry, a.PhoneNumber as ph, a.FaxNumber as fax, a.StoreType as store_typ, a.StaffLevel as staff_lvl, e.SqFtRange as s_ft_rng, d.SquareFeet as sq_ft, a.Latitude as lattd,a.Longitude as lngtd, a.TimeZone as tz, a.AdjustDST as adj_dst_ind, a.EmailAddress as email, a.ConsumerLicenseNumber as cnsmr_lic_nbr, a.Taxes as taxes, d.TotalMonthlyRent as rnt, a.UseLocationEmail as use_loc_email_ind, d.StoreType as loc_typ, a.LandlordNotes as lndlrd_nt, a.LeaseStartDate as lease_start_dt, a.LeaseEndDate as lease_end_dt, c.OpenDate as store_open_dt, c.CloseDate as store_close_dt,  a.RelocationDate as reloc_dt, e.StoreTier as store_tier, case when a.MonTm = 'M: Closed' then '00:00 AM' when a.MonTm = '' then 'null' end as mon_open_tm, case when a.MonTm = 'M: Closed' then '00:00 AM' when a.MonTm = '' then 'null' end as mon_close_tm, case when a.TueTm = 'T: Closed' then '00:00 AM' when a.TueTm = '' then 'null' end as tue_open_tm, case when a.TueTm = 'T: Closed' then '00:00 AM' when a.TueTm = '' then 'null' end as tue_close_tm, case when a.WedTm = 'W: Closed' then '00:00 AM' when a.WedTm = '' then 'null' end as wed_open_tm, case when a.WedTm = 'W: Closed' then '00:00 AM' when a.WedTm = '' then 'null' end as wed_close_tm, case when a.ThuTm = 'R: Closed' then '00:00 AM' when a.ThuTm = '' then 'null' end as thu_open_tm, case when a.ThuTm = 'R: Closed' then '00:00 AM' when a.ThuTm = '' then 'null' end as thu_close_tm, case when a.FriTm = 'F: Closed' then '00:00 AM' when a.FriTm = '' then 'null' end as fri_open_tm, case when a.FriTm = 'F: Closed' then '00:00 AM' when a.FriTm = '' then 'null' end as fri_close_tm, case when a.SatTm = 'S: Closed' then '00:00 AM' when a.SatTm = '' then 'null' end as sat_open_tm, case when a.SatTm = 'S: Closed' then '00:00 AM' when a.SatTm = '' then 'null' end as sat_close_tm, case when a.SunTm = 'U: Closed' then '00:00 AM' when a.SunTm = '' then 'null' end as sun_open_tm, case when a.SunTm = 'U: Closed' then '00:00 AM' when a.SunTm = '' then 'null' end as sun_close_tm, e.AcquisitionName as acn_nm, case when e.Base = 'x' then '1' else '0' end as base_store_ind, case when e.Comp = 'x' then '1' else '0' end as comp_store_ind, case when e.Same = 'x' then '1' else '0' end as same_store_ind, ' ' as frnchs_store_ind, d.LeaseExpiration as lease_exp, d.BuildType as build_type_cd, d.`C&Cdesignation` as c_and_c_dsgn, d.AuthorizedRetailerTagLine as auth_rtl_tag_line_stat_ind, d.Pylon_MonumentPanels as pylon_monmnt_panels, d.SellingWalls as sell_walls, d.MemorableAccessoryWall as memorable_acc_wall, d.cashwrapexpansion as csh_wrap_expnd, d.WindowWrapGrpahics as wndw_wrap_grphc, d.LiveDTV as lve_dtv, d.LearningTables as lrn_tbl, d.CommunityTable as comnty_tbl_ind, d.DiamondDisplays as dmnd_dsp, d.CFixtures as c_fixtures, d.TIOKiosk as tio_ksk_ind, d.ApprovedforFlexBlade as aprv_for_flex_bld, d.CapIndexScore as cap_idx_score, d.SellingWallsNotes as sell_wall_nt, d.RemodelorOpenDate as remdl_dt, ' ' as dtv_now_ind, b.BAEWorkdayID as bae_wrkday_id, b.BSISWorkdayID as bsis_wrkday_id, e.storeTier as store_hier_id, ' ' as cdc_ind_cd, ' ' as edw_batch_id, ' ' as edw_create_dttm, ' ' as edw_update_dttm"
        #     +" from API a left outer join BAE b on a.StoreNo = b.StoreNo left outer join Dealer c on a.StoreNo = c.StoreNo left outer join RealEstate d on a.StoreNo = d.Loc left outer join SpringMobile e on a.StoreNo = e.StoreNo and a.City = e.City").registerTempTable(
        #     "store")

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
                            + ", a.PhoneNumber as ph"
                            + ", a.FaxNumber as fax"
                            + ", a.StoreType as store_typ"
                            + ", a.StaffLevel as staff_lvl"
                            + ", e.SqFtRange as s_ft_rng"
                            + ", case when d.SquareFeet is null then a.SquareFootage else d.SquareFeet end as sq_ft"
                            + ", a.Latitude as lattd"
                            + ", a.Longitude as lngtd"
                            + ", a.TimeZone as tz"
                            + ", case when a.AdjustDST = 'TRUE' or a.AdjustDST = 'true' then '1' when a.AdjustDST = 'FALSE' or a.AdjustDST = 'false' then '0' else ' ' end as adj_dst_ind"
                            + ", a.EmailAddress as email"
                            + ", a.CnsmrLicNbr as cnsmr_lic_nbr"
                            + ", a.Taxes as taxes"
                            + ", case when d.TotalMonthlyRent is null then a.rent else d.TotalMonthlyRent end as rnt"
                            + ", a.UseLocationEmail as use_loc_email_ind"
                            + ", d.StoreType as loc_typ"
                            + ", a.LandlordNotes as lndlrd_nt"
                            + ", a.LeaseStartDate as lease_start_dt"
                            + ", a.LeaseEndDate as lease_end_dt"
                            + ", c.OpenDate as store_open_dt"
                            + ", c.CloseDate as store_close_dt"
                            + ", a.RelocationDate as reloc_dt"
                            + ", e.StoreTier as store_tier"
                            + ", case when a.MonTm = 'M: Closed' then '00:00 AM' when a.MonTm = '' then 'null' else a.MonOpenTm end as mon_open_tm"
                            + ", case when a.MonTm = 'M: Closed' then '00:00 AM' when a.MonTm = '' then 'null' else a.MonCloseTm end as mon_close_tm"
                            + ", case when a.TueTm = 'T: Closed' then '00:00 AM' when a.TueTm = '' then 'null' else a.TueOpenTm end as tue_open_tm"
                            + ", case when a.TueTm = 'T: Closed' then '00:00 AM' when a.TueTm = '' then 'null' else a.TueCloseTm end as tue_close_tm"
                            + ", case when a.WedTm = 'W: Closed' then '00:00 AM' when a.WedTm = '' then 'null' else a.WedOpenTm end as wed_open_tm"
                            + ", case when a.WedTm = 'W: Closed' then '00:00 AM' when a.WedTm = '' then 'null' else a.WedCloseTm end as wed_close_tm"
                            + ", case when a.ThuTm = 'R: Closed' then '00:00 AM' when a.ThuTm = '' then 'null' else a.ThuOpenTm end as thu_open_tm"
                            + ", case when a.ThuTm = 'R: Closed' then '00:00 AM' when a.ThuTm = '' then 'null' else a.ThuCloseTm end as thu_close_tm"
                            + ", case when a.FriTm = 'F: Closed' then '00:00 AM' when a.FriTm = '' then 'null' else a.FriOpenTm end as fri_open_tm"
                            + ", case when a.FriTm = 'F: Closed' then '00:00 AM' when a.FriTm = '' then 'null' else a.FriCloseTm end as fri_close_tm"
                            + ", case when a.SatTm = 'S: Closed' then '00:00 AM' when a.SatTm = '' then 'null' else a.SatOpenTm end as sat_open_tm"
                            + ", case when a.SatTm = 'S: Closed' then '00:00 AM' when a.SatTm = '' then 'null' else a.SatCloseTm end as sat_close_tm"
                            + ", case when a.SunTm = 'U: Closed' then '00:00 AM' when a.SunTm = '' then 'null' else a.SunOpenTm end as sun_open_tm"
                            + ", case when a.SunTm = 'U: Closed' then '00:00 AM' when a.SunTm = '' then 'null' else a.SunCloseTm end as sun_close_tm"
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
                            + ", d.RemodelorOpenDate as remdl_dt"
                            + ", ' ' as dtv_now_ind"
                            + ", b.BAEWorkdayID as bae_wrkday_id"
                            + ", b.BSISWorkdayID as bsis_wrkday_id"
                            + ", e.storeTier as store_hier_id"
                            + ", e.storeTier as att_hier_id"
                            + ", ' ' as cdc_ind_cd"
                            + ", ' ' as edw_batch_id"
                            + ", ' ' as edw_create_dttm"
                            + ", ' ' as edw_update_dttm "
                            + "from API a left outer join BAE b on a.StoreNo = b.StoreNo "
                            + "left outer join Dealer c on a.StoreNo = c.StoreNo left outer join RealEstate d "
                            + "on a.StoreNo = d.Loc left outer join SpringMobile e on a.StoreNo = e.StoreNo and "
                            + "a.City = e.City").dropDuplicates(subset=['store_num']).registerTempTable("store")

        dfStoreWithMaxStoreStatus = spark.sql("select a.* from store a ")

        #dfStoreWithMaxStoreStatus = spark.sql(
        #    "select a.* from store a INNER JOIN (select store_num, max(store_stat) as value from store group by store_num) as b on a.store_num=b.store_num and a.store_stat=b.store_stat")

        dfStoreWithMaxStoreStatus.coalesce(1).write.mode("overwrite").parquet(self.storeRefinedPath)
        spark.stop()
        #df_location_master_csv = spark.read.parquet('s3n://tb-us-east-1-dev-refined-regular/Store/2018/01/StoreRefined/201801151258')
        #df_location_master_csv.coalesce(1).select("*").write.csv(self.storeRefinedPath)

if __name__ == "__main__":
    DimStoreRefined().loadRefined()