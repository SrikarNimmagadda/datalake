from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql.functions import regexp_replace, col, when, hash
from pyspark.sql.types import *

import sys, boto3
from datetime import datetime


class DimStoreRefined:

    def __init__(self):

        self.discoveryBucketWithS3 = sys.argv[1]
        self.discoveryBucket = self.discoveryBucketWithS3[self.discoveryBucketWithS3.index('tb-us'):]
        self.refinedBucketWithS3 = sys.argv[2]
        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.storeWorkingPath = 's3://' + self.refinedBucket + '/Store/Working'
        self.storePartitonPath = 's3://' + self.refinedBucket + '/Store'
        self.prefixLocationPath = 'store/location'
        self.prefixBaePath = 'store/bae'
        self.prefixdealerPath = 'store/dealer'
        self.prefixSpringMobilePath = 'store/spring-mobile'
        self.prefixMultitrackerPath = 'store/multi-tracker'
        self.prefixDtvLocationPath = 'store/dtv-location'
        self.prefixStorePath = 'store'

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

        self.storeColumns = "store_num,co_cd,src_store_id,loc_nm,abbr,gl_cd,store_stat,sm_emp_id,mgr_cmsnble_ind," \
                            + "addr,cty,st_prv,pstl_cd,cntry,ph,fax,store_typ,staff_lvl,s_ft_rng,sq_ft,lattd,lngtd,tz" \
                            + ",adj_dst_ind,email,cnsmr_lic_nbr,taxes,rnt,use_loc_email_ind,loc_typ,lndlrd_nt," \
                            + "lease_start_dt,lease_end_dt,store_open_dt,store_close_dt,reloc_dt,store_tier," \
                            + "mon_open_tm,mon_close_tm,tue_open_tm,tue_close_tm,wed_open_tm,wed_close_tm,thu_open_tm" \
                            + ",thu_close_tm,fri_open_tm,fri_close_tm,sat_open_tm,sat_close_tm,sun_open_tm," \
                            + "sun_close_tm,acn_nm,base_store_ind,comp_store_ind,same_store_ind,frnchs_store_ind," \
                            + "lease_exp,build_type_cd,c_and_c_dsgn,auth_rtl_tag_line_stat_ind,pylon_monmnt_panels," \
                            + "sell_walls,memorable_acc_wall,csh_wrap_expnd,wndw_wrap_grphc,lve_dtv,lrn_tbl," \
                            + "comnty_tbl_ind,dmnd_dsp,c_fixtures,tio_ksk_ind,aprv_for_flex_bld,cap_idx_score," \
                            + "sell_wall_nt,remdl_dt,dtv_now_ind,bae_wrkday_id,bsis_wrkday_id,store_hier_id,att_hier_id"
        self.storeColumnsWithAlias = "a.store_num,a.co_cd,a.src_store_id,a.loc_nm,a.abbr,a.gl_cd,a.store_stat," \
                                     + "a.sm_emp_id,a.mgr_cmsnble_ind,a.addr,a.cty,a.st_prv,a.pstl_cd,a.cntry,a.ph," \
                                     + "a.fax,a.store_typ,a.staff_lvl,a.s_ft_rng,a.sq_ft,a.lattd,a.lngtd,a.tz," \
                                     + "a.adj_dst_ind,a.email,a.cnsmr_lic_nbr,a.taxes,a.rnt,a.use_loc_email_ind," \
                                     + "a.loc_typ,a.lndlrd_nt,a.lease_start_dt,a.lease_end_dt,a.store_open_dt," \
                                     + "a.store_close_dt,a.reloc_dt,a.store_tier,a.mon_open_tm,a.mon_close_tm," \
                                     + "a.tue_open_tm,a.tue_close_tm,a.wed_open_tm,a.wed_close_tm,a.thu_open_tm," \
                                     + "a.thu_close_tm,a.fri_open_tm,a.fri_close_tm,a.sat_open_tm,a.sat_close_tm," \
                                     + "a.sun_open_tm,a.sun_close_tm,a.acn_nm,a.base_store_ind,a.comp_store_ind," \
                                     + "a.same_store_ind,a.frnchs_store_ind,a.lease_exp,a.build_type_cd," \
                                     + "a.c_and_c_dsgn,a.auth_rtl_tag_line_stat_ind,a.pylon_monmnt_panels," \
                                     + "a.sell_walls,a.memorable_acc_wall,a.csh_wrap_expnd,a.wndw_wrap_grphc," \
                                     + "a.lve_dtv,a.lrn_tbl,a.comnty_tbl_ind,a.dmnd_dsp,a.c_fixtures,a.tio_ksk_ind," \
                                     + "a.aprv_for_flex_bld,a.cap_idx_score,a.sell_wall_nt,a.remdl_dt,a.dtv_now_ind," \
                                     + "a.bae_wrkday_id,a.bsis_wrkday_id,a.store_hier_id,a.att_hier_id"

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        print("prefixPath is ", prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        all_values_dict = {}
        req_values_dict = {}
        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified
        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v
        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        print("Number of part files is : ", numFiles)
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            print("Last Modified ", prefixType, " file in s3 format is : ", lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        spark = SparkSession.builder.appName("DimStoreRefined").getOrCreate()

        s3 = boto3.resource('s3')
        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedLocationFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixLocationPath,
                                                            self.discoveryBucket)
        dfLocationMaster = spark.read.parquet(lastUpdatedLocationFile)

        lastUpdatedBAEFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixBaePath, self.discoveryBucket)
        spark.read.parquet(lastUpdatedBAEFile).registerTempTable("BAE")

        lastUpdatedDealerFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDealerPath,
                                                          self.discoveryBucket)
        spark.read.parquet(lastUpdatedDealerFile).withColumnRenamed("TBLoc", "StoreNo").registerTempTable("Dealer")

        lastUpdatedSpringMobileFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixSpringMobilePath,
                                                                self.discoveryBucket)
        dfSpringMobile = spark.read.parquet(lastUpdatedSpringMobileFile)
        dfSpringMobile.withColumn("StoreNo", dfSpringMobile["Store"].cast(IntegerType())).\
            registerTempTable("SpringMobile")

        lastUpdatedMultiTrackerFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixMultitrackerPath,
                                                                self.discoveryBucket)
        spark.read.parquet(lastUpdatedMultiTrackerFile).registerTempTable("RealEstate")

        lastUpdatedDTVLocationFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDtvLocationPath,
                                                               self.discoveryBucket)
        spark.read.parquet(lastUpdatedDTVLocationFile).registerTempTable("dtvLocation")
        dfLocationMaster = dfLocationMaster.withColumn('StoreNo', regexp_replace(col('StoreName'), '\D', ''))

        dfLocationMaster = dfLocationMaster.filter("StoreNo != ''")

        dfLocationMaster = dfLocationMaster.withColumn('Location', regexp_replace(col('StoreName'), '[0-9]', ''))
        dfLocationMaster = dfLocationMaster.withColumn('SaleInvoiceCommentRe',
                                                       split(regexp_replace(
                                                           col('SaleInvoiceComment'), '\n', ' '), 'License #').getItem(0))
        dfLocationMaster = dfLocationMaster.withColumn('CnsmrLicNbr',
                                                       split(regexp_replace(
                                                           col('SaleInvoiceComment'), '\n', ' '), 'License #').getItem(1))

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
                                                       when((col("MonTm").like('%Closed%')), '00:00 AM').
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
                            + ", case when a.ManagerCommissionable = 'TRUE' or a.ManagerCommissionable = 'true' "
                            + "then '1' when a.ManagerCommissionable = 'FALSE' or a.ManagerCommissionable = 'false' "
                            + "then '0' else ' ' end as mgr_cmsnble_ind"
                            + ", case when a.Address is null or a.Address = '' then d.StreetAddress "
                            + "else a.Address end as addr"
                            + ", case when a.City is null or a.City = '' then e.City else a.City end as cty"
                            + ", case when a.StateProv is null or a.StateProv = '' then e.State else a.StateProv "
                            + "end as st_prv"
                            + ", case when a.ZipPostal is null or a.ZipPostal = '' then e.Zip else a.ZipPostal "
                            + "end as pstl_cd"
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
                            + ", case when a.AdjustDST = 'TRUE' or a.AdjustDST = 'true' then '1' "
                            + "when a.AdjustDST = 'FALSE' or a.AdjustDST = 'false' then '0' else ' ' end as adj_dst_ind"
                            + ", a.CashPolicy as cash_policy"
                            + ", a.MaxCashDrawer as max_cash_drawer"
                            + ", a.Serial_on_OE as Serial_on_OE"
                            + ", a.Phone_on_OE as Phone_on_OE"
                            + ", a.PAW_on_OE as PAW_on_OE"
                            + ", a.Comment_on_OE as Comment_on_OE"
                            + ", case when a.HideCustomerAddress = 'TRUE' or a.HideCustomerAddress = 'true' then '1' "
                            + "when a.HideCustomerAddress = 'FALSE' or a.HideCustomerAddress = 'false' then '0' "
                            + "else ' ' end as hide_customer_address"
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
                            + ", case when a.UseLocationEmail = 'TRUE' or a.UseLocationEmail = 'true' then '1' "
                            + "when a.UseLocationEmail = 'FALSE' or a.UseLocationEmail = 'false' then '0' else ' ' "
                            + "end as use_loc_email_ind"
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
                            + ", case when unix_timestamp(d.RemodelorOpenDate,'MM/dd/yyyy') > "
                            + "unix_timestamp(e.OpenDate,'MM/dd/yyyy') then d.RemodelorOpenDate end as remdl_dt"
                            + ", case when a.ChannelName is null then d.SpringMarket else a.ChannelName "
                            + "end as spring_market"
                            + ", case when a.RegionName is null then d.Region else a.RegionName end as spring_region"
                            + ", case when a.DistrictName is null then d.District else a.DistrictName "
                            + "end as spring_district"
                            + ", case when f.DTVNowLocation = 'x' then '0' else '1' end as dtv_now_ind"
                            + ", b.BAEWorkdayID as bae_wrkday_id"
                            + ", b.BSISWorkdayID as bsis_wrkday_id"
                            + ", e.storeTier as store_hier_id"
                            + ", e.storeTier as att_hier_id"
                            + ", 'I' as cdc_ind_cd "
                            + ", YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, "
                            + "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
                            + " from API a left outer join BAE b on a.StoreNo = b.StoreNo"
                            + " left outer join Dealer c on a.StoreNo = c.StoreNo left outer join RealEstate d"
                            + " on a.StoreNo = d.Loc left outer join SpringMobile e on a.StoreNo = e.StoreNo and"
                            + " a.City = e.City left outer join dtvLocation f on a.StoreName = f.Location").\
            dropDuplicates(subset=['store_num']).registerTempTable("store")

        dfStoreSource = spark.sql("select " + self.storeColumns+" from store")
        dfStoreCurr = spark.sql("select a.* from store a").\
            withColumn("Hash_Column", hash(self.storeColumns)).registerTempTable("store_curr")

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)
        storePrevRefinedPath = self.findLastModifiedFile(refinedBucketNode, self.prefixStorePath, self.refinedBucket)

        if storePrevRefinedPath != '':
            spark.read.parquet(storePrevRefinedPath).withColumn("Hash_Column",
                                                                hash(self.storeColumns)).registerTempTable(
                "store_prev")

            dfStoreNoChange = spark.sql("select " + self.storeColumnsWithAlias +
                                        " from store_prev a left join store_curr b on a.store_num = b.store_num "
                                        + "where a.Hash_Column = b.Hash_Column").\
                registerTempTable("store_no_change_data")

            dfStoreUpdated = spark.sql("select " + self.storeColumnsWithAlias +
                                       " from store_curr a left join store_prev b on a.store_num = b.store_num "
                                       + "where a.Hash_Column <> b.Hash_Column")
            updateRowsCount = dfStoreUpdated.count()
            dfStoreUpdated = dfStoreUpdated.registerTempTable("store_updated_data")

            dfStoreNew = spark.sql("select " + self.storeColumnsWithAlias +
                                   " from store_curr a left join store_prev b on a.store_num = b.store_num "
                                   + "where b.store_num = null")
            newRowsCount = dfStoreNew.count()
            dfStoreNew = dfStoreNew.registerTempTable("store_new_data")

            # print to be deleted cc
            dfUpdatedCount = spark.sql("select a.* from store_updated_data a")
            dfNewCount = spark.sql("select a.* from store_new_data a")
            print('Updated rows are')
            dfStoreUpdatedPrint = spark.sql("select store_num from store_updated_data")
            print(dfStoreUpdatedPrint.show())
            print('New added rows are')
            dfStoreNewPrint = spark.sql("select store_num from store_new_data")
            print(dfStoreNewPrint.show())
            #######################################

            if updateRowsCount > 0 or newRowsCount > 0:
                print("Updated file has arrived..")
                dfStoreSource.coalesce(1).write.mode("overwrite").parquet(self.storeWorkingPath)
                dfStoreSource.coalesce(1).write.mode("append").partitionBy('year', 'month').format('parquet').\
                    save(self.storePartitonPath)
            else:
                print(" The prev and current files are same. So no file will be generated in refined bucket.")
        else:
            print(" This is the first transaformation call, So keeping the file in refined bucket.")
            dfStoreSource.coalesce(1).write.mode("overwrite").parquet(self.storeWorkingPath)
            dfStoreSource.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').\
                save(self.storePartitonPath)
        spark.stop()


if __name__ == "__main__":

    DimStoreRefined().loadRefined()
