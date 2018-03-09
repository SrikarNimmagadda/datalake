from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from urlparse import urlparse


class DimStoreDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')

        self.refinedBucketWorking = sys.argv[1]
        self.storeCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]

        self.deliveryBucket = self.storeCurrentPath[self.storeCurrentPath.index('tb'):].split("/")[0]
        self.storeDeliveryName = self.storeCurrentPath[self.storeCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.storeCurrentPath[self.storeCurrentPath.index('tb'):].split("/")[2]

        self.prefixAttDealerPath = 'ATTDealerCode'
        self.prefixStoreDealerAssocPartitionPath = 'StoreDealerAssociation'
        self.prefixStorePartitionPath = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]

        self.storePreviousPath = 's3://' + self.deliveryBucket + '/' + self.storeDeliveryName + '/Previous'

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

        self.storeDeliveryColumnSelect = "select a.StoreNumber as store_num ,a.CompanyCd as co_cd ," \
                                         + "a.SourceStoreIdentifier as src_store_id ,a.LocationName as loc_nm ," \
                                         + "a.Abbreviation as abbr ,a.GLCode as gl_cd ,a.StoreStatus as store_stat ," \
                                         + "a.StoreManagerEmployeeId as sm_emp_id ,a.ManagerCommisionableIndicator as" \
                                         + " mgr_cmsnble_ind ,a.Address as addr ,a.City as cty ,a.StateProvince as " \
                                         + "st_prv ,a.PostalCode as pstl_cd ,a.Country as cntry ,a.Phone as ph ,a.Fax" \
                                         + " as fax ,a.StoreType as store_typ ,a.StaffLevel as staff_lvl ," \
                                         + "a.SquareFootRange as s_ft_rng ,a.SquareFoot as sq_ft ,a.Lattitude as " \
                                         + "lattd ,a.Longitude as lngtd ,a.Timezone as tz ,a.AdjustDST as adj_dst_ind" \
                                         + " ,a.EmailAddress as email ,a.ConsumerLicenseNumber as cnsmr_lic_nbr ," \
                                         + "a.Taxes as taxes ,a.Rent as rnt ,a.UseLocationEmailIndicator as " \
                                         + "use_loc_email_ind ,a.LocationType as loc_typ ,a.LandlordNote as lndlrd_nt" \
                                         + " ,a.LeaseStartDate as lease_start_dt ,a.LeaseEndDate as lease_end_dt ," \
                                         + "a.StoreOpenDate as store_open_dt ,a.StoreCloseDate as store_close_dt ," \
                                         + "a.RelocationDate as reloc_dt ,a.StoreTier as store_tier ,a.MondayOpenTime" \
                                         + " as mon_open_tm ,a.MondayCloseTime as mon_close_tm ,a.TuesdayOpenTime as" \
                                         + " tue_open_tm ,a.TuesdayCloseTime as tue_close_tm ,a.WednesdayOpenTime as" \
                                         + " wed_open_tm ,a.WednesdayCloseTime as wed_close_tm ,a.ThursdayOpenTime as" \
                                         + " thu_open_tm ,a.ThursdayCloseTime as thu_close_tm ,a.FridayOpenTime as" \
                                         + " fri_open_tm ,a.FridayCloseTime as fri_close_tm ,a.SaturdayOpenTime as" \
                                         + " sat_open_tm ,a.SaturdayCloseTime as sat_close_tm ,a.SundayOpenTime as" \
                                         + " sun_open_tm ,a.SundayCloseTime as sun_close_tm ,a.AcquisitionName as" \
                                         + " acn_nm ,a.BaseStoreIndicator as base_store_ind ,a.CompStoreIndicator as" \
                                         + " comp_store_ind ,a.SameStoreIndicator as same_store_ind ," \
                                         + "a.FranchiseStoreIndicator as frnchs_store_ind ,a.LeaseExpiration as" \
                                         + " lease_exp ,a.BuildTypeCode as build_type_cd ,a.C_CDesignation as " \
                                         + "c_and_c_dsgn ,a.AuthorizedRetailedTagLineStatusIndicator as" \
                                         + " auth_rtl_tag_line_stat_ind ,a.PylonMonumentPanels as pylon_monmnt_panels" \
                                         + " ,a.SellingWalls as sell_walls ,a.MemorableAccessoryWall as " \
                                         + "memorable_acc_wall ,a.CashWrapExpansion as csh_wrap_expnd ," \
                                         + "a.WindowWrapGraphics as wndw_wrap_grphc ,a.LiveDTV as lve_dtv ," \
                                         + "a.LearningTables as lrn_tbl ,a.CommunityTableIndicator as comnty_tbl_ind" \
                                         + " ,a.DiamondDisplays as dmnd_dsp ,a.CFixtures as c_fixtures ," \
                                         + "a.TIOKioskIndicator as tio_ksk_ind ,a.ApprovedforFlexBladeIndicator as" \
                                         + " aprv_for_flex_bld ,a.CapIndexScore as cap_idx_score ,a.SellingWallNotes" \
                                         + " as sell_wall_nt ,a.RemodelDate as remdl_dt ,a.DTVNowIndicator as " \
                                         + "dtv_now_ind ,a.BAEWorkDayId as bae_wrkday_id ,a.BSISWorkDayId as " \
                                         + "bsis_wrkday_id,a.store_hier_id,a.att_hier_id,a.cdc_ind_cd from store a"

    def makeZeroByteFile(self, destinationPath, fileName):

        newBucketWithPath = urlparse(destinationPath)
        newBucket = newBucketWithPath.netloc
        newBucketNode = self.s3.Bucket(name=newBucket)
        newPath = newBucketWithPath.path.lstrip('/')
        objs = newBucketNode.objects.filter(Prefix=newPath)
        for s3Object in objs:
            s3Object.delete()

        newFile = open(fileName, 'w')
        newFile.close()
        self.client.upload_file(fileName, newBucket, newPath + '/' + fileName)

    def findLastModifiedFile(self, bucketNode, prefixType, bucket, currentOrPrev=1):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
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
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastUpdatedFilePath)

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        storeColumnsWithAlias = ','.join(['a.' + x for x in self.storeColumns.split(',')])

        self.log.info('bucket name: ' + self.refinedBucket)
        refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

        lastUpdatedAttDealerCodeFile = self.findLastModifiedFile(refinedBucketNode, self.prefixAttDealerPath,
                                                                 self.refinedBucket)

        self.sparkSession.read.parquet(lastUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code")

        lastUpdatedstoreDealerCodeAssocFile = self.findLastModifiedFile(refinedBucketNode,
                                                                        self.prefixStoreDealerAssocPartitionPath,
                                                                        self.refinedBucket)

        self.sparkSession.read.parquet(lastUpdatedstoreDealerCodeAssocFile).registerTempTable("store_dealer_code_assoc")

        StoreRefineCurrFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStorePartitionPath,
                                                        self.refinedBucket)
        StoreRefinePrevFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStorePartitionPath,
                                                        self.refinedBucket, 0)

        if StoreRefinePrevFile != '':

            self.log.info('current and previous refined data files are found. So processing for delivery layer starts.')
            dfStoreCurr = self.sparkSession.read.parquet(StoreRefineCurrFile)
            dfStorePrev = self.sparkSession.read.parquet(StoreRefinePrevFile)
            dfStoreCurr.subtract(dfStorePrev).registerTempTable("store_delta")
            dfStorePrev.registerTempTable("store_prev")
            dfStoreNew = self.sparkSession.sql(
                "select " + storeColumnsWithAlias + ",'I' as cdc_ind_cd from store_delta a left join store_prev b on"
                                                    " a.StoreNumber = b.StoreNumber where b.StoreNumber is null")
            dfStoreUpdate = self.sparkSession.sql(
                "select " + storeColumnsWithAlias + ",'C' as cdc_ind_cd from store_delta a left join store_prev b on "
                                                    "a.StoreNumber = b.StoreNumber where b.StoreNumber is not null")

            if dfStoreNew.count() > 0 or dfStoreUpdate.count() > 0:
                dfStoreDelta = dfStoreNew.unionAll(dfStoreUpdate)
                self.log.info("Delta file generated....")
                dfStoreDelta.registerTempTable("store_delta")

                self.sparkSession.sql("select " + storeColumnsWithAlias +
                                      ", concat(a.SpringMarket,a.SpringRegion,a.SpringDistrict) as store_hier_id, "
                                      "concat(c.ATTRegion,c.ATTMarket) as att_hier_id, a.cdc_ind_cd from store_delta"
                                      " a inner join store_dealer_code_assoc b on a.storeNumber = b.StoreNumber and"
                                      " b.AssociationType = 'Retail' and b.AssociationStatus = 'Active' inner join "
                                      "att_dealer_code c on b.DealerCode = c.DealerCode").dropDuplicates(
                    subset=['StoreNumber']).registerTempTable("store")
                dfStoreDelta = self.sparkSession.sql(self.storeDeliveryColumnSelect)
                dfStoreDelta.coalesce(1).write.mode("overwrite").csv(self.storeCurrentPath, header=True)
                dfStoreDelta.coalesce(1).write.mode("append").csv(self.storePreviousPath, header=True)
            else:
                self.makeZeroByteFile(self.storeCurrentPath, 'wt_store.csv')
                self.log.info("The prev and current files same.So zero size delta file generated in delivery bucket.")
        else:
            self.log.info(" This is the first transaformation call, So keeping the file in delivery bucket.")
            self.sparkSession.read.parquet(StoreRefineCurrFile).registerTempTable("store_curr")
            self.sparkSession.sql(
                "select " + storeColumnsWithAlias + ", concat(a.SpringMarket,a.SpringRegion,a.SpringDistrict) as"
                                                    " store_hier_id, concat(c.ATTRegion,c.ATTMarket) as att_hier_id,"
                                                    "'I' as cdc_ind_cd from store_curr a inner join "
                                                    "store_dealer_code_assoc b on a.storeNumber = b.StoreNumber and "
                                                    "b.AssociationType = 'Retail' and b.AssociationStatus = 'Active' "
                                                    "inner join att_dealer_code c on b.DealerCode = c.DealerCode")\
                .registerTempTable("store")

            newRow = self.sparkSession.createDataFrame([[0, 4, 0, 'General Location', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', 'I'],
                                                        [-2, 4, -2, 'Unknown', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                         '', '', '', '', 'I']
                                                        ])
            dfStoreCurr = self.sparkSession.sql(self.storeDeliveryColumnSelect).union(newRow)
            dfStoreCurr.coalesce(1).write.mode("overwrite").csv(self.storeCurrentPath, header=True)
            dfStoreCurr.coalesce(1).write.mode("append").csv(self.storePreviousPath, header=True)
            self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreDelivery().loadDelivery()
