from pyspark.sql import SparkSession
import sys, boto3, datetime


class DimStoreDelivery:

    def __init__(self):

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb-us'):]

        self.prefixAttDealerPath = 'ATTDealerCode/year='
        self.prefixStoreDealerAssocPartitionPath = 'StoreDealerAssociation/year='
        self.prefixStorePartitionPath = 'Store/year='

        self.storeCurrentPath = 's3://' + self.deliveryBucket + '/WT_STORE/Current'
        self.storePreviousPath = 's3://' + self.deliveryBucket + '/WT_STORE/Previous'
        self.storeColumns = "store_num,co_cd,src_store_id,loc_nm,abbr,gl_cd,store_stat,sm_emp_id,mgr_cmsnble_ind," \
                            "addr,cty,st_prv,pstl_cd,cntry,ph,fax,store_typ,staff_lvl,s_ft_rng,sq_ft,lattd,lngtd,tz," \
                            "adj_dst_ind,email,cnsmr_lic_nbr,taxes,rnt,use_loc_email_ind,loc_typ,lndlrd_nt," \
                            "lease_start_dt,lease_end_dt,store_open_dt,store_close_dt,reloc_dt,store_tier," \
                            "mon_open_tm,mon_close_tm,tue_open_tm,tue_close_tm,wed_open_tm,wed_close_tm,thu_open_tm," \
                            "thu_close_tm,fri_open_tm,fri_close_tm,sat_open_tm,sat_close_tm,sun_open_tm,sun_close_tm," \
                            "acn_nm,base_store_ind,comp_store_ind,same_store_ind,frnchs_store_ind,lease_exp," \
                            "build_type_cd,c_and_c_dsgn,auth_rtl_tag_line_stat_ind,pylon_monmnt_panels,sell_walls," \
                            "memorable_acc_wall,csh_wrap_expnd,wndw_wrap_grphc,lve_dtv,lrn_tbl,comnty_tbl_ind," \
                            "dmnd_dsp,c_fixtures,tio_ksk_ind,aprv_for_flex_bld,cap_idx_score,sell_wall_nt,remdl_dt," \
                            "dtv_now_ind,bae_wrkday_id,bsis_wrkday_id,store_hier_id,att_hier_id"
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

        self.storeColumnsWithoutHierId = "a.store_num,a.co_cd,a.src_store_id,a.loc_nm,a.abbr,a.gl_cd,a.store_stat," \
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
                                     + "a.bae_wrkday_id,a.bsis_wrkday_id"

    def findLastModifiedFile(self, bucketNode, prefixPath, bucket, currentOrPrev=1):
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
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            print("Last Modified file in s3 format is : ", lastUpdatedFilePath)

        elif numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + self.bucket + "/" + secondLastModifiedFileName
            print("Last Modified file in s3 format is : ", lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        spark = SparkSession.builder.appName("DimStoreDelivery").getOrCreate()

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        attDealerCodePrefixPath = self.prefixAttDealerPath + datetime.now().strftime('%Y')
        lastUpdatedAttDealerCodeFile = self.findLastModifiedFile(refinedBucketNode, attDealerCodePrefixPath,
                                                                 self.refinedBucket)

        spark.read.parquet(lastUpdatedAttDealerCodeFile).registerTempTable("att_dealer_code")

        storeDealerCodeAssocPrefixPath = self.prefixStoreDealerAssocPartitionPath + datetime.now().strftime('%Y')
        lastUpdatedstoreDealerCodeAssocFile = self.findLastModifiedFile(refinedBucketNode,
                                                                        storeDealerCodeAssocPrefixPath,
                                                                        self.refinedBucket)

        spark.read.parquet(lastUpdatedstoreDealerCodeAssocFile).registerTempTable("store_dealer_code_assoc")

        storePrefixPath = self.prefixStorePartitionPath + datetime.now().strftime('%Y')
        lastUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, storePrefixPath, self.refinedBucket)
        lastPrevUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, storePrefixPath, self.refinedBucket, 0)

        if lastUpdatedStoreFile != '':

            spark.read.parquet(lastUpdatedStoreFile).withColumn("Hash_Column", hash(self.storeColumns)).registerTempTable(
                "store_curr1")
            spark.sql(
                "select " + self.storeColumnsWithoutHierId + ",a.Hash_Column"
                + ", concat(a.spring_market,a.spring_region,a.spring_district) as store_hier_id"
                + ", concat(c.ATTRegion,c.ATTMarket) as att_hier_id"
                + ", a.cdc_ind_cd from store_curr1 a inner join store_dealer_code_assoc b on"
                + " a.store_num = b.StoreNumber and b.AssociationType = 'Retail' and"
                + " b.AssociationStatus = 'Active' inner join att_dealer_code c on"
                + " b.DealerCode = c.DealerCode").dropDuplicates(subset=['store_num']).registerTempTable("store_curr")

        if lastPrevUpdatedStoreFile != '':

            spark.read.parquet(lastPrevUpdatedStoreFile).withColumn("Hash_Column",
                                                                    hash(self.storeColumns)).registerTempTable(
                "store_prev1")
            spark.sql(
                " select " + self.storeColumnsWithoutHierId + ",a.Hash_Column"
                + ", concat(a.spring_market,a.spring_region,a.spring_district) as store_hier_id"
                + ", concat(c.ATTRegion,c.ATTMarket) as att_hier_id"
                + ", a.cdc_ind_cd from store_prev1 a inner join store_dealer_code_assoc b on"
                + " a.store_num = b.StoreNumber and b.AssociationType = 'Retail' and"
                + " b.AssociationStatus = 'Active' inner join att_dealer_code c on"
                + " b.DealerCode = c.DealerCode").dropDuplicates(subset=['store_num']).registerTempTable("store_prev")
        else:
            print("There are no file, hence not computing the latest file name")

        if lastUpdatedStoreFile != '' and lastPrevUpdatedStoreFile != '':
            print('current and previous refined data files are found. So processing for delivery layer starts.')

            dfStoreNoChange = spark.sql(
                "select " + self.storeColumnsWithAlias +
                " from store_prev a left join store_curr b on a.store_num = b.store_num where "
                + "a.Hash_Column = b.Hash_Column").registerTempTable("store_no_change_data")

            dfStoreUpdated = spark.sql(
                "select " + self.storeColumnsWithAlias +
                " from store_curr a left join store_prev b on a.store_num = b.store_num where "
                + "a.Hash_Column <> b.Hash_Column")

            rowCountUpdateRecords = dfStoreUpdated.count()

            dfStoreUpdated = dfStoreUpdated.registerTempTable("store_updated_data")

            dfStoreNew = spark.sql(
                "select " + self.storeColumnsWithAlias +
                " from store_curr a left join store_prev b on a.store_num = b.store_num where b.store_num = null")

            rowCountNewRecords = dfStoreNew.count()

            dfStoreNew = dfStoreNew.registerTempTable("store_new_data")

            dfStoreDelta = spark.sql(
                "select " + self.storeColumns + " from store_updated_data union all select " + self.storeColumns
                + " from store_new_data")

            print('Updated store_num are')
            dfStoreUpdatedPrint = spark.sql("select store_num from store_updated_data")
            print(dfStoreUpdatedPrint.show())
            print('New added store_num are')
            dfStoreNewPrint = spark.sql("select store_num from store_new_data")
            print(dfStoreNewPrint.show())

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                print("Updated file has arrived..")
                dfStoreDelta.coalesce(1).write.mode("overwrite").csv(self.storeCurrentPath, header=True)
                dfStoreDelta.coalesce(1).write.mode("append").csv(self.storePreviousPath, header=True)
            else:
                print(" The prev and current files are same. So no delta file will be generated in refined bucket.")
        elif lastUpdatedStoreFile != '' and lastPrevUpdatedStoreFile == '':

            print(" This is the first transaformation call, So keeping the file in delivery bucket.")
            dfStoreCurr = spark.sql("select " + self.storeColumns + " from store_curr")
            dfStoreCurr.coalesce(1).write.mode("overwrite").csv(self.storeCurrentPath, header=True)
            dfStoreCurr.coalesce(1).write.mode("append").csv(self.storePreviousPath, header=True)
        else:
            print("This should not be printed. Please check.")
        spark.stop()


if __name__ == "__main__":
    DimStoreDelivery().loadDelivery()
