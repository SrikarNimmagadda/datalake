from pyspark.sql import SparkSession
from pyspark.sql.functions import concat
import sys


class DimStoreDelivery:

    def __init__(self):
        self.dimStoreRefined = sys.argv[1]
        self.dimAttDealerCodeRefined = sys.argv[2]
        self.dimAttDealerCodeAssocRefined = sys.argv[3]
        self.storeDeliveryOutput = sys.argv[4]

    def loadDelivery(self):

        spark = SparkSession.builder.appName("DimStoreDelivery").getOrCreate()

        spark.read.parquet(self.dimAttDealerCodeRefined).registerTempTable("att_dealer_code")

        spark.read.parquet(self.dimAttDealerCodeAssocRefined).registerTempTable("store_dealer_code_assoc")

        spark.read.parquet(self.dimStoreRefined).registerTempTable("store_refine")



        dfStoreDelivery = spark.sql("select a.store_num ,a.co_cd ,a.src_store_id ,a.loc_nm ,a.abbr ,a.gl_cd ,a.store_stat,a.sm_emp_id ,a.mgr_cmsnble_ind ,a.addr ,a.cty ,a.st_prv ,a.pstl_cd ,a.cntry ,a.ph ,a.fax ,a.store_typ ,a.staff_lvl ,a.s_ft_rng ,a.sq_ft ,a.lattd ,a.lngtd ,a.tz ,a.adj_dst_ind ,a.email ,a.cnsmr_lic_nbr ,a.taxes ,a.rnt ,a.use_loc_email_ind ,a.loc_typ ,a.lndlrd_nt ,a.lease_start_dt ,a.lease_end_dt ,a.store_open_dt ,a.store_close_dt ,a. reloc_dt ,a.store_tier ,a.mon_open_tm ,a.mon_close_tm ,a.tue_open_tm ,a.tue_close_tm ,a.wed_open_tm ,a.wed_close_tm ,a.thu_open_tm ,a.thu_close_tm ,a.fri_open_tm ,a.fri_close_tm ,a.sat_open_tm ,a.sat_close_tm ,a.sun_open_tm ,a.sun_close_tm ,a.acn_nm ,a.base_store_ind ,a.comp_store_ind ,a.same_store_ind ,a.frnchs_store_ind ,a.lease_exp ,a.build_type_cd ,a.c_and_c_dsgn ,a.auth_rtl_tag_line_stat_ind ,a.pylon_monmnt_panels ,a.sell_walls ,a.memorable_acc_wall ,a.csh_wrap_expnd ,a.wndw_wrap_grphc ,a.lve_dtv ,a.lrn_tbl ,a.comnty_tbl_ind ,a.dmnd_dsp ,a.c_fixtures ,a.tio_ksk_ind ,a.aprv_for_flex_bld ,a.cap_idx_score ,a.sell_wall_nt ,a.remdl_dt ,a.dtv_now_ind ,a.bae_wrkday_id ,a.bsis_wrkday_id"
                                    + ", concat(a.spring_market,a.spring_region,a.spring_district) as store_hier_id"
                                    + ", concat(c.ATTRegion,c.ATTMarket) as att_hier_id"
                                    + ", a.cdc_ind_cd from store_refine a inner join store_dealer_code_assoc b on"
                                    + " a.store_num = b.StoreNumber and b.AssociationType = 'Retail' and"
                                    + " b.AssociationStatus = 'Active' inner join att_dealer_code c on"
                                    + " b.DealerCode = c.DealerCode").dropDuplicates(subset=['store_num'])
        dfStoreDelivery.coalesce(1).select("*").write.mode("overwrite").csv(self.storeDeliveryOutput,header=True);

        spark.stop()

if __name__ == "__main__":
    DimStoreDelivery().loadDelivery()