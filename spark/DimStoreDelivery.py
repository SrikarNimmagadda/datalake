from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *

DimATTDealerCode = sys.argv[1]
DimStoreDealerAssociation = sys.argv[2]
DimStoreRefined = sys.argv[3]
DimTechBrandHierarchy = sys.argv[4]
StoreDeliveryOutput = sys.argv[5]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("LocationStore").getOrCreate()
        

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

DimATTDealerCode_DF = spark.read.parquet(DimATTDealerCode).registerTempTable("DimATTDealerCodeTT")

#DimATTDealerCode_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/ATTDealerCodeRefine201711150708/").registerTempTable("DimATTDealerCodeTT")

DimStoreDealerAssociation_DF = spark.read.parquet(DimStoreDealerAssociation).registerTempTable("DimStoreDealerAssociationTT")

#DimStoreDealerAssociation_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/StoreDealerAssociationRefine201711150708/").registerTempTable("DimStoreDealerAssociationTT")


DimStoreRefined_DF = spark.read.parquet(DimStoreRefined).registerTempTable("DimStoreRefinedTT") 

#DimStoreRefined_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular-qa/Store/2017/11/StoreRefined201711200848").registerTempTable("DimStoreRefinedTT")


DimTechBrandHierarchy_DF = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(DimTechBrandHierarchy).registerTempTable("DimTechBrandHierarchyTT")
		
#DimTechBrandHierarchy_DF = spark.read.format("com.databricks.spark.csv").\
#        option("header", "true").\
#        option("treatEmptyValuesAsNulls", "true").\
#        option("inferSchema", "true").\
#        load("s3n://tb-us-east-1-dev-delivery-regular/WT_TB_HIER_LVL/Current/").registerTempTable("DimTechBrandHierarchyTT")
        
#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

joined_DF = spark.sql("select distinct 'I' as CDC_IND_CD, "
                    + "case when d.HierarchyName = 'Store Hierarchy' then d.Hierarchy_Id else ' ' end as STORE_HIER_ID, "
                    + "case when d.HierarchyName = 'ATT Hierrarchy' then d.Hierarchy_Id else ' ' end as ATT_HIER_ID, "
                    + "a.storenumber as STORE_NUM, a.companycd CO_CD, a.source_identifier as SRC_ID, a.locationname as LOC_NM, a.Abbreviation as ABBR, "
                    + "a.GLCode as GL_CD,a.storestatus as STORE_STAT, a.storemanageremployeeid as SM_EMP_ID, a.managercommissionable as MGR_CMSNBLE, a.address as ADDR, "
					+ "a.city as CTY, a.stateprovince as ST_PRV, a.zippostal as ZIP_PSTL, a.country as CNTRY, a.phone as PH, a.fax as FAX, a.storetype as STORE_TYP, "
                    + "a.stafflevel as STAFF_LVL, a.squarefootrange as S_FT_RNG, a.squarefoot as SQ_FT, a.latitude as LATTD, a.longitude as LNGTD, "
					+ "a.addressverficationstatus as ADDR_VER_STAT, a.timezone as TZ, a.adjustdst as ADJ_DST, a.emailaddress as EMAIL, a.consumerlicensenumber as CNSMR_LIC_NBR, "
                    + "a.saleinvoicecomment as SALE_INV_COMNT, a.taxes as TAXES, a.rent as RNT, a.insurancecompanyname as INS_CO_NM, a.uselocationemail as USE_LOC_EMAIL, "
                    + "a.locationtype as LOC_TYP, a.landlordname as LNDLRD_NT, a.leasestartdate as LEASE_START_DT, a.leaseenddate as LEASE_END_DT, "
                    + "a.opendate as OPEN_DT, a.closedate AS CLOSE_DT, a.relocationdate as RELOC_DT, a.storetier as STORE_TIER, a.monday_open_time as MON_OPEN_TM, "
					+ "a.monday_close_time as MON_CLOSE_TM, a.tuesday_open_time as TUE_OPEN_TM, a.tuesday_close_time as TUE_CLOSE_TM, a.wednesday_open_time as WED_OPEN_TM, "
                    + "a.wednesday_close_time as WED_CLOSE_TM, a.thursday_open_time as THU_OPEN_TM, a.thursday_close_time as THU_CLOSE_TM, a.friday_open_time as FRI_OPEN_TM, "
					+ "a.friday_close_time as FRI_CLOSE_TM, a.saturday_open_time as SAT_OPEN_TM, a.saturday_close_time as SAT_CLOSE_TM, a.sunday_open_time as SUN_OPEN_TM, "
                    + "a.sunday_close_time as SUN_CLOSE_TM, a.acquisitionname as ACN_NM, a.storeclosed as STORE_CLOS, a.basestore as BASE_STORE, a.compstore as COMP_STORE, "
					+ "a.samestore as SAME_STORE, a.franchisestore as FRNCHS_STORE, a.leaseexpiration as LEASE_EXP, a.buildtype as BUILD_TYPE_CD, "
                    + "a.c_n_cdesignation as C_AND_C_DSGN, a.authorizedretailertagline as AUTH_RTL_TAG_LINE_STAT, a.pylonormonumentpanels as PYLON_MONMNT_PANELS, "
					+ "a.sellingwalls as SELL_WALLS, a.memorableaccessorywall as MEMORABLE_ACC_WALL, a.cashwrapexpansion as CSH_WRAP_EXPND, a.windowwrapgrpahics as WNDW_WRAP_GRPHC, "
					+ "a.dtvnowindicator as LVE_DTV, a.learningtables as LRN_TBL, a.communitytable as COMNTY_TBL, a.diamonddisplays as DMND_DSP, "
                    + "a.c_fixtures as C_FIXTURES, a.tickiosk as TIO_KSK, a.approvedforflexblade as APRV_FOR_FLEX_BLD, "
					+ "a.capindexscore as CAP_IDX_SCORE, a.sellingwallsnotes as SELL_WALL_NT, a.remodeldate as REMDL_DT, a.dtvnowindicator as DTV_NOW_IND, "
					+ "a.baeworkdayid as BAE_WRKDAY_ID, a.bsisworkdayid as BSIS_WRKDAY_ID "
                    + "from DimStoreRefinedTT a "
                    + "inner join DimStoreDealerAssociationTT b "
                    + "on a.storenumber = b.StoreNumber "
                    + "inner join DimATTDealerCodeTT c "
                    + "on b.DealerCode = c.DealerCode "
                    + "inner join DimTechBrandHierarchyTT d "
                    + "on a.district = d.Level_3_Name or c.ATTMarketName = d.Level_2_Name "
                    + "where b.AssociationType = 'Retail' and b.AssociationStatus = 'Active' "
                    + "order by SRC_ID").registerTempTable("StoreDel")
					
					

 #+ "case when d.HierarchyName = 'ATT Hierrarchy' then d.Hierarchy_Id else ' ' end as ATTHierarchyID, "
 #+ "case when d.HierarchyName = 'Store Hierarchy' then d.Hierarchy_Id else ' ' end as StoreHierarchyID, "
dfDealerTemp1 = spark.sql("select STORE_NUM,max(coalesce(ATT_HIER_ID)) as ATT_HIER_ID ,max(coalesce(STORE_HIER_ID)) as STORE_HIER_ID from StoreDel group by STORE_NUM")

dfDealerTemp1.registerTempTable("StoreDel1")
dfDealerTemp2 = spark.sql("select distinct a.CDC_IND_CD, a.STORE_HIER_ID, a.ATT_HIER_ID, a.STORE_NUM, a.CO_CD, "
                    + "a.SRC_ID, a.LOC_NM, a.ABBR, a.GL_CD,a.STORE_STAT, a.SM_EMP_ID, a.MGR_CMSNBLE, a.ADDR, "
					+ "a.CTY, a.ST_PRV, a.ZIP_PSTL, a.CNTRY, a.PH, a.FAX, a.STORE_TYP, "
                    + "a.STAFF_LVL, a.S_FT_RNG, a.SQ_FT, a.LATTD, a.LNGTD, "
					+ "a.ADDR_VER_STAT, a.TZ, a.ADJ_DST, a.EMAIL, a.CNSMR_LIC_NBR, "
                    + "a.SALE_INV_COMNT, a.TAXES, a.RNT, a.INS_CO_NM, a.USE_LOC_EMAIL, "
                    + "a.LOC_TYP, a.LNDLRD_NT, a.LEASE_START_DT, a.LEASE_END_DT, "
                    + "a.OPEN_DT, a.CLOSE_DT, a.RELOC_DT, a.STORE_TIER, a.MON_OPEN_TM, "
					+ "a.MON_CLOSE_TM, a.TUE_OPEN_TM, a.TUE_CLOSE_TM, a.WED_OPEN_TM, "
                    + "a.WED_CLOSE_TM, a.THU_OPEN_TM, a.THU_CLOSE_TM, a.FRI_OPEN_TM, "
					+ "a.FRI_CLOSE_TM, a.SAT_OPEN_TM, a.SAT_CLOSE_TM, a.SUN_OPEN_TM, "
                    + "a.SUN_CLOSE_TM, a.ACN_NM, a.STORE_CLOS, a.BASE_STORE, a.COMP_STORE, "
					+ "a.SAME_STORE, a.FRNCHS_STORE, a.LEASE_EXP, a.BUILD_TYPE_CD, "
                    + "a.C_AND_C_DSGN, a.AUTH_RTL_TAG_LINE_STAT, a.PYLON_MONMNT_PANELS, "
					+ "a.SELL_WALLS, a.MEMORABLE_ACC_WALL, a.CSH_WRAP_EXPND, a.WNDW_WRAP_GRPHC, "
					+ "a.LVE_DTV, a.LRN_TBL, a.COMNTY_TBL, a.DMND_DSP, "
                    + "a.C_FIXTURES, a.TIO_KSK, a.APRV_FOR_FLEX_BLD, "
					+ "a.CAP_IDX_SCORE, a.SELL_WALL_NT, a.REMDL_DT, a.DTV_NOW_IND, "
					+ "a.BAE_WRKDAY_ID, a.BSIS_WRKDAY_ID "
                    + " from StoreDel a inner join StoreDel1 b on a.STORE_NUM = b.STORE_NUM")


#dfDealerTemp2 = spark.sql("select * from StoreDel a right outer join StoreDel1 b on a.StoreID = b.StoreID")

#dfDealerTemp1= spark.sql("select a.* from StoreDel a INNER JOIN (select StoreID, max(coalesce(ATTHierarchyID)) as value,max(coalesce(StoreHierarchyID)) as value2 from StoreDel group by StoreID) as b on a.StoreID=b.StoreID and a.ATTHierarchyID=b.value or a.StoreHierarchyID=b.value2");
#dfDealerTemp1 = spark.sql("select a.* from StoreDel a INNER JOIN (select StoreID,max(coalesce(ATTHierarchyID)) as value1,max(coalesce(StoreHierarchyID)) as value2 from StoreDel group by StoreID) as b on a.StoreID=b.StoreID and a.ATTHierarchyID=b.value1 and a.StoreHierarchyID=b.value2")

#dfDealerTemp1= spark.sql("select a.* from StoreDel a INNER JOIN "
#                        + "(select StoreID, isnotnull(ATTHierarchyID) as value ,isnotnull(ATTHierarchyID) as value1 from StoreDel group by StoreID ) " 
#                        + "as b on a.StoreID=b.StoreID and a.ATTHierarchyID=b.value and a.StoreHierarchyID=b.value1") 
#dfLocationMaster.show()
dfDealerTemp2.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(StoreDeliveryOutput);


spark.stop()