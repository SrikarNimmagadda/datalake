#This module for Product Delivery#############

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

ProdRefineInp = sys.argv[1]
ProdOutputArg = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
    appName("ProductDelivery").getOrCreate()

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfSpringComm = spark.read.parquet(ProdRefineInp)

dfSpringComm.registerTempTable("spring")


#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfSpringComm = spark.sql("select distinct a.productsku AS PROD_SKU,a.companycd AS CO_CD,a.productname AS PROD_NME,a.categoryid AS CAT_ID,"
                         + "a.sourcesystemname as SRC_SYS_NM,"
                         + "a.productlabel AS PROD_LBL,a.defaultcost AS DFLT_COST,"
                         + "a.averagecost AS AVERAGE_CST,a.unitcost AS UNT_CST,a.mostrecentcost AS MST_RCNT_CST,a.manufacturer AS MFR,"
                         + "a.manufacturerpartnumber AS MFR_PRT_NBR,"
                         + "a.pricingtype AS PRC_TYP,a.defaultretailprice AS DFLT_RTL_PRC,a.defaultmargin AS DFLT_MRGN,a.floorprice AS FLOOR_PRC,"
                         + "a.defaultminimumquantity AS DFLT_MIN_QTY,"
                         + "a.defaultmaximumquantity AS DFLT_MAX_QTY,a.nosaleflag AS NO_SALE_FLG,a.rmadays AS RMA_DAY,"
                         + "a.defaultinvoicecomments AS DFLT_INV_CMNT,"
                         + "a.discountable AS DSCNT_FLG,"
                         + "a.defaultdiscontinueddate AS DFLT_DSCNT_DT,a.datecreatedatsource AS DT_CRT_AT_SRC,a.productactiveindicator AS PROD_ACT_IND,"
                         + "a.ecommerceitem AS ECOM_ITM,"
                         + "a.defaultvendorname AS DFLT_VNDR_NME,a.primaryvendorsku AS PRI_VNDR_SKU,a.costaccount AS CST_ACCT,"
                         + "a.revenueaccount AS RVNU_ACCT,a.inventoryaccount AS INV_ACCT,a.inventorycorrectionsaccount AS INV_CRCT_ACCT,"
                         + "a.rmanumberrequired AS RMA_NBR_RQMT,a.warrantylengthunits AS WRNTY_LEN_UNTS,a.warrantylengthvalue AS WRNTY_LEN_VAL,"
                         + "a.commissiondetailslocked AS CMSN_DTL_LCK,"
                         + "a.showoninvoice AS SHOW_ON_INV,a.refundable AS REFND,a.refundperiodlength AS REFND_PRD_LEN,"
                         + "a.refundtoused AS REFND_TO_USED,a.servicerequesttype AS SRVC_RQST_TYP,"
                         + "a.multilevelpricedetailslocked AS MLTI_LVL_PRC_DTL_LCK,a.backorderdate AS BAK_ORD_DT,a.storeinstoresku as STORE_INSTORE_SKU,"
                         + "a.storeinstoreprice as STORE_INSTORE_PRC,"
                         + "a.defaultdonotorder AS DFLT_DO_NOT_ORD,a.defaultspecialorder AS DFLT_SPCL_ORD,"
                         + "a.defaultdateeol AS DFLT_DT_EOL,a.defaultwriteoff AS DFLT_WRT_OFF,a.noautotaxes AS NO_AUTO_TAXES,"
                         + "a.cdcindicator as CDC_IND_CD, a.discountableindicator as DSCNT_IND from spring a")


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(ProdOutputArg)


spark.stop()
