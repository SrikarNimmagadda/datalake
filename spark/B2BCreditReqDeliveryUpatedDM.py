from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col

B2BCreditReqDeliveryInp = sys.argv[1]
B2BCreditReqDeliveryOP = sys.argv[2]
FileTime = sys.argv[3]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("SalesLeadDelivery").getOrCreate()
		   

#Reading Parquert and creating dataframe
#dfB2BCreditReq  = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Transaction/2017/11/B2BCreditReqRefined/2017/11/B2BCreditRequest20170911/")		   
dfB2BCreditReq = spark.read.parquet(B2BCreditReqDeliveryInp)
			   
#Register spark temp schema
dfB2BCreditReq.registerTempTable("b2bcreditreqrefine")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    


#FinalJoin_DF = spark.sql("select a.report_date as RPT_DT, a.companycd as CO_CD,"
#+"a.reqdatetime as RQST_DT_TM, a.dateoftransaction as TRANS_DT, a.invoicenumber as INVOICE_NBR,"
#+"a.clientname as CLNT_NM, a.ban as BAN, a.salesrepresentativeid as SALES_REP_SRC_EMP_ID_REF_BY, a.storerefby as SALES_REP_STORE_NUM_REF_BY, a.repstorenumber as STORE_NUM, '' as JOB_TITLE_CRDT_RQST_FOR, '' as STORE_CRDT_RQST_FOR,"
#+"a.fullgpinvamount as FULL_GRS_PRFT_INVOICE_AMT, a.gpcredit as GRS_PRFT_CRDT, a.gpbaedebit as GRS_PRFT_BAE_DEBIT,"
#+"a.noofcrugrossactivation as NBR_OF_CRU_GRS_ACTVNGS, a.nooftablets as NBR_OF_TBLTS, a.noofhsis as NBR_OF_HSIS, a.nooftv as NBR_OF_TVS,"
#+"a.noofopportunity as NBR_OF_OPTNY, a.protectedreven as PRTC_RVNU_TTL, a.protectioneligopps as PRTC_ELIG_OPTNY,"
#+"a.accessoryunits as ACC_UNTS, a.amaunits as AMA_UNTS, a.acceligopps as ACC_ELIG_OPTNY, '' as STORE_CRDT_APPLY, a.approvalstatus as APRV_STAT,"
#+"a.b2bmanagernmcrdapproved as B2B_MGR_EMPID_CRDT_APRV_BY, a.crdreqstatus as CRDT_RQST_STAT, a.notes as NT, a.invoicelink as INVOICE_LNK,"
#+"a.sourceemployeeid as EMPID_CRDT_APRV, '' as SRC_EMP_ID, a.sourcesystemname as SRC_SYS_NM "
#+"from b2bcreditreqrefine a")

FinalJoin_DF = spark.sql("select a.report_date as RPT_DT, a.companycd as CO_CD,"
+"a.reqdatetime as RQST_DT_TM, a.dateoftransaction as TRANS_DT, a.invoicenumber as INVOICE_NBR,"
+"a.clientname as CLNT_NM, a.ban as BAN, a.salesrepresentativeid as SALES_REP_SRC_EMP_ID_REF_BY, a.storerefby as SALES_REP_STORE_NUM_REF_BY, '' as JOB_TITLE_CRDT_RQST_FOR, '' as STORE_CRDT_RQST_FOR,"
+"a.fullgpinvamount as FULL_GRS_PRFT_INVOICE_AMT, a.gpcredit as GRS_PRFT_CRDT, a.gpbaedebit as GRS_PRFT_BAE_DEBIT,"
+"a.noofcrugrossactivation as NBR_OF_CRU_GRS_ACTVNGS, a.nooftablets as NBR_OF_TBLTS, a.noofhsis as NBR_OF_HSIS, a.nooftv as NBR_OF_TVS,"
+"a.noofopportunity as NBR_OF_OPTNY, a.protectedreven as PRTC_RVNU_TTL, a.protectioneligopps as PRTC_ELIG_OPTNY,"
+"a.accessoryunits as ACC_UNTS, a.amaunits as AMA_UNTS, a.acceligopps as ACC_ELIG_OPTNY, '' as STORE_CRDT_APPLY, a.approvalstatus as APRV_STAT,"
+"a.b2bmanagernmcrdapproved as B2B_MGR_EMPID_CRDT_APRV_BY, a.crdreqstatus as CRDT_RQST_STAT, a.notes as NT, a.invoicelink as INVOICE_LNK,"
+"a.sourceemployeeid as EMPID_CRDT_APRV, a.sourcesystemname as SRC_SYS_NM "
+"from b2bcreditreqrefine a")


FinalJoin_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(B2BCreditReqDeliveryOP);
                
spark.stop()