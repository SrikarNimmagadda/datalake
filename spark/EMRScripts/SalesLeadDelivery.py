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



SalesLeadInp = sys.argv[1]
SalesLeadOP = sys.argv[2]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("SalesLeadDelivery").getOrCreate()
		   

#Reading Parquert and creating dataframe
#dfSalesLead  = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Sales/2017/11/SalesLeadRefined/")		   
dfSalesLead  = spark.read.parquet(SalesLeadInp)
			   
#Register spark temp schema
dfSalesLead.registerTempTable("salesleadtable")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    


FinalJoin_DF = spark.sql("select a.customeraccount as CUST_ACCT_NM, a.baeemployeeid as BAE_EMP_ID, a.companycd as CO_CD,"
+"a.dealercode as DLR_CD, a.storenumber as STORE_NUM,a.attdirectorofsales as ATT_DIR_OF_SALES,"
+"a.attregionsalesmanager as ATT_RGN_SALES_MGR, a.currencustomerindicator as CRNT_CUST_IND,a.leadstatus as LEAD_STAT,"
+"a.grossactivations as GRS_ACTVNGS, a.sbassistancerequest as SB_ASST_RQST_IND, a.entereddate as ENTR_DT, a.closeddate as CLOS_DT,"
+"a.lastupdatedate as LST_UPDT_DT, a.report_date as RPT_DT, a.salesrepemployeeid as SALES_REP_EMP_ID,"
+"a.description as DESCRIPTION, a.notes as NT "
+"from salesleadtable a")

FinalJoin_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(SalesLeadOP);
                
spark.stop()