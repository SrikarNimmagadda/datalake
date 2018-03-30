from pyspark.sql import SparkSession
import sys

SalesLeadInp = sys.argv[1]
SalesLeadOP = sys.argv[2]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SalesLeadDelivery").getOrCreate()
# Reading Parquert and creating dataframe
dfSalesLead = spark.read.parquet(SalesLeadInp)
# Register spark temp schema
dfSalesLead.registerTempTable("salesleadtable")
####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################
FinalJoin_DF = spark.sql("select a.reportdate as RPT_DT, a.storenumber as STORE_NUM, a.dealercode as DLR_CD, '4' as CO_CD, a.baeemployeename as  BAE_NME, a.sourcesystemname as SRC_SYS_NM,a.customeraccountname as CUST_ACCT_NM,a.attdirectorofsales as ATT_DIR_OF_SALES, a.attregionsalesmanager as ATT_RGN_SALES_MGR,a.currencustomerindicator as CRNT_CUST_IND, a.salesrepresentativename as SALES_REP_NME, a.grossactivations as GRS_ACTVNGS,a.leadstatus as LEAD_STAT,a.sbassistancerequestindicator as SB_ASST_RQST_IND,a.entereddate as ENTR_DT, a.closeddate as CLOS_DT, a.lastupdatedate as LST_UPDT_DT,a.description as DESCRIPTION,a.contactinfo as CNTCT_INFO ,a.fan as FAN from salesleadtable a")
FinalJoin_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("overwrite").save(SalesLeadOP + '/' + 'Current')
FinalJoin_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("append").save(SalesLeadOP + '/' + 'Previous')
spark.stop()
