# This module for Company Delivery

from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col

SpringCommCust = sys.argv[1]
CustomerOutputArg = sys.argv[2]


spark = SparkSession.builder.\
    appName("CompanyDelivery").getOrCreate()


dfSpringComm = spark.read.parquet(SpringCommCust)

dfSpringComm.registerTempTable("springcomm")

#  Spark Transformation begins here


dfSpringComm = spark.sql("select a.sourcecustomerid AS SRC_CUST_ID," +
                         "a.companycd AS CO_CD," +
                         "a.sourcesystemname as SRC_SYS_NM," +
                         "a.firstname AS FIRST_NM,a.lastname AS LAST_NM," +
                         "a.companyname AS COMP_NM," +
                         "a.customertype as CUST_TYP," +
                         "a.city AS CTY,a.stateprovince AS ST_PRV," +
                         "a.postalcode AS PSTL_CD,a.country AS CNTRY," +
                         "a.multilevelpriceidentifier AS MULTI_PRC_ID," +
                         "a.billingaccountnumber AS BILL_ACCTNG_NUM," +
                         "a.totalactivations AS TOT_ACTVNG," +
                         "a.datecreatedatsource AS DT_CRT_AT_SRC," +
                         "a.vipcustomerindicator AS VIP_CUST_IND," +
                         "a.tracpointmembernumber as TRKPTMBRNBR," +
                         "a.sourceemployeeid as SRC_EMP_ID," +
                         "a.sourceemployeeidassignedto as " +
                         "SRC_EMP_ID_ASSIGNED_TO," +
                         "a.customerlastmodifieddate as CUST_LST_MOD_DT," +
                         " a.phone2type as PHN2_TYP," +
                         "a.phone3type as PHN3_TYP," +
                         "a.phone4type as PHN4_TYP," +
                         "a.phone5type as PHN5_TYP," +
                         " a.companycontact as CO_CNTCT," +
                         "a.storelastservicedat as STORE_LST_SRVCD_AT," +
                         "a.CDC_IND_CD as CDC_IND_CD, a.title as Title " +
                         " from springcomm a")

dfSpringComm = dfSpringComm.where(col("SRC_CUST_ID").isNotNull())
dfSpringComm = dfSpringComm.where(col("CO_CD").isNotNull())
dfSpringComm = dfSpringComm.where(col("SRC_SYS_NM").isNotNull())


dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").\
    save(CustomerOutputArg + '/' + 'Current')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("append").\
    save(CustomerOutputArg + '/' + 'Previous')

spark.stop()
