# This module for Company PII Delivery

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


CustomerPIIIntputFile = sys.argv[1]
CustomerPIIOutputPath = sys.argv[2]


spark = SparkSession.builder.\
    appName("CompanyPIIDelivery").getOrCreate()


#   Read the 2 source files

dfcustomerPII = spark.read.parquet(CustomerPIIIntputFile)

dfcustomerPII.registerTempTable("customerpii")


#  Spark Transformation begins here


dfSpringComm = spark.sql("select a.sourceidentifier AS SRC_CUST_ID,"
                         + "a.companycd AS CO_CD,"
                         + "a.sourcesystemname as SRC_SYS_NM,"
                         + "a.phone as PH,a.email as EMAIL,"
                         + "a.address1 as ADDR1,a.address2 as ADDR2,"
                         + "a.phone2type as PHN2_TYP,"
                         + "a.phone3type as PHN3_TYP,"
                         + "a.phone4type as PHN4_TYP,"
                         + " a.phone5type as PHN5_TYP,"
                         + "a.phone2 as PHN2,a.phone3 as PHN3,"
                         + "a.phone4 as PHN4,a.phone5 as PHN5,"
                         + "a.CDC_IND_CD as CDC_IND_CD "
                         + " from customerpii a")

dfSpringComm = dfSpringComm.where(col("SRC_CUST_ID").isNotNull())
dfSpringComm = dfSpringComm.where(col("CO_CD").isNotNull())
dfSpringComm = dfSpringComm.where(col("SRC_SYS_NM").isNotNull())
dfSpringComm = dfSpringComm.withColumn("CO_CD",
                                       dfSpringComm["companycd"].
                                       cast(IntegerType()))


dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").\
    save(CustomerPIIOutputPath + '/' + 'Current')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("append").\
    save(CustomerPIIOutputPath + '/' + 'Previous')


spark.stop()
