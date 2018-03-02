# This module for Company Refine

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

CustomerPIIIntputFile = sys.argv[1]
CustomerPIIOutputPath = sys.argv[2]

spark = SparkSession.builder.\
    appName("CustomerPIIRefinary").getOrCreate()


#  Read the 2 source files

dfCustomerPII = spark.read.parquet(CustomerPIIIntputFile)

dfCustomerPII.registerTempTable("customerPII")


# Spark Transformation begins here

dfCustomerPII = spark.sql("select a.sourcecustomerid as sourceidentifier," +
                          "4 as companycd,'RQ4' as sourcesystemname," +
                          "a.firstname,a.lastname,a.companyname," +
                          "a.customertype,a.phone,a.email," +
                          "a.address as address1, a.address2,a.city," +
                          "a.stateprovince, a.postalcode as zippostalcode," +
                          "a.country,a.multilevelpriceid " +
                          "as multilevelpriceidentifier," +
                          "a.billingaccountnumber," +
                          "a.numberofactivations as totalactivations," +
                          "a.rowinserted as datecreatedatsource," +
                          "a.vipcustomer as vipcustomerindicator," +
                          "a.tracpointmembernumber " +
                          "as  tracpointmembernumber," +
                          "a.employeename as employeename ," +
                          "a.employeeid as sourceemployeeid," +
                          "a.employeenameassignedto as employeeassignedto," +
                          "a.employeeidassignedto " +
                          "as sourceemployeeidassignedto," +
                          " a.rowupdated as customerlastmodifieddate," +
                          "a.notes, 'NA' as phone2type, 'NA' as phone2," +
                          " 'NA' as phone3type, 'NA' as phone3," +
                          " 'NA' as phone4type, 'NA' as phone4," +
                          " 'NA' as phone5type, 'NA' as phone5," +
                          "a.companycontact," +
                          "a.storeid as storelastservicedat," +
                          "CASE WHEN a.rowevent = 'Inserted' " +
                          "THEN 'I' WHEN a.rowevent = 'Updated' " +
                          "THEN 'C' END as CDC_IND_CD  from customerPII a")


dfCustomerPII.registerTempTable("customer")
dfCustomerPII = spark.sql("select * ," +
                          "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                          "as year," +
                          "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) " +
                          "as month from customer")


dfCustomerPII = dfCustomerPII.where(col("sourceidentifier").isNotNull())
dfCustomerPII = dfCustomerPII.where(col("companycd").isNotNull())
dfCustomerPII = dfCustomerPII.where(col("sourcesystemname").isNotNull())
dfCustomerPII = dfCustomerPII.\
    withColumn("companycd", dfCustomerPII["companycd"]. cast(IntegerType()))

dfCustomerPII.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(CustomerPIIOutputPath + '/' + 'Working')

dfCustomerPII.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(CustomerPIIOutputPath)

spark.stop()
