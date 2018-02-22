#!/usr/bin/python
# Author           : Sarath M
# Date Created     : 12/01/2018
# Purpose: To copy data to Refined Layer after necessary transformations.
# Version          : 1.0
# Revision History :

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Input and Output Arguments

CustomerInp = sys.argv[1]
CustomerOutput = sys.argv[2]

# Creating Spark Session

spark = SparkSession.builder.\
    appName("CustomerRefine").getOrCreate()

# Loading Inputfile into Dataframe

dfCustomer = spark.read.parquet(CustomerInp)

# Creating Temporary Table

dfCustomer.registerTempTable("customer")

dfCustomer = spark.sql("select a.sourcecustomerid as sourcecustomerid,"
                       + "4 as companycd,'RQ4' as sourcesystemname,"
                       + "a.firstname as firstname,a.lastname as lastname,"
                       + "a.companyname as companyname,"
                       + "a.customertype as customertype,a.city as city,"
                       + "a.stateprovince as stateprovince,"
                       + "a.postalcode as postalcode,a.country as country,"
                       + "a.multilevelpriceid as multilevelpriceidentifier,"
                       + "a.billingaccountnumber as billingaccountnumber,"
                       + "a.numberofactivations as totalactivations,"
                       + "a.rowinserted as datecreatedatsource,"
                       + "a.vipcustomer as vipcustomerindicator,"
                       + "a.tracpointmembernumber,a.employeename,"
                       + " a.employeeid as sourceemployeeid,"
                       + "a.employeenameassignedto,"
                       + " a.employeeidassignedto as "
                       + "sourceemployeeidassignedto,"
                       + "a.rowupdated as customerlastmodifieddate, "
                       + "'NA' as phone2type,"
                       + "'NA' as phone3type, 'NA' as phone4type,"
                       + "'NA' as phone5type,a.companycontact,"
                       + "a.storeid as storelastservicedat,"
                       + "CASE WHEN a.rowevent = 'Inserted' "
                       + "THEN 'I' WHEN a.rowevent = 'Updated' "
                       + "THEN 'C' END as CDC_IND_CD,"
                       + "a.title as title from customer a")


dfCustomer = dfCustomer.where(col("sourcecustomerid").isNotNull())
dfCustomer = dfCustomer.where(col("companycd").isNotNull())
dfCustomer = dfCustomer.where(col("sourcesystemname").isNotNull())
dfCustomer.registerTempTable("customer")
dfCustomer = spark.sql("select *,"
                       + "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) "
                       + " as year,"
                       + "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) "
                       + "as month from customer")
# FinalDF=dfCustomer.na.fill({'sourceemployeeidassignedto':-1,})

dfCustomer.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(CustomerOutput + '/' + 'Working')

dfCustomer.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(CustomerOutput)

spark.stop()
