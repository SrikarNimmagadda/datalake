#!/usr/bin/python

# Author           : Sarath M
# Date Created     : 12/01/2018
# Purpose  : To convert the csv file to parquet and move it to
#                    Discovery layer PII-Customer Bucket
# Version          : 1.0
# Revision History :

from pyspark.sql import SparkSession
import sys


# Input and Output Arguments

CustomerInput = sys.argv[1]
CustomerOutput = sys.argv[2]

# Creating Spark Session

spark = SparkSession.builder.\
    appName("ParquetCustomeEmployeeFile").getOrCreate()


# Loading Inputfile into Dataframe
dfCustomer = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    option("quote", "\"").\
    option("multiLine", "true").\
    option("spark.read.simpleMode", "true").\
    option("useHeader", "true").\
    load(CustomerInput)


# Renaming column names of source file

dfCust_ColRename = dfCustomer.\
    withColumnRenamed("CustomerEntityID", "customerentityid").\
    withColumnRenamed("CustomerID", "sourcecustomerid").\
    withColumnRenamed("PrimaryName", "firstname").\
    withColumnRenamed("FamilyName", "lastName").\
    withColumnRenamed("CompanyName", "companyname").\
    withColumnRenamed("CompanyContact", "companycontact").\
    withColumnRenamed("Title", "title").\
    withColumnRenamed("CustomerType", "customertype").\
    withColumnRenamed("CustomerTypeID", "customertypeid").\
    withColumnRenamed("Phone", "phone").\
    withColumnRenamed("Email", "email").\
    withColumnRenamed("DeclineToProvideEmail", "eeclineeoerovideemail").\
    withColumnRenamed("Address", "address").\
    withColumnRenamed("Address2", "address2").\
    withColumnRenamed("City", "city").\
    withColumnRenamed("StateProvince", "stateprovince").\
    withColumnRenamed("PostalCode", "postalcode").\
    withColumnRenamed("Country", "country").\
    withColumnRenamed("DoNotEmail", "donotemail").\
    withColumnRenamed("DoNotPostOfficeMail", "donotpostofficemail").\
    withColumnRenamed("DoNotCall", "donotcall").\
    withColumnRenamed("StoreID", "storeid").\
    withColumnRenamed("EmployeeID", "employeeid").\
    withColumnRenamed("EmployeeName", "employeename").\
    withColumnRenamed("EmployeeIDAssignedTo", "employeeidassignedto").\
    withColumnRenamed("EmployeeNameAssignedTo", "employeenameassignedto").\
    withColumnRenamed("MultiLevelPriceID", "multilevelpriceid").\
    withColumnRenamed("BillingAccountNumber", "billingaccountnumber").\
    withColumnRenamed("NumberOfActivations", "numberofactivations").\
    withColumnRenamed("VIPCustomer", "vipcustomer").\
    withColumnRenamed("TracPointMemberNumber", "tracpointmembernumber").\
    withColumnRenamed("ContactTypeName", "contacttypename").\
    withColumnRenamed("IndustryTypeName", "industrytypename").\
    withColumnRenamed("PositionTypeName", "positiontypename").\
    withColumnRenamed("Notes", "notes").\
    withColumnRenamed("Version", "version").\
    withColumnRenamed("RowThumbprint", "rowthumbprint").\
    withColumnRenamed("RowInserted", "rowinserted").\
    withColumnRenamed("RowUpdated", "rowupdated").\
    withColumnRenamed("RowEvent", "rowevent")

# Creating Temporary Table
dfCust_ColRename.registerTempTable("customer")


dfCust_Write = spark.sql("select * ," +
                         "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                         "as year," +
                         "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) " +
                         "as month from customer")

# Writing data to working Directory

dfCust_Write.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(CustomerOutput + '/' + 'Working')

# Writing data to partitioned Directory

dfCust_Write.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(CustomerOutput)


spark.stop()
