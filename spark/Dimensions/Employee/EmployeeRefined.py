from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *


# Passing argument for reading the file

SpringCommunication = sys.argv[1]
CustomerInp = sys.argv[2]
CompanyOutputArg = sys.argv[3]
FileTime = sys.argv[4]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()


#########################################################################################################
#                                 Reading the source data file                                         #
#########################################################################################################

dfSpringComm = spark.read.parquet(SpringCommunication)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    load(CustomerInp)

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfSpringComm = dfSpringComm.withColumnRenamed("ID#", "workdayid").\
    withColumnRenamed("EmployeeID", "sourceemployeeid").\
    withColumnRenamed("EmployeeName", "name").\
    withColumnRenamed("Gender", "gender").\
    withColumnRenamed("EmailAddress", "workemail").\
    withColumnRenamed("Disabled", "status").\
    withColumnRenamed("PartTime", "parttimeindicator").\
    withColumnRenamed("Language", "language").\
    withColumnRenamed("JobTitle", "title").\
    withColumnRenamed("SecurityRole", "securityrole").\
    withColumnRenamed("City", "city").\
    withColumnRenamed("State/Prov", "stateprovision").\
    withColumnRenamed("Country", "country").\
    withColumnRenamed("Supervisor", "supervisorname").\
    withColumnRenamed("StartDate", "startdate").\
    withColumnRenamed("TerminationDate", "terminationdate").\
    withColumnRenamed("TerminationNotes", "terminationnotes").\
    withColumnRenamed("CommissionGroup", "commissiongroup").\
    withColumnRenamed("CompensationType", "commissiontype").\
    withColumnRenamed("PersonalEmail", "personalemail").registerTempTable("Spring")
dfCustomer.registerTempTable("customer")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

dfCustomer = spark.sql(
    "select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")
dfSpringComm = spark.sql("select '' as report_date,b.companycode as companycd,a.sourceemployeeid,a.workdayid,'RQ4' as sourcesystemname,a.name,"
                         + "a.gender,a.workemail,"
                         + "a.status,a.parttimeindicator,a.language,a.title,a.securityrole as jobrole,a.city,a.stateprovision,a.country,"
                         + "a.supervisorname as employeemanagername,a.startdate,a.terminationdate,a.terminationnotes,a.commissiongroup,a.commissiontype,"
                         + "'I' as cdcindicator from Spring a cross join customer1 b")

dfSpringComm.printSchema()
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(CompanyOutputArg)

dfSpringComm.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' +
                  todaymonth + '/' + 'EmployeeRefine' + FileTime)

spark.stop()
