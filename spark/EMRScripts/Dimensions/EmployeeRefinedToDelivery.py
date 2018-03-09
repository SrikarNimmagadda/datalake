# This module for Company Delivery

from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col


EmpRefineInp = sys.argv[1]
EmpOutputArg = sys.argv[2]

spark = SparkSession.builder.\
    appName("EmployeeDelivery").getOrCreate()


# Read the 2 source files

dfEmployee = spark.read.parquet(EmpRefineInp)

dfEmployee.registerTempTable("emp")


# Spark Transformation begins here

dfEmployee = spark.sql("select a.sourceemployeeid as SRC_EMP_ID, " +
                       "a.companycd as CO_CD," +
                       "a.sourcesystemname as SRC_SYS_NM," +
                       "a.workdayid as WRKDAY_ID," +
                       "a.name as NME," +
                       "a.gender as GNDR,a.workemail as WRK_EMAIL," +
                       "a.statusindicator as STAT_IND," +
                       "a.parttimeindicator as PTTM_IND," +
                       "a.title as JOB_TITLE, a.language as LANG," +
                       "a.jobrole as JOB_ROLE,a.city as CTY," +
                       "a.stateprovince as ST_PROVN,a.country as CNTRY," +
                       "a.startdate as START_DT,a.terminationdate " +
                       "as TRMNTN_DT," +
                       "a.commissiongroup as CMSN_GRP," +
                       "a.compensationtype AS COMP_TYP," +
                       "a.employeemanagerid as EMP_MGR_ID," +
                       "a.username as USRNM," +
                       "a.worknumber as WRKNBR,a.email as EMAIL," +
                       "a.CDC_IND_CD as CDC_IND_CD," +
                       "a.datecreatedatsource as DT_CRT_AT_SRC," +
                       "a.employeelastmodifieddate as " +
                       "EMP_LST_MOD_DT from emp a")

dfEmployee = dfEmployee.where(col("SRC_EMP_ID").isNotNull())
dfEmployee = dfEmployee.where(col("CO_CD").isNotNull())
dfEmployee = dfEmployee.where(col("SRC_SYS_NM").isNotNull())

dfEmployee.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").\
    save(EmpOutputArg + '/' + 'Current')

dfEmployee.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("append").\
    save(EmpOutputArg + '/' + 'Previous')


spark.stop()
