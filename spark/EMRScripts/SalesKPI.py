#This module for Sales Revenue KPI #############
from __future__ import print_function
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf
import re


SalesDetailInp = sys.argv[1]
ProductInp = sys.argv[2]
ProductCategoryInp = sys.argv[3]
StoreTransAdjustmentInp = sys.argv[4]
B2BCreditReqInp = sys.argv[5]
StoreInp = sys.argv[6]
ATTSalesActualInp = sys.argv[7]
StoreTrafficInp = sys.argv[8]
StoreCCTrainingInp = sys.argv[9]
StorePayrollInp = sys.argv[10]
StoreRecruitingHeadcountInp = sys.argv[11]
EmployeeGoalInp = sys.argv[12]
EmployeeMasterInp = sys.argv[13]
EmpStoreAssociationInp = sys.argv[14]
SalesLeadInp = sys.argv[15]
StoreCustomerExpInp = sys.argv[16]
KPIList = sys.argv[17]
KPIOutput = sys.argv[18]
FileTime = sys.argv[19]

spark = SparkSession.builder.\
        appName("SalesKPI").getOrCreate()
        
#########################################################################################################
#                                 Read the source files                                              #
#########################################################################################################
        
dfSalesDetail  = spark.read.parquet(SalesDetailInp)
dfProduct  = spark.read.parquet(ProductInp)
dfProductCategory  = spark.read.parquet(ProductCategoryInp)
dfStoreTransAdjustment  = spark.read.parquet(StoreTransAdjustmentInp)
dfB2BCreditReq  = spark.read.parquet(B2BCreditReqInp)
dfStore  = spark.read.parquet(StoreInp)
dfATTSalesActual  = spark.read.parquet(ATTSalesActualInp)
dfStoreTraffic  = spark.read.parquet(StoreTrafficInp)
dfStoreCCTraining  = spark.read.parquet(StoreCCTrainingInp)
dfStorePayroll  = spark.read.parquet(StorePayrollInp)
dfStoreRecruitingHeadcount  = spark.read.parquet(StoreRecruitingHeadcountInp)
dfEmployeeGoal  = spark.read.parquet(EmployeeGoalInp)
dfEmployeeMaster  = spark.read.parquet(EmployeeMasterInp)
dfEmpStoreAssociation  = spark.read.parquet(EmpStoreAssociationInp)
dfSalesLead  = spark.read.parquet(SalesLeadInp)
dfStoreCustomerExp  = spark.read.parquet(StoreCustomerExpInp)

dfKpilist = spark.read.format("com.crealytics.spark.excel").\
                option("location", KPIList).\
                option("sheetName", "KPIs for DL-Calculation").\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "true").\
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")

'''
ATTSalesActualInp = sys.argv[1]
KPIList = sys.argv[2]
KPIOutput = sys.argv[3]
FileTime = sys.argv[4]
'''
spark = SparkSession.builder.\
        appName("SalesKPI").getOrCreate()
        
#########################################################################################################
#                                 Read the source files                                              #
#########################################################################################################

dfSalesDetail.registerTempTable("SalesDetails")
dfProduct.registerTempTable("Product")
dfProductCategory.registerTempTable("ProductCategories")
dfStoreTransAdjustment.registerTempTable("StoreTransactionAdjustments")
dfB2BCreditReq.registerTempTable("B2BCreditRequests")
dfStore.registerTempTable("Store")
dfATTSalesActual.registerTempTable("ATTSalesActuals")
dfStoreTraffic.registerTempTable("StoreTraffic")
dfStoreCCTraining.registerTempTable("StoreCCTraining")
dfStorePayroll.registerTempTable("StorePayroll")
dfStoreRecruitingHeadcount.registerTempTable("StoreRecruitingHeadcount")
dfEmployeeMaster.registerTempTable("Employee")
dfEmpStoreAssociation.registerTempTable("EmployeeStoreAssociation")
dfSalesLead.registerTempTable("SalesLeads")
dfStoreCustomerExp.registerTempTable("StoreCustomerExperience")
dfEmployeeGoal.registerTempTable("EmployeeGoal")
                
#dfKpilist = dfKpilist.withColumnRenamed("Calculation Level", "CalculationLevel")

dfKpilistlev0 = dfKpilist.filter(col('CalculationLevel') == 0)
rowcount = dfKpilistlev0.count()
print(rowcount)
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
listvalue = dfKpilistlev0.rdd
#listvalue.foreach(print)
list1 = listvalue.take(listvalue.count())
#print(list1)
i = 0
for value in list1:
    #Row(name=x[0], age=int(x[1])))
    i = i+1
    doclist = []
    print(i)
    kpiheader = value.KPIName
    expression = value.Expression
    filtercondition = value.FilterCondition
    tablename = value.FromClause
    groupbycolumn = value.GroupByColumns
    fieldcolumns = value.FieldColumns
    #re.split(r"[\[\]]", fieldcolumns)
    words = fieldcolumns.split('.')
    for word in words:
        doclist.append(word)
        
    firsttablename = doclist[0]
    print(kpiheader)
    if filtercondition:
        sqlstring = "select " + fieldcolumns + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue,first_value(" + firsttablename + ".report_date) as report_date,first_value(" \
                + firsttablename + ".companycd) as companycd from " \
                + tablename + "  " + filtercondition + "  " + groupbycolumn
                
        df = spark.sql(sqlstring)
    
        df.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").save(KPIOutput + '/' + todayyear + '/' + todaymonth + '/' + kpiheader + FileTime)
                
    else:
        sqlstring = "select " + fieldcolumns + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue,first_value(" + firsttablename + ".report_date) as report_date,first_value(" \
                + firsttablename + ".companycd) as companycd from " \
                + tablename + "  " + groupbycolumn
                
        df = spark.sql(sqlstring)
    
        df.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").save(KPIOutput + '/' + 'SalesKPI' + FileTime + '/' + kpiheader + FileTime)
      
    
    #print(sqlstring)
    
    
       
     
spark.stop()

