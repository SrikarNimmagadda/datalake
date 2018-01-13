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

# Passing argument for reading the file
#attscoredgoal
StoreGoalRefineInp = sys.argv[1] 
#employeeassociation
EmployeeAssInp = sys.argv[2] 
#employeerefine
EmployeerefineInp = sys.argv[3]
EmpGoalOP = sys.argv[4] 
FileTime = sys.argv[5]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()
        
dfATTGoal  = spark.read.parquet(StoreGoalRefineInp)
dfEmpAss  = spark.read.parquet(EmployeeAssInp)
dfEmpRefine  = spark.read.parquet(EmployeerefineInp)

'''
split_col = split(dfATTGoal['Store'], ' ')

dfATTGoal = dfATTGoal.withColumn('storenumber', split_col.getItem(0))
'''
dfATTGoal.registerTempTable("attscoregoal")
dfEmpAss.registerTempTable("empass")
dfEmpRefine.registerTempTable("emprefine")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

#todaydate  =  datetime.now().strftime('%Y%m%d')                                    
                
                
dfEmpGoalEmpcount = dfEmpGoal=spark.sql("select first_value(b.sourceemployeeid) as sourceemployeeid,"
            + "first_value(a.KPI_Name) as kpiname,"
            + "first_value(a.store_num) as store_num,count(b.sourceemployeeid) as empnum "
            + "from attscoregoal a inner join empass b"
            + " on a.store_num = b.StoreNumber "
            + "inner join emprefine c "
            + "on b.sourceemployeeid = c.sourceemployeeid "
            + "group by a.store_num").registerTempTable("groupempsrorenum")
            
dfEmpGoal=spark.sql("select '' as report_date,b.companycd ,b.sourceemployeeid,"
            + "c.name as employeename,a.KPI_Name as kpiname,"
            + "a.Goal_Value/d.empnum as goalvalue "
            + "from attscoregoal a inner join empass b"
            + " on a.store_num = b.StoreNumber "
            + "inner join emprefine c "
            + "on b.sourceemployeeid = c.sourceemployeeid "
            + "inner join groupempsrorenum d "
            + "on a.store_num = d.store_num")
            
dfOP = dfEmpGoal.withColumn("goalvalue", sf.round(dfEmpGoal["goalvalue"], 2))                
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOP.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpGoalOP)

dfOP.coalesce(1).select("*"). \
write.parquet(EmpGoalOP + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeGoalRefine' + FileTime);
                
spark.stop()                  

          
