from pyspark.sql import SparkSession
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
#Employee is nothing but july17transcationadjustment is the transcation source file
transcationInp = sys.argv[1] 
StoreRefineInp = sys.argv[2] 
CustomerInp = sys.argv[3]
EmpStoreAssInp = sys.argv[4]
EmployeeRefineInp = sys.argv[5]
CompanyOutputArg = sys.argv[6] 
FileTime = sys.argv[7]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()
        
        
        
#########################################################################################################
#                                 Reading the source data file                                         #
#########################################################################################################

dftranscation  = spark.read.parquet(transcationInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfEmpStoreAssRefine  = spark.read.parquet(EmpStoreAssInp)
dfEmployeeRefine  = spark.read.parquet(EmployeeRefineInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CustomerInp)
                   
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dftranscation = dftranscation.withColumnRenamed("SalesPerson", "employeename").\
                withColumnRenamed("SalesPersonID", "sourceemployeeid").\
                withColumnRenamed("AccruedWired", "adjustmenttype").\
                withColumnRenamed("DFAccessory", "adjustmentamount").registerTempTable("transcation")
                
dfCustomer.registerTempTable("customer")
dfStoreRefine.registerTempTable("store")
dfEmpStoreAssRefine.registerTempTable("empstoreass")
dfEmployeeRefine.registerTempTable("employee")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfSpringComm = spark.sql("select distinct '' as reportdate,b.companycode as companycd,"
            + "a.sourceemployeeid,c.storenumber,"
            + "d.Market as springmarket,d.Region as springregion,d.District as springdistrict,"
            + "a.employeename,a.adjustmenttype,a.adjustmentamount,'I' as cdcindicator  "
            + "from transcation a "
            + "cross join customer1 b "
            + "inner join employee e "
            + "on a.sourceemployeeid = e.workdayid "
            + "inner join empstoreass c "
            + "on e.sourceemployeeid = c.sourceemployeeid "
            + "inner join store d "
            + "on c.storenumber = d.StoreNumber")

#d.Market as springmarket,d.Region as springregion,d.District as springdistrict,"            
todaydate  =  datetime.now().strftime('%Y%m%d') 

'''  
dfSpringComm = dfSpringComm.withColumn('reportdate1',sf.when((dfSpringComm.reportdate == ''),todaydate)).\
drop(dfSpringComm.reportdate).\
select(col('reportdate1').alias('reportdate'),col('companycd'),col('sourceemployeeid'),\
col('springmarket'),\
col('springregion'),col('springdistrict'),col('employeename'),col('adjustmenttype'),col('adjustmentamount'),col('cdcindicator'))
'''

#dfSpringComm.printSchema()
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSpringComm.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(CompanyOutputArg)

dfSpringComm.coalesce(1).select("*"). \
write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeTransAdj' + FileTime);
                
spark.stop()
