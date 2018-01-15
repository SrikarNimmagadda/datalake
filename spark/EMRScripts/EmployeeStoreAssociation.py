from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split,explode
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col


StoreRefineInp = sys.argv[1] 
EmployeeMasterListInp = sys.argv[2] #employee source parquet
CustomerInp = sys.argv[3]
EmpStoreAssOP = sys.argv[4] 
FileTime = sys.argv[5]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()

dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfEmployeeMasterList  = spark.read.parquet(EmployeeMasterListInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CustomerInp)
                   

dfEmployeeMasterList = dfEmployeeMasterList.withColumnRenamed("EmployeeID", "sourceemployeeid")

dfStoreRefine.registerTempTable("store")

dfCustomer.registerTempTable("customer")
         
split_col = split(dfEmployeeMasterList['PrimaryLocation'], ' ')

dfEmployeeMasterList = dfEmployeeMasterList.withColumn('storenumber', split_col.getItem(0))
dfEmployeeMasterList = dfEmployeeMasterList.withColumn('primarylocationname', split_col.getItem(1))

dfEmployeeMasterList.registerTempTable("employee")


#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")


dfEmpStoreAss=spark.sql("select c.companycode as companycd,a.StoreNumber as storenumber,b.sourceemployeeid,"
            + "b.PrimaryLocation,b.AssignedLocations,'I' as cdcindicator "
            + "from store a inner join employee b "
            + "on a.locationname = b.primarylocationname "
            + "cross join customer1 c")
            
# Explode column
dfEmpStoreAssInd = dfEmpStoreAss.withColumn('primarylocationindicator', sf.expr("IF(INSTR(AssignedLocations, PrimaryLocation) > 0, 1, 0)")).\
withColumn('AssignedLocations',explode(split('AssignedLocations',','))).\
select(col('sourceemployeeid'),\
col('primarylocationindicator'),col('companycd'),col('storenumber'),col('cdcindicator'),col('AssignedLocations'))

dfEmpStoreAssInd.registerTempTable("empstoreass")

dfOP=spark.sql("select distinct  '' as report_date,companycd,storenumber,sourceemployeeid,primarylocationindicator,cdcindicator,'RQ4' as sourcesystemname "
            + "from empstoreass")
            

                #or a.StoreName is NULL and b.PrimaryLocation is NULL")             
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOP.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpStoreAssOP)

dfOP.coalesce(1).select("*"). \
write.parquet(EmpStoreAssOP + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeStoreAssociation' + FileTime);
                
spark.stop()