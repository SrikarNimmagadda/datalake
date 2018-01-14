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

EmpStoreAssRefineInp = sys.argv[1] 
EmpStoreAssdeliveryOP = sys.argv[2] 
FileTime = sys.argv[3]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()

dfEmpStore  = spark.read.parquet(EmpStoreAssRefineInp)

dfEmpStore.registerTempTable("employee")
    


#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

                                      
dfEmpStoreAss=spark.sql("select distinct a.sourcesystemname as SRC_SYS_NM,a.companycd as CO_CD,a.storenumber as STORE_NUM,a.sourceemployeeid as SRC_EMP_ID, "
                + "a.primarylocationindicator as IS_PRI_LOC,a.cdcindicator as CDC_IND_CD "
                + "from employee a")
                #or a.StoreName is NULL and b.PrimaryLocation is NULL")
                
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfEmpStoreAss.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpStoreAssdeliveryOP)


spark.stop()