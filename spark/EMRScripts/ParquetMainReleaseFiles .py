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

SpringCommCust = sys.argv[1]
EmployeeCust = sys.argv[2]
CompanyOutputArg = sys.argv[3]
CompanyFileTime = sys.argv[4]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        

dfSpringCommCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        option("quote", "\"").\
                        option("multiLine", "true").\
                        option("spark.read.simpleMode","true").\
                        option("useHeader", "true").\
                        load(SpringCommCust) 
                        
#option("delimiter", ",").\

###############Rename columns of customer#######################                
                
 
 
            
dfEmployeeCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        option("quote", "\"").\
                        option("multiLine", "true").\
                        option("spark.read.simpleMode","true").\
                        option("useHeader", "true").\
                        load(EmployeeCust)                          

                
#dfSpringCommCust = dfSpringCommCust.withColumnRenamed("Employee Name", "EmployeeName")
           
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

   
dfEmployeeCust.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(CompanyOutputArg)


dfEmployeeCust.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'employee' + CompanyFileTime);
    
    
dfSpringCommCust.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'customer' + CompanyFileTime);


      
spark.stop()                        