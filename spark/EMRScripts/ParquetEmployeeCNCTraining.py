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

EmpCNCInp = sys.argv[1]
EmpCNCOP = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("ParquetCNCTraining").getOrCreate()
        
#					PRD573	RET650				l2-dlr4006	l2-dlr4013	l2-dlr4016	l2-dlr4020	l2-dlr4025	l2-dlr4029

CNCschema = StructType() .\
                        add("Sorter", StringType(), True). \
                        add("Market", StringType(), True). \
                        add("Region", StringType(), True). \
                        add("District", StringType(), True). \
                        add("RQ4Location", StringType(), True). \
                        add("LSOLocation", StringType(), True). \
                        add("Position", StringType(), True). \
                        add("XMID", StringType(), True). \
                        add("RQ4/SpringUName", StringType(), True). \
                        add("LSOHireDate", StringType(), True). \
                        add("Pass/Fail", StringType(), True). \
                        add("Complance", StringType(), True). \
                        add("MIT", StringType(), True). \
                        add("Ongoing", StringType(), True). \
                        add("Wk/WkInc", StringType(), True). \
                        add("Incomplete", StringType(), True). \
                        add("2017CompletionDateRequired", StringType(), True)                      
                    
                        
 
     
dfEmpCNC = spark.read.format("com.crealytics.spark.excel").\
                option("location", EmpCNCInp).\
                option("sheetName", "C&C Audit Report").\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(CNCschema). \
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")
                
                                             
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfEmpCNC.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpCNCOP)

dfEmpCNC.coalesce(1).select("*"). \
    write.parquet(EmpCNCOP + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeCNCTraining' + FileTime);

    
      
spark.stop()                        