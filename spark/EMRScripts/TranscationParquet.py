from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

TransEmpInp = sys.argv[1]
TransEmpOp = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
    appName("DealerCodeRefinary").getOrCreate()

TransEmpSchema = StructType() .\
    add("Market", StringType(), True). \
    add("Region", StringType(), True). \
    add("District", StringType(), True). \
    add("SalesPerson", StringType(), True). \
    add("SalesPersonID", StringType(), True). \
    add("AccruedWired", StringType(), True). \
    add("DFAccessory", StringType(), True). \
    add("FeatureAdjustment", StringType(), True). \
    add("IncorrectActUp", StringType(), True). \
    add("IncorrectPlan", StringType(), True). \
    add("IncorrectPrepaid", StringType(), True). \
    add("TVAdjustment", StringType(), True). \
    add("UncollectedDeposit", StringType(), True). \
    add("WiredAdjustment", StringType(), True). \
    add("GrandTotal", StringType(), True)

dfTransEmp = spark.read.format("com.crealytics.spark.excel").\
    option("location", TransEmpInp).\
    option("sheetName", "Employee").\
    option("treatEmptyValuesAsNulls", "true").\
    option("addColorColumns", "false").\
    option("inferSchema", "false").\
    schema(TransEmpSchema). \
    option("spark.read.simpleMode", "true"). \
    option("useHeader", "true").\
    load("com.databricks.spark.csv")


#########################################################################################################
# Reading the source data files #
#########################################################################################################

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

# dfTransEmp.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(TransEmpOp)

dfTransEmp.coalesce(1).select("*"). \
    write.parquet(TransEmpOp + '/' + todayyear + '/' +
                  todaymonth + '/' + 'transEmp' + FileTime)

spark.stop()
