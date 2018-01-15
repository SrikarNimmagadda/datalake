from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField,FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

StoreHeadCountInp = sys.argv[1]
StoreHeadCountOp = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("ParquetStoreHeadCount").getOrCreate()
        
StoreheadSchema = StructType() .\
                        add("Location", StringType(), True). \
                        add("Market", StringType(), True). \
                        add("Region", StringType(), True). \
                        add("District", StringType(), True). \
                        add("PlanFTE", StringType(), True). \
                        add("ForecastedFTE", StringType(), True). \
                        add("VPApproval", StringType(), True). \
                        add("RDProposed+/-", StringType(), True). \
                        add("ApprovedFTE", StringType(), True). \
                        add("ApprovedHeadcount", StringType(), True). \
                        add("StoreManager", StringType(), True). \
                        add("BAM1", StringType(), True). \
                        add("BAM2", StringType(), True). \
                        add("FTEC", StringType(), True). \
                        add("PTEC", StringType(), True). \
                        add("FTFloater", StringType(), True).\
                        add("FTFloaterHomeStore#", StringType(), True).\
                        add("DLSCDistrictLeadSalesConsultant", StringType(), True). \
                        add("DLSCHomeStore#", StringType(), True). \
                        add("MIT", StringType(), True). \
                        add("RDApprovedHCStore&MIT", StringType(), True). \
                        add("ActualHC", StringType(), True). \
                        add("Underage", StringType(), True). \
                        add("Overage", StringType(), True). \
                        add("ApprovedSeasonalPositions", StringType(), True). \
                        add("FirstHalfApprovedHC", StringType(), True). \
                        add("SeasonalsRegisteredforTraining", StringType(), True). \
                        add("SeasonalsCurrentlyEmployeed", StringType(), True). \
                        add("WaitTimeScore", StringType(), True). \
                        add("WaitTimeScore-3MO", StringType(), True). \
                        add("HCbyTraffic", StringType(), True). \
                        add("HCbyTrafficDiff", StringType(), True). \
                        add("HCbyTraffic-3MO", StringType(), True). \
                        add("HCbyTrafficDiff-3MO", StringType(), True).\
                        add("RDJusitifcationforOverage", StringType(), True)                      
                 
                    
dfStoreHeadCount = spark.read.format("com.crealytics.spark.excel").\
                option("location", StoreHeadCountInp).\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(StoreheadSchema). \
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")                        


                
#########################################################################################################
# Reading the source data files #
#########################################################################################################

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

#dfStoreHeadCount.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(StoreHeadCountOp)

dfStoreHeadCount.coalesce(1).select("*"). \
    write.parquet(StoreHeadCountOp + '/' + todayyear + '/' + todaymonth + '/' + 'ParquetStoreHeadCount' + FileTime);
    
spark.stop()  