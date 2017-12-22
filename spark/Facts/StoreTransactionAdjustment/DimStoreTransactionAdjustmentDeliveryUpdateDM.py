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

spark = SparkSession.builder.\
    appName("DimStoreTransAdjDelivery").getOrCreate()


DimStoreTransAdjRefinedInput = sys.argv[1]
DimCustExpRefinedOutput = sys.argv[2]

DimCustExpRefinedInput_DF = spark.read.parquet(
    DimStoreTransAdjRefinedInput).registerTempTable("DimStoreTransAdjRefinedTable")

Final_Joined_DF = spark.sql("select a.storenumber as STORE_NUM, a.companycd as CO_CD, a.reportdate as RPT_DT, "
                            + "a.adjustment_type as ADJMNT_TYP, a.adjustment_category as ADJMNT_CAT, a.adjustment_amount as ADJMNT_AMT, a.sourcesystemname as SRC_SYS_NM "
                            + "from DimStoreTransAdjRefinedTable a")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')


Final_Joined_DF.coalesce(1).select("*"). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(DimCustExpRefinedOutput)

spark.stop()
