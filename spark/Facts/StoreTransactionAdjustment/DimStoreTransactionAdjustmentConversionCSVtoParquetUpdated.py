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

Transaction_AdjustmentStoreInput = sys.argv[1]
MiscTransaction_AdjustmentInput = sys.argv[2]
Transaction_AdjustmentStoreExpOutput = sys.argv[3]
MiscTransaction_AdjustmentExpOutput = sys.argv[4]
FileTime = sys.argv[5]

spark = SparkSession.builder.\
    appName("StoreTransactionAdj_CSVToParquet").getOrCreate()

dfStoreTransAdj = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    load(Transaction_AdjustmentStoreInput)

dfMiscTransAdj = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    load(Transaction_AdjustmentStoreInput)

dfStoreTransAdj = dfStoreTransAdj.withColumnRenamed("Location", "location").\
    withColumnRenamed("Loc", "loc").\
    withColumnRenamed("Features Commission1", "featurescommission_a").\
    withColumnRenamed("Features Commission2", "featurescommission_b").\
    withColumnRenamed("Features Commission3", "featurecommission_c").\
    withColumnRenamed("Features Commission4", "featurescommission_d").\
    withColumnRenamed("Features Commission5", "featurescommission_e").\
    withColumnRenamed("Features", "features").\
    withColumnRenamed("PerFeatures", "percentilefeatures").\
    withColumnRenamed("DirectTV Commission1", "directtvcommission_a").\
    withColumnRenamed("DirectTV Commission2", "directtvcommission_b").\
    withColumnRenamed("DirectTV Commission3", "directtvcommission_c").\
    withColumnRenamed("DirectTV Commission4", "directtvcommission_d").\
    withColumnRenamed("DirectTV Commission5", "directtvcommission_e").\
    withColumnRenamed("DirectTV", "directtv").\
    withColumnRenamed("PerDirectTV", "percentiledirecttv").\
    withColumnRenamed("Wired Commission1", "wiredcommission_a").\
    withColumnRenamed("Wired Commission2", "wiredcommission_b").\
    withColumnRenamed("Wired Commission3", "wiredcommission_c").\
    withColumnRenamed("Wired Commission4", "wiredcommission_d").\
    withColumnRenamed("Wired Commission5", "wiredcommission_e").\
    withColumnRenamed("Wired", "wired").\
    withColumnRenamed("PerWired", "percentilewired").\
    withColumnRenamed("Tier", "tire").\
    withColumnRenamed("Activation/Upgrade", "activationupgrade").\
    withColumnRenamed("Plan", "plan").\
    withColumnRenamed("Prepaid", "prepaid").\
    withColumnRenamed("Mar-17", "march_17").\
    withColumnRenamed("March 2017 Payback", "march2017payback").\
    withColumnRenamed("Total", "total").\
    withColumnRenamed("Accessories", "accessories").\
    withColumnRenamed("Spif Adjustment", "spifadjustment").\
    withColumnRenamed("Accrued", "accrued").\
    withColumnRenamed("Adjustment", "adjustment")

dfStoreTransAdj.printSchema()

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfStoreTransAdj.coalesce(1).select("*"). \
    write.parquet(Transaction_AdjustmentStoreExpOutput + '/' + todayyear +
                  '/' + todaymonth + '/' + 'StoreTransactionAdj' + FileTime)

spark.stop()
