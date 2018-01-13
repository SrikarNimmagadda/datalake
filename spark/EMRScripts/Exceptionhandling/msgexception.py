from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext
from pyspark import sql

conf = SparkConf()
conf.setMaster('local')
conf.setAppName('Test')
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)


LocationMasterList = sys.argv[1]
OutputException = sys.argv[2]

spark = SparkSession.builder.\
    appName("LocationMasterRQ4Parquet").getOrCreate()


class Error(Exception):
    """Base class for other exceptions"""
    pass

try:
    dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load("LocationMasterList")
except Exception as err:
    msg = [('GameStop_Fileformat_Exception',)]  
    schema = StructType([StructField("message", StringType(), True)])           
    df = sqlContext.createDataFrame(msg, schema)
    df.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(OutputException)

spark.stop()
