# -*- coding: utf-8 -*-
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

LocationMasterList = sys.argv[1]
OutputException = sys.argv[2]
OutputException1=sys.argv[3]
OutputException2=sys.argv[4]
Fileformat=sys.argv[5]
NullFound1=sys.argv[6]
NullFound2=sys.argv[7]

spark = SparkSession.builder.\
    appName("LocationMasterRQ4Parquet").getOrCreate()


class Error(Exception):
    """Base class for other exceptions"""
    pass


class NullValueError1(Error):
    """Raised StoreType not null"""
    pass


try:
    dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(LocationMasterList)
except Exception as err:
    print('GameStopFile format Exception') 
    dfOutput1= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(Fileformat)
           
    dfOutput1.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException)

try:
    storeIDnullcount = dfLocationMaster.filter(dfLocationMaster['StoreID'].isNull()).count()
    if storeIDnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('StoreID have NULL records') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)


try:
    Abbreviationnullcount = dfLocationMaster.filter(dfLocationMaster['Abbreviation'].isNull()).count()
    if Abbreviationnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('Abbreviation have NULL records') 
    dfOutput3= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound2)
           
    dfOutput3.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException2)
spark.stop()
