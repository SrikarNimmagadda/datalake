from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
import re
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


LocationMasterList = sys.argv[1]
OutputException = sys.argv[2]
unexpectedrecordFound=sys.argv[3]

spark = SparkSession.builder.\
    appName("LocationMasterRQ4Parquet").getOrCreate()

class Error(Exception):
    """Base class for other exceptions"""
    pass

class NullValueError1(Error):
    """Raised StoreType not null"""
    pass

dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(LocationMasterList)


try:
    if dfLocationMaster['GeneralLocationNotes'].str.contains('...-... ....-..., ... ....-..., ... ....-...',regex=True) | \
       dfLocationMaster['GeneralLocationNotes'].str.contains('...-... ....-..., ... ......',regex=True) | \
       dfLocationMaster['GeneralLocationNotes'].str.contains('...-... ....-..., ... ....-...',regex=True):
         raise NullValueError1

except NullValueError1 as ve:
    print('GeneralLocationNotes have invalid format records') 
    dfOutput= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(unexpectedrecordFound)
           
    dfOutput.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException)



spark.stop()
