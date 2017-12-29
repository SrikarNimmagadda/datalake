# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import re 
import logging
import pandas
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

LocationMasterList = sys.argv[1]


spark = SparkSession.builder.\
    appName("LocationMasterRQ4Parquet").getOrCreate()


dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(LocationMasterList)


dfLocationMaster.City




spark.stop()
