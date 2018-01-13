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
from pyspark.sql.functions import *


spark = SparkSession.builder.\
        appName("DimStoreGoalsTranspose").getOrCreate()

StoreGoalsInput = sys.argv[1]
StoreGoalsOutput = sys.argv[2]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################
 
							 
StoreGoals_DF = spark.read.parquet(StoreGoalsInput).registerTempTable("StoreGoalsRefined_TT")
				   

FinalStoreGoals_DF = spark.sql("select a.report_date as RPT_DT, a.kpi_name as KPI_NM, a.store_num as STORE_NUM, a.company_code as CO_CD, a.goal_value GOAL_VAL "
                             + "from StoreGoalsRefined_TT a")

FinalStoreGoals_DF.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(StoreGoalsOutput)
			
spark.stop()