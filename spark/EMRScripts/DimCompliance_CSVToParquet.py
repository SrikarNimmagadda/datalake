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

CnCTrainingReportsInput = sys.argv[1]
CnCTrainingReportsOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("DimCompliance_CSVToParquet").getOrCreate()
        
dfProductCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(CnCTrainingReportsInput)					
						
						
dfProductCust = dfProductCust.withColumnRenamed("Region", "region").\
                              withColumnRenamed("District", "district").\
                              withColumnRenamed("RQ4 Location", "rq4_location").\
                              withColumnRenamed("LSO Location", "lso_location").\
                              withColumnRenamed("Position", "position").\
			  				  withColumnRenamed("XMID", "xmid").\
			 				  withColumnRenamed("RQ4/Spring U Name", "rq4_or_spring_u_name").\
			 				  withColumnRenamed("LSO Hire Date", "lso_hire_date").\
			 				  withColumnRenamed("Pass/Fail", "pass_or_fail").\
			 				  withColumnRenamed("Complance", "compliance").\
							  withColumnRenamed("MIT", "mit").\
							  withColumnRenamed("Ongoing", "ongoing").\
							  withColumnRenamed("Wk/Wk Inc", "wk_or_wk_inc").\
							  withColumnRenamed("Incomplete", "incomplete")


dfProductCust.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(CnCTrainingReportsOutput);
    
spark.stop()