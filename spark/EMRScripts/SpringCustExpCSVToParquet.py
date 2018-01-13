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

SpringCustExpInput = sys.argv[1]
SpringCustExpOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("SpringCustExp_CSVToParquet").getOrCreate()
        
dfProductCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(SpringCustExpInput)
					
						
dfProductCust = dfProductCust.withColumnRenamed("Location", "location").\
                              withColumnRenamed("5 Key Behaviors", "5_key_behaviors").\
                              withColumnRenamed("Effective Solutioning", "effective_solutioning").\
                              withColumnRenamed("Integrated Experience", "integrated_experience").\
                              withColumnRenamed("Spring Market", "spring_market").\
			  				  withColumnRenamed("Spring Region ", "spring_region").\
			 				  withColumnRenamed("Spring District", "spring_district").\
			 				  withColumnRenamed("Market", "market").\
			 				  withColumnRenamed("Dealer Code", "dealer_code")


dfProductCust.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(SpringCustExpOutput);

spark.stop()