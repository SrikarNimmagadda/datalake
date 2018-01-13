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

ProductCategoryInput = sys.argv[1]
ProductCategoryOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("SpringCustExp_CSVToParquet").getOrCreate()
        
dfProductCatg = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(ProductCategoryInput)
					
						
dfProductCatg = dfProductCatg.withColumnRenamed("ID", "id").\
                              withColumnRenamed("Description", "description").\
                              withColumnRenamed("CategoryPath", "categorypath").\
                              withColumnRenamed("ParentID", "Parentid").\
                              withColumnRenamed("DivisionID", "divisionid").\
			  				  withColumnRenamed("AccessedDate", "accesseddate").\
			 				  withColumnRenamed("DateCreated", "datecreated")


dfProductCatg.coalesce(1).select("*"). \
write.mode("overwrite").parquet(ProductCategoryOutput);

spark.stop()