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

ProductCust = sys.argv[1]
ProductIden = sys.argv[2]
CompanyOutputArg = sys.argv[3]
CompanyFileTime = sys.argv[4]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        
dfProductCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(ProductCust)
						
dfProductIden = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(ProductIden)
                        
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfProductCust.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(CompanyOutputArg)

dfProductIden.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'productIdentifier' + CompanyFileTime);

dfProductCust.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'product' + CompanyFileTime);
    
spark.stop()      