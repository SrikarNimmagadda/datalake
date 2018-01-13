from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
import pyspark.sql.functions as sf

StoreAssociationInput = sys.argv[1]
StoreAssociationOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        
#########################################################################################################
# Reading the source parquet files #
#########################################################################################################

dfStoreDealer  = spark.read.parquet(StoreAssociationInput)         

#########################################################################################################
############# Transformation starts #
#########################################################################################################
dfStoreDealer.registerTempTable("storeAss")                    
dfStoreDealerTemp = spark.sql("select a.StoreNumber as STORE_NUM,'4' as CO_CD,a.DealerCode as DLR_CD,a.AssociationType as ASSOC_TYP,"
                    + "a.AssociationStatus as ASSOC_STAT from storeAss a")

'''
new_df = dfStoreDealerTemp.withColumn('EdwBatchId', lit(None).cast(StringType())).\
                           withColumn('EdwCreateDttm', lit(None).cast(StringType())).\
                           withColumn('EdwUpdateDttm', lit(None).cast(StringType())).\
                           withColumn('EfcDt', lit(None).cast(StringType())).\
                           withColumn('EndDt', lit(None).cast(StringType())).\
                           withColumn('ActInd', lit(None).cast(StringType())) 

'''
#########################################################################################################
############# write output in CSV file #
#########################################################################################################

dfStoreDealerTemp.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(StoreAssociationOutput);


spark.stop()   
                    