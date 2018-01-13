#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimStoreCnCConversionCSVtoParquet                                                                                               *
#   Written By            : Aakash Basu                                                                                                      *
#   Date Written          : October 2017                                                                                                     *
#   Project Name          : GameStop                                                                                                         *
#   Version               : 1.0                                                                                                              *
#   Description           : This code is for retrieving the customer experience of stores                                                    *
#   Change                : None																                                             *
#   History 	          : None																                                             *          
#   NOTES		          : Complete                                                                                                         *   
#	Spark Submit Command  : spark-submit --class spark.FactDeposits --master yarn-client --num-executors 30 --executor-cores 4               *
#                          --executor-memory 24G --driver-memory 4G                                                                          *
#                          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0                               *
#                          target/scala-2.11/factdeposits_2.11-1.0.jar <FactSnapShot HDFS Path> <DimCustomer HDFS Path>                      * 
#                          <DimCalendar HDFS Path> <DimProduct HDFS Path> <DimScenario  HDFS Path> <PrmMain HDFS Path>                       *
#                          <BplData_norm HDFS Path> <parallelism config value> <shuffle partitions config value>                             *
#                          <storage memoryfraction config value> <maxresultsize config value> <shuffle spillafterread config value>          *
#                          <executor memoryoverhead config value>                                                                            *
#*********************************************************************************************************************************************


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

StoreGoalsCSVInput = sys.argv[1]
StoreGoalsParquetOutput = sys.argv[2]
StoreFileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("DimStoreGoalsCSVToParquet").getOrCreate()

storeGoals_DF = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(StoreGoalsCSVInput)

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')						
						
storeGoals_DF.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(StoreGoalsParquetOutput + '/' + todayyear + '/' + todaymonth + '/' + 'StoreGoalsConverted' + StoreFileTime);

	
spark.stop()