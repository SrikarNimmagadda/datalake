#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimEmpCnCCSVtoParquet                                                                                            *
#   Written By            : Aakash Basu                                                                                                      *
#   Date Written          : November 2017                                                                                                     *
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

#********************************************************************************************
#                                                                                           *
# ______     _                 ________                          ______            ______   *
#|_   _ `.  (_)               |_   __  |                       .' ___  |         .' ___  |  *
#  | | `. \ __   _ .--..--.     | |_ \_| _ .--..--.  _ .--.   / .'   \_| _ .--. / .'   \_|  *
#  | |  | |[  | [ `.-. .-. |    |  _| _ [ `.-. .-. |[ '/'`\ \ | |       [ `.-. || |         *
# _| |_.' / | |  | | | | | |   _| |__/ | | | | | | | | \__/ | \ `.___.'\ | | | |\ `.___.'\  *
#|______.' [___][___||__||__] |________|[___||__||__]| ;.__/   `.____ .'[___||__]`.____ .'  *
#                                                   [__|                                    *
#********************************************************************************************


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

spark = SparkSession.builder.\
        appName("DimEmpCnCCSVtoParquet").getOrCreate()

DimEmpCnCCSVtoParquetInput = sys.argv[1]
DimEmpCnCCSVtoParquetOutput = sys.argv[2]
FileTime = sys.argv[3]

        
DimEmpCnCCSVtoParquet_DF = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load("s3n://tb-us-east-1-dev-raw-regular/tb-us-east-1-dev-raw-emr/Store/CnC_Training_Report.csv").\
						withColumnRenamed("RQ4 Location", "RQ4_Location")
						
split_col = split(DimEmpCnCCSVtoParquet_DF['RQ4_Location'], ' ')

DimEmpCnCCSVtoParquet_DF = DimEmpCnCCSVtoParquet_DF.withColumn('LocNo', split_col.getItem(0))
DimEmpCnCCSVtoParquet_DF = DimEmpCnCCSVtoParquet_DF.withColumn('LocationName', split_col.getItem(1))
						
DimEmpCnCCSVtoParquet_DF = DimEmpCnCCSVtoParquet_DF.\
						   select("Position", "Pass/Fail", "Complance", "MIT", "Ongoing", "Incomplete", "XMID", "LocNo")


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')
						
DimEmpCnCCSVtoParquet_DF.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(DimEmpCnCCSVtoParquetOutput + '/' + todayyear + '/' + todaymonth + '/' + 'DimEmpCnCCSV' + FileTime);

spark.stop()