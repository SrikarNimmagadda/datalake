#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimCustExpDelivery                                                                                               *
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

#*********************************************************************************************************************************************
# ______     _                  ______    _                           _________                                      _            __      _  *
#|_   _ `.  (_)               .' ____ \  / |_                        |  _   _  |                                    / \          |  ]    (_) *
#  | | `. \ __   _ .--..--.   | (___ \_|`| |-' .--.   _ .--.  .---.  |_/ | | \_|_ .--.  ,--.   _ .--.   .--.       / _ \     .--.| |     __  *
#  | |  | |[  | [ `.-. .-. |   _.____`.  | | / .'`\ \[ `/'`\]/ /__\\     | |   [ `/'`\]`'_\ : [ `.-. | ( (`\]     / ___ \  / /'`\' |    [  | *
# _| |_.' / | |  | | | | | |  | \____) | | |,| \__. | | |    | \__.,    _| |_   | |    // | |, | | | |  `'.'.   _/ /   \ \_| \__/  |  _  | | *
#|______.' [___][___||__||__]  \______.' \__/ '.__.' [___]    '.__.'   |_____| [___]   \'-;__/[___||__][\__) ) |____| |____|'.__.;__][ \_| | *
#                                                                                                                                     \____/ *
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

spark = SparkSession.builder.\
        appName("DimStoreTransAdjDelivery").getOrCreate()
		

DimStoreTransAdjRefinedInput = sys.argv[1]
DimCustExpRefinedOutput = sys.argv[2]

DimCustExpRefinedInput_DF = spark.read.parquet(DimStoreTransAdjRefinedInput).registerTempTable("DimStoreTransAdjRefinedTable")

Final_Joined_DF = spark.sql("select a.storenumber as STORE_NUM, a.companycd as CO_CD, a.report_date as RPT_DT, "
                          + "a.adjustment_type as ADJMNT_TYP, a.adjustment_category as ADJMNT_CAT, a.adjustment_amount as ADJMNT_AMT, a.sourcesystemname as SRC_SYS_NM "
						  + "from DimStoreTransAdjRefinedTable a")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

						  
Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimCustExpRefinedOutput);

spark.stop()