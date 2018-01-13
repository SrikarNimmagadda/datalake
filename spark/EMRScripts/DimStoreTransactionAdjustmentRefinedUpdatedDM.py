#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimStoreTransAdjRefined                                                                                          *
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
        appName("DimStoreTransAdjRefined").getOrCreate()
		

DimStoreTransAdjInput = sys.argv[1]
StoreRefined = sys.argv[2]
DimStoreTransAdjRefinedOutput = sys.argv[3]
FileTime = sys.argv[4]

#DimStoreTransAdjInput_DF = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Store/Transaction_Adjustment_Store/").registerTempTable("DimStoreTransAdjTT")
#StoreRefinedInput_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/09/StoreRefined201709280943/").registerTempTable("StoreRefinedTT")

DimStoreTransAdjInput_DF = spark.read.parquet(DimStoreTransAdjInput).registerTempTable("DimStoreTransAdjTT")
StoreRefined_DF = spark.read.parquet(StoreRefined).registerTempTable("StoreRefinedTT")

Final_Joined_DF = spark.sql("select b.Loc as storenumber, '4' as companycd, ' ' as report_date, b.Location as location_name, "
			 + "a.Market as spring_market, a.Region as spring_region, a.District as spring_district, ' ' as adjustment_type, ' ' as adjustment_category, "
                          + "b.Adjustment as adjustment_amount, 'RQ4' as sourcesystemname "
                          + "from StoreRefinedTT a "
						  + "inner join DimStoreTransAdjTT b "
						  + "on a.storenumber = b.Loc ")
						  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')
						  
Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimStoreTransAdjRefinedOutput + '/' + todayyear + '/' + todaymonth + '/' + 'DimStoreTransAdjRefined' + FileTime)

#Final_Joined_DF.coalesce(1).select("*"). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(DimStoreTransAdjRefinedOutput + '/' + todayyear + '/' + todaymonth + '/' + 'DimStoreTransAdjRefined' + "/CSV/");

spark.stop()