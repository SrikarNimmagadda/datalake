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

#***********************************************************************************************************************************************************************************
#                                                                                                                                                                                  *
#__/\\\\\\\\\\\\_______________________________________________/\\\\\\\\\_____________________________________________________/\\\\\\\\\\\\\\\_____________________________        *
# _\/\\\////////\\\__________________________________________/\\\////////_____________________________________________________\/\\\///////////______________________________       *
#  _\/\\\______\//\\\__/\\\_________________________________/\\\/__________________________________________/\\\________________\/\\\_____________________________/\\\\\\\\\__      *
#   _\/\\\_______\/\\\_\///_____/\\\\\__/\\\\\______________/\\\______________/\\\____/\\\__/\\\\\\\\\\__/\\\\\\\\\\\___________\/\\\\\\\\\\\______/\\\____/\\\__/\\\/////\\\_     *
#    _\/\\\_______\/\\\__/\\\__/\\\///\\\\\///\\\___________\/\\\_____________\/\\\___\/\\\_\/\\\//////__\////\\\////____________\/\\\///////______\///\\\/\\\/__\/\\\\\\\\\\__    *
#     _\/\\\_______\/\\\_\/\\\_\/\\\_\//\\\__\/\\\___________\//\\\____________\/\\\___\/\\\_\/\\\\\\\\\\____\/\\\________________\/\\\_______________\///\\\/____\/\\\//////___   *
#      _\/\\\_______/\\\__\/\\\_\/\\\__\/\\\__\/\\\____________\///\\\__________\/\\\___\/\\\_\////////\\\____\/\\\_/\\____________\/\\\________________/\\\/\\\___\/\\\_________  *
#       _\/\\\\\\\\\\\\/___\/\\\_\/\\\__\/\\\__\/\\\______________\////\\\\\\\\\_\//\\\\\\\\\___/\\\\\\\\\\____\//\\\\\_____________\/\\\\\\\\\\\\\\\__/\\\/\///\\\_\/\\\_________ *
#        _\////////////_____\///__\///___\///___\///__________________\/////////___\/////////___\//////////______\/////______________\///////////////__\///____\///__\///__________*
#                                                                                                                                                                                  *
#***********************************************************************************************************************************************************************************


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
        appName("DimCompliance").getOrCreate()
		

DimCustExpRefinedInput = sys.argv[1]
DimCustExpRefinedOutput = sys.argv[2]

DimCustExpRefinedInput_DF = spark.read.parquet(DimCustExpRefinedInput).registerTempTable("DimCustExpRefinedTable")

Final_Joined_DF = spark.sql("select a.storenumber STORE_NUM, a.companycd CO_CD, a.traffic_time TRAFFIC_TM, a.traffic_date TRAFFIC_DT, "
                          + "a.report_date RPT_DT, a.source_system_name SRC_SYS_NM, a.source_system_location_id SRC_SYS_LOC_ID, "
                          + "a.traffic_type TRAFFIC_TYP, a.traffic_count TRAFFIC_CNT "
						  + "from DimCustExpRefinedTable a")

						  
Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimCustExpRefinedOutput);

spark.stop()