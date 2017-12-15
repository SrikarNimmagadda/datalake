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
		

DimCnCRefinedInput = sys.argv[1]
DimCnCDeliveryOutput = sys.argv[2]

DimCnCRefinedInput_DF = spark.read.parquet(DimCnCRefinedInput).registerTempTable("DimCnCRefinedTable")

DimCnCRefinedInput_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/StoreCnCRefined201711071756/").registerTempTable("DimCnCRefinedTable")

Final_Joined_DF = spark.sql("select a.storenumber STORE_NUM, a.dealer_codes DLR_CD, a.companycd CO_CD, a.report_date as RPT_DT,  "
                          + "a.compliance_completion CMPLY_CMPLT, a.mit_completion as MIT_CMPLT, a.ongoing_completion as ONGO_CMPLT, "
                          + "a.overall_completion as OVR_CMPLT, a.pass_fail_indicator as PASS_FAIL_IND "
						  + "from DimCnCRefinedTable a")

Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimCnCDeliveryOutput);

spark.stop()