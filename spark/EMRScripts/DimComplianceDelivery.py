#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimEmployeeOperationalEfficiency                                                                                 *
#   Written By            : Aakash Basu                                                                                                      *
#   Date Written          : October 2017                                                                                                     *
#   Project Name          : GameStop                                                                                                         *
#   Version               : 1.0                                                                                                              *
#   Description           : This code is for retrieving the operational effiency of employees                                                *
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

#***********************************************************************************************************************************************************************
#                                                                                                                                                                      *
######   ###### #### ### ########  # ####  ######  ######  ######   # ####       ######   ######  #### ### #######  ####     ######  ######   # ####  ######  ######## *
#    ##  #    # #  ### # #      # #  ## # ##    ## #    # ##    ## #  ## #      ##    ## ##    ## #  ### # #     ## #  #     #    # ##    ## #  ## # ##    ## #      # *
#  #  ## ##  ## #   #  # #  ##### #   # # #  ##  # ##  ## #  ##  # #   # #      #  ##  # #  ##  # #   #  # #  ##  # #  #     ##  ## #  ##  # #   # # #  ##  # #  ##### *
#  ##  #  #  #  #      # #    #   #     # ##  ####  #  #  #  ##  # #     #      #  ##### #  ##  # #      # #     ## #  #      #  #  #      # #     # #  ##### #    #   *
#  ##  #  #  #  #  # # # #  ###   #  #  # ####  ##  #  #  #  ##  # #  #  #      #  ##### #  ##  # #  # # # #  ####  #  #      #  #  #  ##  # #  #  # #  ##### #  ###   *
#  #  ## ##  ## #  ### # #  ##### #  ## # #  ##  # ##  ## #  ##  # #  ## #      #  ##  # #  ##  # #  ### # #  #     #  ##### ##  ## #  ##  # #  ## # #  ##  # #  ##### *
#    ##  #    # #  # # # #      # #  ## # ##    ## #    # ##    ## #  ## #      ##    ## ##    ## #  # # # #  #     #      # #    # #  ##  # #  ## # ##    ## #      # *
######   ###### #### ### ######## #######  ######  ######  ######  #######       ######   ######  #### ### ####     ######## ###### ######## #######  ######  ######## *
#                                                                                                                                                                      *
#***********************************************************************************************************************************************************************

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
        appName("DimComplianceDelivery").getOrCreate()
		

DimComplianceRefinedInput = sys.argv[1]
DimComplianceDeliveryOut = sys.argv[2]

DimComplianceRefinedInput_DF = spark.read.parquet(DimComplianceRefinedInput).registerTempTable("DimComplianceRefined")


Final_Joined_DF = spark.sql("select a.company_cd CO_CD, a.report_date RPT_DATE, a.source_employee_id WRKDAY_ID, ' ' as STORE_NUM, a.pass_fail_indicator PASS_FAIL_IND, "
                          + "a.compliance_completion CMPLY_COMPLETION, a.mit_completion MIT_COMPLETION, a.ongoing_training_completion ONGO_TRAING_COMPLETION, "
                          + "a.training_completion_indicator TRAING_COMPLETION_IND, a.xmid XMID, 'I' as CDC_IND "
                          + "from DimComplianceRefined a")
						  
Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimComplianceDeliveryOut);

spark.stop()