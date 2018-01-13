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

#****************************************************************************************************************************
#                                                                                                                           *
######   ###### #### ###    ######## #### ### #######      ######  #######  ######## #######     ######## ######## ######## *
#    ##  #    # #  ### #    #      # #  ### # #     ##    ##    ## #     ## #      # #     ##    #      # #     #  #     #  *
#  #  ## ##  ## #   #  #    #  ##### #   #  # #  ##  #    #  ##  # #  ##  # #  ##### #  ##  #    #  ##### #  ####  #  ####  *
#  ##  #  #  #  #      #    #    #   #      # #     ##    #  ##  # #     ## #    #   #     ##    #    #   #    #   #    #   *
#  ##  #  #  #  #  # # #    #  ###   #  # # # #  ####     #  ##  # #  ####  #  ###   #    ##     #  ###   #  ###   #  ###   *
#  #  ## ##  ## #  ### #    #  ##### #  ### # #  #        #  ##  # #  #     #  ##### #  #  ##    #  ##### #  #     #  #     *
#    ##  #    # #  # # #    #      # #  # # # #  #        ##    ## #  #     #      # #  ##  #    #      # #  #     #  #     *
######   ###### #### ###    ######## #### ### ####         ######  ####     ######## ########    ######## ####     ####     *
#                                                                                                                           *
#****************************************************************************************************************************

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
        appName("DimEmployeeOperationalEfficiencyDelivery").getOrCreate()
		

OperationalEfficiencyInput = sys.argv[1]
DimEmpOperEffOutput = sys.argv[2]

DimOpEff_DF = spark.read.parquet(OperationalEfficiencyInput).registerTempTable("OperationalEfficiency")

Final_Joined_DF = spark.sql("select distinct a.report_date RPT_DATE, a.company_cd CO_CD, a.source_employee_id SRC_EMP_ID, a.total_loss_amount TTL_LOSS_AMT, 'RQ4' as SRC_SYS_NM, "
                       + "a.total_issues_count TTL_ISUS_CNT, a.action_taken ACTN_TAKN, a.hr_consultation_indicator HR_CNSLTATION_IND, a.transaction_errors_amount TRANS_ERRS_AMT, "
					   + "a.transaction_errors_count TRANS_ERRS_CNT, a.next_trades_amount NXT_TRDS_AMT, a.next_trades_device_count NXT_TRDS_DVC_CNT, "
					   + "a.hyla_loss_amount HYLA_LOSS_AMT, a.hyla_device_count HYLA_DVC_CNT, a.denied_rma_devices_amount DNY_RMA_DVC_AMT, "
					   + "a.denied_rma_devices_count DNY_RMA_DVC_CNT, a.cash_deposits_amount CSH_DPSTS_AMT, "
					   + "a.total_missing_deposits_count TTL_MISSING_DPSTS_CNT, a.total_short_deposits_count TTL_SHRT_DPSTS_CNT, a.shrinkage_amount SHRNKAGE_AMT, "
                       + "a.loss_comments LOSS_CMNT, a.store_number as STORE_NUM, ' ' as EDW_BATCH_ID "
					   + "from OperationalEfficiency a")

Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimEmpOperEffOutput);

spark.stop()