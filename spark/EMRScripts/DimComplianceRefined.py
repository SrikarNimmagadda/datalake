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
        appName("DimCompliance").getOrCreate()
		

CnCTrainingReportsInput = sys.argv[1]
DimEmpRefinedInput = sys.argv[2]
DimComplianceOutput = sys.argv[3]
DimEmpStoreAssInput = sys.argv[4]
StoreDealerCodeAss = sys.argv[5]
DealerCodes = sys.argv[6]

CnCTrainingReports_DF = spark.read.parquet(CnCTrainingReportsInput)

split_col = split(CnCTrainingReports_DF['lso_location'], '/')

CnCTrainingReports_DF = CnCTrainingReports_DF.withColumn('lso_loc_name', split_col.getItem(0))
CnCTrainingReports_DF = CnCTrainingReports_DF.withColumn('lso_loc_id', split_col.getItem(1))

CnCTrainingReports_DF.registerTempTable("CnCTrainingReports")

DimEmpRefined_DF = spark.read.parquet(DimEmpRefinedInput).registerTempTable("DimEmpRefined")






DimEmpStoreAssInput_DF = spark.read.parquet(DimEmpStoreAssInput).registerTempTable("DimEmpStoreAss")

StoreDealerCodeAss_DF = spark.read.parquet(StoreDealerCodeAss).registerTempTable("StoreDealerCodeAsso")

DealerCodes_DF = spark.read.parquet(DealerCodes).registerTempTable("DealerCodesTable")

			  
Final_Joined_DF = spark.sql("select distinct b.companycd as company_cd, ' ' as report_date, b.sourceemployeeid as source_employee_id, "
                          + "case when d.AssociationType = 'Retail' then e.DealerCode when d.AssociationType = 'SMF' then e.DealerCode  else ' ' end as dealer_code, "
                          + "' ' as spring_market, a.region as spring_region, a.district as spring_district, a.rq4_or_spring_u_name as employee_name, "
						  + "a.rq4_location as store_name, a.lso_loc_name as att_location_name, a.lso_loc_id as att_location_id, "
						  + "a.position job_title, a.pass_or_fail as pass_fail_indicator, a.compliance as compliance_completion, a.mit as mit_completion, "
						  + "a.ongoing as ongoing_training_completion, a.incomplete as training_completion_indicator, a.xmid as xmid "
                          + "from CnCTrainingReports a "
						  + "inner join DimEmpRefined b "
						  + "on a.rq4_or_spring_u_name = b.name "
						  + "inner join DimEmpStoreAss c "
						  + "on b.sourceemployeeid = c.sourceemployeeid "
						  + "inner join StoreDealerCodeAsso d "
						  + "on c.storenumber = d.StoreNumber "
						  + "inner join DealerCodesTable e "
						  + "on d.StoreNumber = e.TBLoc OR d.StoreNumber = e.SMFMapping")						  
						  
Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimComplianceOutput)

Final_Joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save("s3n://tb-us-east-1-dev-refined-regular/Employee/DimEmployeeComplianceCSV/");

spark.stop()