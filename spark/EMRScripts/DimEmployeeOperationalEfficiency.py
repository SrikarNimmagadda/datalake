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
        appName("LocationStore").getOrCreate()
		

OperationalEfficiencyScoreCard = sys.argv[1]
DimEmpStoreAssInput = sys.argv[2]
DimStoreRefinedInput = sys.argv[3]
DimEmployeeOperationalEfficiencyOutput = sys.argv[4]

DimOpEff_DF = spark.read.parquet(OperationalEfficiencyScoreCard)

DimEmpStoreAss_DF = spark.read.parquet(DimEmpStoreAssInput).registerTempTable("DimEmpStoreAss")

DimStoreRefined_DF = spark.read.parquet(DimStoreRefinedInput).registerTempTable("DimStoreRefined")

split_col = split(DimOpEff_DF['location'], ' ')

DimOpEff_DF = DimOpEff_DF.withColumn('att_loc_id', split_col.getItem(0))
DimOpEff_DF = DimOpEff_DF.withColumn('att_loc_name', split_col.getItem(1))

DimOpEff_DF.registerTempTable("DimOperEff")

Final_Joined_DF = spark.sql("select ' ' as report_date, b.CompanyCode as company_cd, b.sourceemployeeid as source_employee_id, c.StoreName as store_name, a.market as spring_market, "
                       + "a.region as spring_region, a.district as spring_district, a.att_loc_name as att_location_name, a.att_loc_id as att_location_id, "
					   + "a.sales_person as employee_name, a.total_loss as total_loss_amount, a.total_issues as  total_issues_count, a.action_taken as action_taken, "
					   + "a.hr_consulted_before_termination as hr_consultation_indicator, a.transaction_errors as transaction_errors_amount, "
					   + "a.total_errors as transaction_errors_count, a.next_trades as next_trades_amount, a.total_devices1 as next_trades_device_count, "
					   + "a.hyla_loss as hyla_loss_amount, a.total_devices2 as hyla_device_count, a.denied_rma_devices as denied_rma_devices_amount, "
					   + "a.total_device3 as denied_rma_devices_count, a.cash_deposits as cash_deposits_amount, a.total_missing_deposits as total_missing_deposits_count, "
					   + "a.total_short_deposits as total_short_deposits_count, a.shrinkage as shrinkage_amount, a.comments as loss_comments "
					   + "from DimOperEff a "
					   + "inner join DimEmpStoreAss b "
					   + "on a.att_loc_id = b.StoreNumber "
					   + "inner join DimStoreRefined c "
					   + "on b.StoreNumber = c.StoreNumber")

#Final_Joined_DF.coalesce(1).select("*"). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save("s3n://tb-us-east-1-dev-refined-regular/Employee/DimEmpOperEff/CSV/")

Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimEmployeeOperationalEfficiencyOutput)

spark.stop()