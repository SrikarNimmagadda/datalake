#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimStoreCnCRefined                                                                                                *
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

spark = SparkSession.builder.\
        appName("DimStoreCnCRefined").getOrCreate()
		

SpringCustExpInput = sys.argv[1]
ATTDealerCodeRefine = sys.argv[2]
StoreRefinedIn = sys.argv[3]
StoreDealerCodeAssoIn = sys.argv[4]
DimCnCRefinedOutput = sys.argv[5]

#StoreCnCInput_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/DimStoreCnC_StoreDashboard/")
#
#split_col = split(StoreCnCInput_DF['region_district_store'], ' ')
#
#StoreCnCInput_DF = StoreCnCInput_DF.withColumn('store_num', split_col.getItem(0))
#StoreCnCInput_DF = StoreCnCInput_DF.withColumn('att_loc_name', split_col.getItem(1))
#
#StoreCnCInput_DF.registerTempTable("StoreCnCInput")
#
#ATTDealerCodeRefined_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/09/ATTDealerCodeRefine201709271440/").registerTempTable("ATTDealerCodeRefined")
#
#StoreRefined_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/09/StoreRefined201709280943/").registerTempTable("StoreRefinedTT")
#
#StoreDealerAsso_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/09/StoreDealerAssociationRefine201709291421/").\
#                             filter("AssociationType != 'Retail'").registerTempTable("StoreDealerCodeAsso")
							 
							 
StoreCnCInput_DF = spark.read.parquet(SpringCustExpInput)

split_col = split(StoreCnCInput_DF['region_district_store'], ' ')

StoreCnCInput_DF = StoreCnCInput_DF.withColumn('store_num', split_col.getItem(0))
StoreCnCInput_DF = StoreCnCInput_DF.withColumn('att_loc_name', split_col.getItem(1))

StoreCnCInput_DF.registerTempTable("StoreCnCInput")

ATTDealerCodeRefined_DF = spark.read.parquet(ATTDealerCodeRefine).registerTempTable("ATTDealerCodeRefined")

StoreRefined_DF = spark.read.parquet(StoreRefinedIn).registerTempTable("StoreRefinedTT")

StoreDealerAsso_DF = spark.read.parquet(StoreDealerCodeAssoIn).\
                             filter("AssociationType != 'Retail'").registerTempTable("StoreDealerCodeAsso")


Final_Joined_DF = spark.sql("select a.StoreNumber as store_number, b.DealerCode as dealer_codes, "
                          + "case when a.CompanyCode = 'SpringMobile' then '4' when a.CompanyCode = 'Spring' then '4'  else ' ' end as company_cd, "
						  + "' ' as report_date, a.Market as spring_market, a.Region as spring_region, a.District as spring_district, c.annual_compliance_courses as compliance_completion, "
                          + "c.monthly_initiative_trainings as mit_completion, c.ongoing_learning_plan as ongoing_completion, c.overall_audit as overall_completion, "
						  + "c.pass_or_fail as pass_fail_indicator "
                          + "from StoreRefinedTT a "
						  + "inner join StoreCnCInput c "
						  + "on a.StoreNumber = c.store_num "
						  + "inner join StoreDealerCodeAsso d "
						  + "on a.StoreNumber = d.StoreNumber "
						  + "inner join ATTDealerCodeRefined b "
						  + "on b.DealerCode = d.DealerCode")
						  
						  
Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(DimCnCRefinedOutput)

spark.stop()

spark.stop()