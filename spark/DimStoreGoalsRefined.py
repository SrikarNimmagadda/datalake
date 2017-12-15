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
from pyspark.sql.functions import *


spark = SparkSession.builder.\
        appName("DimStoreGoalsTranspose").getOrCreate()

StoreGoalsInput = sys.argv[1]
StoreTransAdj = sys.argv[2]
StoreRefined = sys.argv[3]
StoreGoalsOutput = sys.argv[4]
FileTime = sys.argv[5]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################
 
														 
#StoreGoals_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/StoreGoalsTranspose2200/").registerTempTable("StoreGoalsTransposed_TT")

#StoreTransAdj_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/DimStoreTransAdjRefined1225/").registerTempTable("StoreTransAdj_TT")

#StoreRefined_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Store/2017/11/StoreRefined201711241001/").registerTempTable("StoreRefined_TT")
				   
				   
							 
StoreGoals_DF = spark.read.parquet(StoreGoalsInput).registerTempTable("StoreGoalsTransposed_TT")

StoreTransAdj_DF = spark.read.parquet(StoreTransAdj).registerTempTable("StoreTransAdj_TT")

StoreRefined_DF = spark.read.parquet(StoreRefined).registerTempTable("StoreRefined_TT")
				   

FinalStoreGoals_DF = spark.sql("select distinct ' ' as report_date, a.store_num, '4' as company_code, a.KPI_Name as kpi_name, a.Goal_Value as goal_value, "
                                  + "b.locationname, b.market as spring_market, b.region as spring_region, b.district as spring_district "
                                  + "from StoreGoalsTransposed_TT a "
						          + "inner join StoreRefined_TT b "
						          + "on a.store_num = b.storenumber "
						          + "inner join StoreTransAdj_TT c "
						          + "on a.store_num = c.Store_Number")

						  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

FinalStoreGoals_DF.coalesce(1).write.mode("overwrite").parquet(StoreGoalsOutput + '/' + todayyear + '/' + todaymonth + '/' + 'DimStoreGoalsRefined' + FileTime)
						  
spark.stop()