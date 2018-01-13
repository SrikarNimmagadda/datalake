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
StoreGoalsOutput = sys.argv[2]
FileTime = sys.argv[3]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################
 

StoreGoals_DF = spark.read.parquet(StoreGoalsInput).\
						select("Store", "District", "GrossProfit", "AdvTV", "DigitalLife", "AccGP_or_Opp", "CRUGA", "Tablets", "IntegratedProducts", "WTR", "GoPhone", "OTHours", "CloseRate", "AccessoryGP", "AccessoryAttachRate", "Traffic", "PPGAVAB", "AdvTVVAB", "DLifeVAB", "CRUVAB", "HSI").filter("Store != ' '")						
														 
														 
#StoreGoals_DF = spark.read.parquet("s3n://tb-us-east-1-dev-landing/ATT_Scorecard_Goals.csv").\
#						select("Store", "District", "GrossProfit", "AdvTV", "DigitalLife", "AccGP_or_Opp", "CRUGA", "Tablets", "IntegratedProducts", "WTR", "GoPhone", "OTHours", #"CloseRate", "AccessoryGP", "AccessoryAttachRate", "Traffic", "PPGAVAB", "AdvTVVAB", "DLifeVAB", "CRUVAB", "HSI").filter("Store != ' '")										

split_col = split(StoreGoals_DF['Store'], ' ')

StoreGoals_DF = StoreGoals_DF.withColumn('store_num', split_col.getItem(0))
StoreGoals_DF = StoreGoals_DF.withColumn('store_name', split_col.getItem(1))

StoreDropped_DF = StoreGoals_DF.drop("Store").registerTempTable("StoreGoalsTT")
				   

FlatteningStoreGoals_DF = spark.sql("select store_num, store_name, "
                          + "concat('District&',nvl(District,'0.00'),',','GrossProfit&',nvl(GrossProfit,'0.00'),',','AdvTV&',nvl(AdvTV,'0.00'),',','DigitalLife&',nvl(DigitalLife,'0.00'),',','AccGP_or_Opp&',nvl(AccGP_or_Opp,'0.00'),',','CRUGA&',nvl(CRUGA,'0.00'),',','Tablets&',nvl(Tablets,'0.00'),',','IntegratedProducts&',nvl(IntegratedProducts,'0.00'),',','WTR&',nvl(WTR,'0.00'),',','GoPhone&',nvl(GoPhone,'0.00'),',','OTHours&',nvl(OTHours,'0.00'),',','CloseRate&',nvl(CloseRate,'0.00'),',','AccessoryGP&',nvl(AccessoryGP,'0.00'),',','AccessoryAttachRate&',nvl(AccessoryAttachRate,'0.00'),',','Traffic&',nvl(Traffic,'0.00'),',','PPGAVAB&',nvl(PPGAVAB,'0.00'),',','AdvTVVAB&',nvl(AdvTVVAB,'0.00'),',','DLifeVAB&',nvl(DLifeVAB,'0.00'),',','CRUVAB&',nvl(CRUVAB,'0.00'),',','HSI&',nvl(HSI,'0.00')) as concatenated from StoreGoalsTT")
						  

mflatTableExplodedFrame_DF = FlatteningStoreGoals_DF.select("store_num", "store_name", explode(split(FlatteningStoreGoals_DF.concatenated, ",")).alias("Goal_ValueTemp"))


split_col2 = split(mflatTableExplodedFrame_DF['Goal_ValueTemp'], '&')

mflatTableExplodedFrame_DF = mflatTableExplodedFrame_DF.withColumn('KPI_Name', split_col2.getItem(0))
mflatTableExplodedFrame_DF = mflatTableExplodedFrame_DF.withColumn('Goal_Value', split_col2.getItem(1))

StoreGoalTransposed_DF = mflatTableExplodedFrame_DF.drop("Goal_ValueTemp")
						  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

StoreGoalTransposed_DF.coalesce(1).write.mode("overwrite").parquet(StoreGoalsOutput + '/' + todayyear + '/' + todaymonth + '/' + 'StoreGoalsTranspose' + FileTime)
						  
spark.stop()