from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf
from pyspark.sql.functions import *


spark = SparkSession.builder.\
    appName("DimStoreTransactionAdjTranspose").getOrCreate()

StoreTransactionAdjInput = sys.argv[1]
StoreRefined = sys.argv[2]
StoreTransactionAdjOutput = sys.argv[3]
FileTime = sys.argv[4]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################


StoreTransactionAdj_DF = spark.read.parquet(StoreTransactionAdjInput)
#						select("Store", "District", "GrossProfit", "AdvTV", "DigitalLife", "AccGP_or_Opp", "CRUGA", "Tablets", "IntegratedProducts", "WTR", "GoPhone", "OTHours", "CloseRate", "AccessoryGP", "AccessoryAttachRate", "Traffic", "PPGAVAB", "AdvTVVAB", "DLifeVAB", "CRUVAB", "HSI").filter("Store != ' '")


# StoreTransactionAdj_DF = spark.read.parquet("s3n://tb-us-east-1-dev-landing/ATT_Scorecard_Goals.csv").\
#						select("Store", "District", "GrossProfit", "AdvTV", "DigitalLife", "AccGP_or_Opp", "CRUGA", "Tablets", "IntegratedProducts", "WTR", "GoPhone", "OTHours", #"CloseRate", "AccessoryGP", "AccessoryAttachRate", "Traffic", "PPGAVAB", "AdvTVVAB", "DLifeVAB", "CRUVAB", "HSI").filter("Store != ' '")

split_col = split(StoreTransactionAdj_DF['location'], ' ')

StoreTransactionAdj_DF = StoreTransactionAdj_DF.withColumn('store_num', split_col.getItem(0))
StoreTransactionAdj_DF = StoreTransactionAdj_DF.withColumn('location_name', split_col.getItem(1))


StoreDropped_DF = StoreTransactionAdj_DF.drop("location").registerTempTable("StoreTransactionAdjTT")


FlatteningStoreTransactionAdj_DF = spark.sql("select store_num, location_name, "
                                             + "concat('featurescommission_a&',nvl(featurescommission_a,'0.00'),',','featurescommission_b&',nvl(featurescommission_b,'0.00'),',',"
                                             + "'featurecommission_c&',nvl(featurecommission_c,'0.00'),',','featurescommission_d&',nvl(featurescommission_d,'0.00'),',',"
                                             + "'featurescommission_e&',nvl(featurescommission_e,'0.00'),',','features&',nvl(features,'0.00'),',','percentilefeatures&',nvl(percentilefeatures,'0.00'),',',"
                                             + "'directtvcommission_a&',nvl(directtvcommission_a,'0.00'),',','directtvcommission_b&',nvl(directtvcommission_b,'0.00'),',',"
                                             + "'directtvcommission_c&',nvl(directtvcommission_c,'0.00'),',','directtvcommission_d&',nvl(directtvcommission_d,'0.00'),',',"
                                             + "'directtvcommission_e&',nvl(directtvcommission_e,'0.00'),',','directtv&',nvl(directtv,'0.00'),',','percentiledirecttv&',nvl(percentiledirecttv,'0.00'),',',"
                                             + "'wiredcommission_a&',nvl(wiredcommission_a,'0.00'),',','wiredcommission_b&',nvl(wiredcommission_b,'0.00'),',',"
                                             + "'wiredcommission_c&',nvl(wiredcommission_c,'0.00'),',','wiredcommission_d&',nvl(wiredcommission_d,'0.00'),',',"
                                             + "'wiredcommission_e&',nvl(wiredcommission_e,'0.00'),',','wired&',nvl(wired,'0.00'),',','percentilewired&',nvl(percentilewired,'0.00'),',',"
                                             + "'tire&',nvl(tire,'0.00'),',','activationupgrade&',nvl(activationupgrade,'0.00'),',','plan&',nvl(plan,'0.00'),',','prepaid&',nvl(prepaid,'0.00'),',',"
                                             + "'march_17&',nvl(march_17,'0.00'),',','March2017Payback&',nvl(March2017Payback,'0.00'),',','total&',nvl(total,'0.00'),',',"
                                             + "'accessories&',nvl(accessories,'0.00'),',','spifadjustment&',nvl(spifadjustment,'0.00'),',',"
                                             + "'accrued&',nvl(accrued,'0.00'),',','adjustment&',nvl(adjustment,'0.00')) as concatenated from StoreTransactionAdjTT")


mflatTableExplodedFrame_DF = FlatteningStoreTransactionAdj_DF.select("store_num", "location_name", explode(
    split(FlatteningStoreTransactionAdj_DF.concatenated, ",")).alias("adj_amt_temp"))

split_col2 = split(mflatTableExplodedFrame_DF['adj_amt_temp'], '&')

mflatTableExplodedFrame_DF = mflatTableExplodedFrame_DF.withColumn(
    'adjustment_category', split_col2.getItem(0))
#mflatTableExplodedFrame_DF = mflatTableExplodedFrame_DF.withColumn('adjustment_type', 'MISC')
mflatTableExplodedFrame_DF = mflatTableExplodedFrame_DF.withColumn(
    'adjustment_amount', split_col2.getItem(1))

StoreTransactionAdjTransposed_DF = mflatTableExplodedFrame_DF.drop("adj_amt_temp")
StoreTransactionAdjTransposed_DF.show()

#############################################
#			Joining with Store Refine
#############################################

StoreRefined_DF = spark.read.parquet(StoreRefined).registerTempTable("StoreRefinedTT")
DimStoreTransAdjRefine_DF = StoreTransactionAdjTransposed_DF.registerTempTable("DimStoreTransAdjTT")

Final_Joined_DF = spark.sql("select date_sub(current_timestamp(), 1) as reportdate, b.store_num as storenumber, b.adjustment_category as adjustment_category, "
                            + "'MISC' as adjustment_type, '4' as companycd, b.adjustment_amount as adjustment_amount, b.location_name as location_name, "
                            + "a.Market as spring_market, a.Region as spring_region, a.District as spring_district, 'RQ4' as sourcesystemname "
                            + "from StoreRefinedTT a "
                            + "inner join DimStoreTransAdjTT b "
                            + "on a.storenumber = b.store_num ")

Final_Joined_DF.show()

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

Final_Joined_DF.coalesce(1).select("*"). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").save(StoreTransactionAdjOutput)

Final_Joined_DF.coalesce(1).write.mode("overwrite").parquet(StoreTransactionAdjOutput +
                                                            '/' + todayyear + '/' + todaymonth + '/' + 'StoreTransactionAdjTranspose' + FileTime)

spark.stop()
