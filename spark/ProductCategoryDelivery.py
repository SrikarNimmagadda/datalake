from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *

DimProductCategoryIn = sys.argv[1]
DimProductCategoryOut = sys.argv[2]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("LocationStore").getOrCreate()
        

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

DimProductCategoryIn_DF = spark.read.parquet(DimProductCategoryIn).registerTempTable("DimProductCategoryInTT")

#DimProductCategoryIn_DF = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular-qa/Product/2017/11/ProductCategory2220/").registerTempTable("DimProductCategoryInTT")

        
#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################

Final_joined_DF = spark.sql("select a.company_cd as CO_CD, a.category_Id as CAT_ID, a.category_name as CAT_NME, a.category_desc as CAT_DESC, a.parent_category_id as PARNT_CAT, "
                    + "a.level_one_id as LVL_1_ID, a.level_one_name as LVL_1_NME, a.level_two_id as LVL_2_ID, a.level_two_name as LVL_2_NME, a.level_three_id as LVL_3_ID, "
					+ "a.level_three_name as LVL_3_NME, a.level_four_id as LVL_4_ID, a.level_four_name as LVL_4_NME, a.level_five_id as LVL_5_ID, "
					+ "a.level_five_name as LVL_5_NME, a.level_six_id as LVL_6_ID, a.level_six_name as LVL_6_NME, a.level_seven_id as LVL_7_ID, a.level_seven_name as LVL_7_NME, "
                    + "a.level_eight_id as LVL_8_ID, a.level_eight_name as LVL_8_NME,  a.level_nine_id as LVL_9_ID, a.level_nine_name as LVL_9_NME, a.level_ten_id as LVL_10_ID, "
					+ "a.level_ten_name LVL_10_NME, 'I' CDC_IND_CD "
                    + "from DimProductCategoryInTT a")
					
Final_joined_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimProductCategoryOut);


spark.stop()