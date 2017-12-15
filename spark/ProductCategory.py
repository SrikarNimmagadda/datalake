#*********************************************************************************************************************************************
#   SPARK Python Pgm Name : Product Category                                                                                                 *
#   Written By            : Soumi Basu                                                                                                       *
#   Date Written          : October 2017                                                                                                     *
#   Project Name          : GameStop                                                                                                         *
#   Version               : 1.0                                                                                                              *
#   Description           : This code is developed to categorize products as per their levels.                                               *
#   Change                : None																                                             *
#   History 	          : None																                                             *          
#   NOTES		          : Complete                                                                                                         *   
#	Spark Submit Command  : spark-submit --class spark.DimProductCategory --master yarn-client --num-executors 30 --executor-cores 4         *
#                          --executor-memory 24G --driver-memory 4G                                                                          *
#                          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0                               *
#                          target/scala-2.11/factdeposits_2.11-1.0.jar <FactSnapShot HDFS Path> <DimCustomer HDFS Path>                      *
#*********************************************************************************************************************************************

#************************************************************************************************************************************************************************************
#                                                                                                                                                                                   *
#__/\\\\\\\\\\\\_________________________________________/\\\\\\\\\\\\\_______________________________________/\\\___________________/\\\\\\\\\______________________________       * 
#_\/\\\////////\\\______________________________________\/\\\/////////\\\____________________________________\/\\\________________/\\\////////_______________________________       * 
# _\/\\\______\//\\\__/\\\_______________________________\/\\\_______\/\\\____________________________________\/\\\______________/\\\/______________________________/\\\______      * 
#  _\/\\\_______\/\\\_\///_____/\\\\\__/\\\\\_____________\/\\\\\\\\\\\\\/___/\\/\\\\\\\______/\\\\\___________\/\\\_____________/\\\______________/\\\\\\\\\_____/\\\\\\\\\\\_     * 
#   _\/\\\_______\/\\\__/\\\__/\\\///\\\\\///\\\___________\/\\\/////////____\/\\\/////\\\___/\\\///\\\____/\\\\\\\\\____________\/\\\_____________\////////\\\___\////\\\////__    * 
#    _\/\\\_______\/\\\_\/\\\_\/\\\_\//\\\__\/\\\___________\/\\\_____________\/\\\___\///___/\\\__\//\\\__/\\\////\\\____________\//\\\______________/\\\\\\\\\\_____\/\\\______   * 
#     _\/\\\_______/\\\__\/\\\_\/\\\__\/\\\__\/\\\___________\/\\\_____________\/\\\_________\//\\\__/\\\__\/\\\__\/\\\_____________\///\\\___________/\\\/////\\\_____\/\\\_/\\__  * 
#      _\/\\\\\\\\\\\\/___\/\\\_\/\\\__\/\\\__\/\\\___________\/\\\_____________\/\\\__________\///\\\\\/___\//\\\\\\\/\\______________\////\\\\\\\\\_\//\\\\\\\\/\\____\//\\\\\___ * 
#       _\////////////_____\///__\///___\///___\///____________\///______________\///_____________\/////______\///////\//__________________\/////////___\////////\//______\/////____*
#************************************************************************************************************************************************************************************


from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace


# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("productCategoryRefine").getOrCreate()


ProdCategoryInp = sys.argv[1]
ProdCategoryOP = sys.argv[2]

ProdCategoryInp_DF = spark.read.parquet("s3n://tb-us-east-1-dev-discovery-regular/Product/2017/10/ProductCategory201710301545/")

ProdCategoryInp_DF = ProdCategoryInp_DF.withColumn("desc", regexp_replace("description", "([0-9]). ", "")).drop('description')

ProdCategoryInp_DF.registerTempTable("ProdCategoryTempTable")
			
####################################################################################################################
#                                           Final Spark Transformaions                                             #
####################################################################################################################
   
FinalJoin_DF = spark.sql("select 'Spring Mobile' as company_cd, a.id as category_Id, a.desc as category_name, a.desc as category_desc, "
                          + "a.Parentid as parent_category_id, "
                          + "case when length(a.id)/2 = '1' then a.id else ' ' end as level_one_id, "
						  + "case when length(a.id)/2 = '1' then a.desc else ' ' end as level_one_name, "
						  + "case when length(a.id)/2 = '2' then a.id else ' ' end as level_two_id, "
						  + "case when length(a.id)/2 = '2' then a.desc else ' ' end as level_two_name, "
                          + "case when length(a.id)/2 = '3' then a.id else ' ' end as level_three_id, "
                          + "case when length(a.id)/2 = '3' then a.desc else ' ' end as level_three_name, "
                          + "case when length(a.id)/2 = '4' then a.id else ' ' end as level_four_id, "
                          + "case when length(a.id)/2 = '4' then a.desc else ' ' end as level_four_name, "
                          + "case when length(a.id)/2 = '5' then a.id else ' ' end as level_five_id, "
                          + "case when length(a.id)/2 = '5' then a.desc else ' ' end as level_five_name, "
                          + "case when length(a.id)/2 = '6' then a.id else ' ' end as level_six_id, "
						  + "case when length(a.id)/2 = '6' then a.desc else ' ' end as level_six_name, "
						  + "case when length(a.id)/2 = '7' then a.id else ' ' end as level_seven_id, "
                          + "case when length(a.id)/2 = '7' then a.desc else ' ' end as level_seven_name, "
                          + "case when length(a.id)/2 = '8' then a.id else ' ' end as level_eight_id, "
						  + "case when length(a.id)/2 = '8' then a.desc else ' ' end as level_eight_name, "
                          + "case when length(a.id)/2 = '9' then a.id else ' ' end as level_nine_id, "
						  + "case when length(a.id)/2 = '9' then a.desc else ' ' end as level_nine_name, "
						  + "case when length(a.id)/2 = '10' then a.id else ' ' end as level_ten_id, "
                          + "case when length(a.id)/2 = '10' then a.desc else ' ' end as level_ten_name "
						  + "from ProdCategoryTempTable a")


FinalJoin_DF.coalesce(1).select("*"). \
write.parquet(ProdCategoryOP);
                
spark.stop()
