#!/usr/bin/python
#######################################################################################################################
# Author           : Harikesh R
# Date Created     : 12/01/2018
# Purpose          : To convert the csv file to parquet and move it to Discovery layer for Product Category
# Version          : 1.0
# Revision History :
#   Version          : 1.1
#   Revision History : Included CDC logic
########################################################################################################################

from pyspark.sql import SparkSession
import sys
from datetime import datetime

OutputPath = sys.argv[1]
ProdCatInputPath = sys.argv[2]

spark = SparkSession.builder. \
    appName("ProductCategory_CSVToParquet").getOrCreate()

# logger = log4j_logger.LogManager.getLogger('prod_cat_csv_to_parquet')

fileHasDataFlag = 0

dfProductCatg = spark.read.format("com.databricks.spark.csv"). \
    option("header", "true"). \
    option("treatEmptyValuesAsNulls", "true"). \
    option("inferSchema", "true"). \
    load(ProdCatInputPath)

dfProductCatg = dfProductCatg.withColumnRenamed("ID", "id"). \
    withColumnRenamed("Description", "description"). \
    withColumnRenamed("CategoryPath", "categorypath"). \
    withColumnRenamed("ParentID", "parentid"). \
    withColumnRenamed("Enabled", "enabled")

if dfProductCatg.count() > 1:
    print("The product category file has data")
    fileHasDataFlag = 1
else:
    print("The product category file does not have data")
    fileHasDataFlag = 0

if fileHasDataFlag == 1:
    print("Csv file loaded into dataframe properly")

    todayyear = datetime.now().strftime('%Y')
    todaymonth = datetime.now().strftime('%m')

    dfProductCatgWithDupsRem = dfProductCatg.dropDuplicates(['id']).registerTempTable("ProdCatTempTable")

    dfProductCatgFinal = spark.sql("select cast(id as string),description,categorypath,parentid,enabled,"
                                   " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                   "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdCatTempTable  "
                                   "where id is not null")

    dfProductCatgFinal.coalesce(1). \
        select('id', 'description', 'categorypath', 'parentid', 'enabled'). \
        write.mode("overwrite"). \
        format('parquet'). \
        save(OutputPath + '/' + 'ProductCategory' + '/' + 'Working')

    dfProductCatgFinal.coalesce(1). \
        write.mode('append').partitionBy('year', 'month'). \
        format('parquet').\
        save(OutputPath + '/' + 'ProductCategory')

else:
    print("ERROR : Loading csv file into dataframe")

spark.stop()
