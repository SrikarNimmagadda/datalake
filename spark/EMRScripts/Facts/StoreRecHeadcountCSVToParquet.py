#!/usr/bin/python
#######################################################################################################################
# Author                : Harikesh R
# Date Created          : 02/07/2018
# Purpose               : To convert the csv file to parquet, move it to Discovery layer for Store Recruiting Headcount
# Version               : 1.0
# Revision History      :
#
########################################################################################################################

import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField
import boto3

s3 = boto3.resource('s3')

OutputPath = sys.argv[1]
InputPath = sys.argv[2]

Config = SparkConf().setAppName("StoreRecHeadcount_CSVToParquet")
SparkCtx = SparkContext(conf=Config)
spark = SparkSession.builder.config(conf=Config). \
    getOrCreate()

Log4jLogger = SparkCtx._jvm.org.apache.log4j
logger = Log4jLogger.LogManager.getLogger('store_rec_hc_csv_to_parquet')

fileHasDataFlag = 0
bkt = InputPath
my_bucket = s3.Bucket(name=bkt)

pfx = "StoreRecruitingHeadcount/Working"

SRHCSchema = StructType([StructField("Store Number", StringType()),
                         StructField("Store Name", StringType()),
                         StructField("Headcount", StringType()),
                         StructField("Store Manager", StringType()),
                         StructField("BAM 1 & 2", StringType()),
                         StructField("FTEC", StringType()),
                         StructField("PTEC", StringType()),
                         StructField("Other", StringType()),
                         StructField("DLSC", StringType()),
                         StructField("MIT", StringType()),
                         StructField("Seasonal", StringType()),
                         StructField("Approved HC", StringType())
                         ])


def checkFileIsCsv(prefixPath):
    fileValid = 0
    ext = ''
    partitionName = my_bucket.objects.filter(Prefix=prefixPath)
    for obj in partitionName:
        filepat = obj.key.split('/')[2]
        if filepat != '':
            ext = filepat.split('.')[1]
            logger.info("Extension of the file is : ")
            logger.info(ext)
        if ext == 'csv':
            fileValid = 1
    return fileValid


storeRecHCFileCheck = checkFileIsCsv("StoreRecruitingHeadcount/Working")

if storeRecHCFileCheck == 1:
    logger.info("Raw file is in csv format, proceeding with the logic")
    dfStoreRecHC = spark.read.format("com.databricks.spark.csv"). \
        option("header", "true"). \
        option("treatEmptyValuesAsNulls", "true"). \
        schema(SRHCSchema). \
        load('s3://' + InputPath + '/StoreRecruitingHeadcount/Working')

    dfStoreRecHCCnt = dfStoreRecHC.count()

    if dfStoreRecHCCnt > 1:
        logger.info("The store recruiting headcount file has data")
        fileHasDataFlag = 1
    else:
        logger.info("The store recruiting headcount file does not have data")
        fileHasDataFlag = 0

    if fileHasDataFlag == 1:
        logger.info("Csv file loaded into dataframe properly")

        dfStoreRecHCRenamed = dfStoreRecHC.withColumnRenamed("Store Number", "store_number"). \
            withColumnRenamed("Store Name", "store_name"). \
            withColumnRenamed("Headcount", "actual_headcount"). \
            withColumnRenamed("Store Manager", "store_manager"). \
            withColumnRenamed("BAM 1 & 2", "business_assistant_manager_count"). \
            withColumnRenamed("FTEC", "fulltime_equivalent_count"). \
            withColumnRenamed("PTEC", "parttime_equivalent_count"). \
            withColumnRenamed("Other", "fulltime_floater_count"). \
            withColumnRenamed("DLSC", "district_lead_sales_consultant_count"). \
            withColumnRenamed("MIT", "mit_count"). \
            withColumnRenamed("Seasonal", "seasonal_count"). \
            withColumnRenamed("Approved HC", "approved_headcount"). \
            registerTempTable("StoreRecHCTempTable")

        dfStoreRecHCFinal = spark.sql(
            "select store_number,store_name,actual_headcount,store_manager,business_assistant_manager_count,"
            + "fulltime_equivalent_count,parttime_equivalent_count,fulltime_floater_count,"
            + "district_lead_sales_consultant_count,mit_count,seasonal_count,approved_headcount,"
            + "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
            + " from StoreRecHCTempTable "
        )

        todayyear = datetime.now().strftime('%Y')
        todaymonth = datetime.now().strftime('%m')

        dfStoreRecHCFinal.coalesce(1).select('store_number', 'store_name', 'actual_headcount', 'store_manager',
                                             'business_assistant_manager_count', 'fulltime_equivalent_count',
                                             'parttime_equivalent_count', 'fulltime_floater_count',
                                             'district_lead_sales_consultant_count', 'mit_count', 'seasonal_count',
                                             'approved_headcount'). \
            write.mode("overwrite"). \
            parquet(OutputPath + '/' + 'StoreRecruitingHeadcount' + '/' + 'Working')

        dfStoreRecHCFinal.coalesce(1). \
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet').save(OutputPath + '/' + 'StoreRecruitingHeadcount')

    else:
        logger.error("ERROR : Loading csv file into dataframe")

else:
    logger.error("ERROR : Raw file is not in csv format")

spark.stop()
