#!/usr/bin/python
#######################################################################################################################
# Author           : Harikesh R
# Date Created     : 02/07/2018
# Purpose          : To transform and move data from Discovery to Refined layer for Store Recruiting Headcount
# Version          : 1.0
# Revision History :
#
########################################################################################################################

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
from datetime import datetime
import boto3

s3 = boto3.resource('s3')
client = boto3.client('s3')

Config = SparkConf().setAppName("StoreRecHeadcountRefinary")
SparkCtx = SparkContext(conf=Config)
spark = SparkSession.builder.config(conf=Config). \
    getOrCreate()

Log4jLogger = SparkCtx._jvm.org.apache.log4j
logger = Log4jLogger.LogManager.getLogger('store_rec_hc_disc_to_refined')

OutputPath = sys.argv[1]
CdcBucketName = sys.argv[2]
ErrorBucketName = sys.argv[3]
StoreRecHCInputPath = sys.argv[4]

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

ErrorTimestamp = datetime.now().strftime('%Y-%m-%d')
StoreHCErrorFile = 's3://' + ErrorBucketName + '/StoreRecHeadcount/' + ErrorTimestamp

#######################################################################################################
# Finding last modified Store Dealer Code Association file in Refined layer to perform lookup

bkt = CdcBucketName
my_bucket = s3.Bucket(name=bkt)

all_values_dict = {}
req_values_dict = {}

pfx = "StoreDealerAssociation/year=" + todayyear
partitionName = my_bucket.objects.filter(Prefix=pfx)

for obj in partitionName:
    all_values_dict[obj.key] = obj.last_modified

for k, v in all_values_dict.items():
    if 'part-0000' in k:
        req_values_dict[k] = v

revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
logger.info("Files are : ")
logger.info(revSortedFiles)

numFiles = len(revSortedFiles)
logger.info("Number of part files for Store Dealer Code Association is : ")
logger.info(numFiles)

if numFiles > 0:
    logger.info("Store Dealer Code Association Files found in Refined layer\n")
    lastModifiedFileNameTmp = str(revSortedFiles[0])
    StoreDlrCdAssoclastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
    logger.info("Last Modified file is : ")
    logger.info(StoreDlrCdAssoclastModifiedFileName)
    logger.info("\n")

    dfStoreDealerCodeAssocFile = spark.read.parquet(StoreDlrCdAssoclastModifiedFileName). \
        registerTempTable("StoreDealerCodeAssocTempTable")
else:
    logger.info("No file found for Store Dealer Code Association in Refined layer, lookup will not succeed")

#######################################################################################################

# Finding last modified Store file in Refined layer to perform lookup

bkt = CdcBucketName
my_bucket = s3.Bucket(name=bkt)

all_values_dict = {}
req_values_dict = {}

pfx = "Store/year=" + todayyear
partitionName = my_bucket.objects.filter(Prefix=pfx)

for obj in partitionName:
    all_values_dict[obj.key] = obj.last_modified

for k, v in all_values_dict.items():
    if 'part-0000' in k:
        req_values_dict[k] = v

revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
logger.info("Files are : ")
logger.info(revSortedFiles)

numFiles = len(revSortedFiles)
logger.info("Number of part files for Store is : ")
logger.info(numFiles)

if numFiles > 0:
    logger.info("Store Files found in Refined layer\n")
    lastModifiedFileNameTmp = str(revSortedFiles[0])
    StoreModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
    logger.info("Last Modified Store file is : ")
    logger.info(StoreModifiedFileName)
    logger.info("\n")

    dfStoreDealerCodeAssocFile = spark.read.parquet(StoreModifiedFileName). \
        registerTempTable("StoreTempTable")
else:
    logger.info("No file found for Store in Refined layer, lookup will not succeed")

#######################################################################################################

dfStoreRecHCFile = spark.read.parquet(StoreRecHCInputPath + '/StoreRecruitingHeadcount/Working').registerTempTable(
    "StoreRecHCTempTable1")

storeBadRecsDF = spark.sql("select * from StoreRecHCTempTable1 "
                           + " where store_name is null"
                           )

if storeBadRecsDF.count() > 0:
    logger.info("There are Bad records, hence saving them to the error bucket")
    storeBadRecsDF.coalesce(1).write.mode("append").csv(StoreHCErrorFile, header=True)

    dfRecHCExp1 = spark.sql("select split(store_name,' ')[0] as store_number,actual_headcount,"
                            "store_manager as store_managers_count,business_assistant_manager_count,"
                            "fulltime_equivalent_count,"
                            "parttime_equivalent_count,fulltime_floater_count,district_lead_sales_consultant_count,"
                            "mit_count,seasonal_count,"
                            "'4' as companycd,FROM_UNIXTIME(UNIX_TIMESTAMP(),'MM/dd/yyyy') as report_date,"
                            "approved_headcount,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                            "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                            " from StoreRecHCTempTable1"
                            ).registerTempTable("StoreRecHCTempTable2")

    dfRecHCExpFinal = spark.sql("select report_date,cast(a.store_number as int) as store_number,a.companycd as "
                                "companycd,"
                                "c.DealerCode as dealer_code,store_managers_count,business_assistant_manager_count,"
                                "fulltime_equivalent_count,parttime_equivalent_count,fulltime_floater_count,"
                                "district_lead_sales_consultant_count,mit_count,seasonal_count,"
                                "actual_headcount,b.LocationName as location_name,"
                                "b.SpringMarket as spring_market,b.SpringRegion as spring_region,"
                                "b.SpringDistrict as spring_district,approved_headcount,year,month"
                                " from StoreRecHCTempTable2 a "
                                " LEFT OUTER JOIN StoreTempTable b"
                                " on a.store_number = b.StoreNumber"
                                " LEFT OUTER JOIN StoreDealerCodeAssocTempTable c"
                                " on a.store_number = c.StoreNumber"
                                " where c.AssociationType = 'Retail' and c.AssociationStatus = 'Active'"
                                " and a.store_number is not null"
                                )

    dfRecHCExpFinal.coalesce(1). \
        select('report_date', 'store_number', 'companycd', 'dealer_code', 'approved_headcount', 'store_managers_count',
               'business_assistant_manager_count', 'fulltime_equivalent_count', 'parttime_equivalent_count',
               'fulltime_floater_count', 'district_lead_sales_consultant_count', 'mit_count', 'seasonal_count',
               'actual_headcount', 'location_name', 'spring_market', 'spring_region', 'spring_district'). \
        write.mode("overwrite"). \
        format('parquet'). \
        save(OutputPath + '/' + 'StoreRecruitingHeadcount' + '/' + 'Working')

    dfRecHCExpFinal.coalesce(1). \
        write.mode('append').partitionBy('year', 'month'). \
        format('parquet'). \
        save(OutputPath + '/' + 'StoreRecruitingHeadcount')

########################################################################################################################

spark.stop()
