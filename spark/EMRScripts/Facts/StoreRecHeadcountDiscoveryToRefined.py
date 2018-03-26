from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3


class StoreRecHeadcountDiscoveryToRefined(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.inputWorkingPath = sys.argv[1]
        self.outputWorkingPath = sys.argv[2]
        self.CdcBucketName = self.outputWorkingPath[self.outputWorkingPath.index('tb'):].split("/")[0]
        self.discoveryBucketName = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[0]
        self.storeRecHeadcountName = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[1]
        self.workingName = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[2]
        self.outputPartitionPath = "s3://" + self.CdcBucketName + '/' + self.storeRecHeadcountName
        self.outputCSVPath = "s3://" + self.CdcBucketName + '/' + self.storeRecHeadcountName + "/csv"
        self.ErrorBucketName = sys.argv[3] + '/Refined'
        self.storeAssocName = "StoreDealerAssociation"
        self.storeName = "Store"

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.logger.info("prefixPath is " + prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        all_values_dict = {}
        req_values_dict = {}
        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified
        for k, v in all_values_dict.items():
            if 'part-0000' in k:
                req_values_dict[k] = v
        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.logger.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.logger.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        refinedBucketNode = self.s3.Bucket(name=self.CdcBucketName)

        lastUpdatedStoreDealerAssocFile = self.findLastModifiedFile(refinedBucketNode, self.storeAssocName, self.CdcBucketName)
        self.spark.read.parquet(lastUpdatedStoreDealerAssocFile).registerTempTable("StoreDealerCodeAssocTempTable")

        lastUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.storeName, self.CdcBucketName)

        self.spark.read.parquet(lastUpdatedStoreFile).registerTempTable("StoreTempTable")

        self.spark.read.parquet(self.inputWorkingPath).registerTempTable("StoreRecHCTempTable1")

        storeBadRecsDF = self.spark.sql("select * from StoreRecHCTempTable1 where store_name is null")

        if storeBadRecsDF.count() > 0:
            self.logger.info("There are Bad records, hence saving them to the error bucket")
            storeBadRecsDF.coalesce(1).write.mode("append").csv(self.ErrorBucketName, header=True)

        self.spark.sql("select split(store_name,' ')[0] as store_number,actual_headcount,"
                       "store_manager as store_managers_count,business_assistant_manager_count,"
                       "fulltime_equivalent_count,"
                       "parttime_equivalent_count,fulltime_floater_count,district_lead_sales_consultant_count,"
                       "mit_count,seasonal_count,"
                       "'4' as companycd,date as report_date,approved_headcount,"
                       "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                       "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                       " from StoreRecHCTempTable1").registerTempTable("StoreRecHCTempTable2")

        dfRecHCExpFinal = self.spark.sql("select report_date,cast(a.store_number as int) as store_number,a.companycd as companycd,"
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
                                         " and a.store_number is not null")

        dfRecHCExpFinal.coalesce(1). \
            select('report_date', 'store_number', 'companycd', 'dealer_code', 'approved_headcount', 'store_managers_count',
                   'business_assistant_manager_count', 'fulltime_equivalent_count', 'parttime_equivalent_count',
                   'fulltime_floater_count', 'district_lead_sales_consultant_count', 'mit_count', 'seasonal_count',
                   'actual_headcount', 'location_name', 'spring_market', 'spring_region', 'spring_district'). \
            write.mode("overwrite"). \
            format('parquet'). \
            save(self.outputWorkingPath)

        dfRecHCExpFinal.coalesce(1). \
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet'). \
            save(self.outputPartitionPath)
        # dfRecHCExpFinal.coalesce(1).write.mode("overwrite").csv(self.outputCSVPath, header=True)

        self.spark.stop()


if __name__ == "__main__":
    StoreRecHeadcountDiscoveryToRefined().loadRefined()
