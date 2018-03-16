from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3


class StoreRecHeadcountDiscoveryToRefined(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.outputPath = sys.argv[1]
        self.cdcBucketName = sys.argv[2]
        self.errorBucketName = sys.argv[3]
        self.storeRecHCInputPath = sys.argv[4]

        self.errorTimestamp = datetime.now().strftime('%Y-%m-%d')
        self.storeHCErrorFile = 's3://' + self.errorBucketName + '/StoreRecHeadcount/' + self.errorTimestamp

        #######################################################################################################
        # Finding last modified Store Dealer Code Association file in Refined layer to perform lookup

    def loadRefined(self):

        todayyear = datetime.now().strftime('%Y')

        bkt = self.cdcBucketName
        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}
        numFiles = 0

        pfx = "StoreDealerAssociation/year=" + todayyear
        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.log.info("Files are : ")
        self.log.info(revSortedFiles)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files for Store Dealer Code Association is : ")
        self.log.info(numFiles)

        if numFiles > 0:
            self.log.info("Store Dealer Code Association Files found in Refined layer\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            StoreDlrCdAssoclastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.log.info("Last Modified file is : ")
            self.log.info(StoreDlrCdAssoclastModifiedFileName)
            self.log.info("\n")

            self.sparkSession.read.parquet(StoreDlrCdAssoclastModifiedFileName).registerTempTable("StoreDealerCodeAssocTempTable")
        else:
            self.log.info("No file found for Store Dealer Code Association in Refined layer, lookup will not succeed")

        #######################################################################################################

        # Finding last modified Store file in Refined layer to perform lookup

        bkt = self.cdcBucketName
        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}
        numFiles = 0

        pfx = "Store/year=" + todayyear
        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.log.info("Files are : ")
        self.log.info(revSortedFiles)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files for Store is : ")
        self.log.info(numFiles)

        if numFiles > 0:
            self.log.info("Store Files found in Refined layer\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            StoreModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.log.info("Last Modified Store file is : ")
            self.log.info(StoreModifiedFileName)
            self.log.info("\n")

            self.sparkSession.read.parquet(StoreModifiedFileName).registerTempTable("StoreTempTable")
        else:
            self.log.info("No file found for Store in Refined layer, lookup will not succeed")

        #######################################################################################################

        self.sparkSession.read.parquet(self.storeRecHCInputPath + '/StoreRecruitingHeadcount/Working').registerTempTable("StoreRecHCTempTable1")

        storeBadRecsDF = self.sparkSession.sql("select * from StoreRecHCTempTable1 where store_name is null")

        if (storeBadRecsDF.count() > 0):
            self.log.info("There are Bad records, hence saving them to the error bucket")
            storeBadRecsDF.coalesce(1).write.mode("append").csv(self.storeHCErrorFile, header=True)

            self.sparkSession.sql("select split(store_name,' ')[0] as store_number, actual_headcount,"
                                  "store_manager as store_managers_count, business_assistant_manager_count,"
                                  "fulltime_equivalent_count,"
                                  "parttime_equivalent_count,fulltime_floater_count,district_lead_sales_consultant_count,"
                                  "mit_count,seasonal_count,"
                                  "'4' as companycd,FROM_UNIXTIME(UNIX_TIMESTAMP(),'MM/dd/yyyy') as report_date,approved_headcount,"
                                  "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                  "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                                  " from StoreRecHCTempTable1").registerTempTable("StoreRecHCTempTable2")

            dfRecHCExpFinal = self.sparkSession.sql("select report_date,cast(a.store_number as int) as store_number,a.companycd as companycd,"
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
                save(self.outputPath + '/' + 'StoreRecruitingHeadcount' + '/' + 'Working')

            dfRecHCExpFinal.coalesce(1). \
                write.mode('append').partitionBy('year', 'month'). \
                format('parquet'). \
                save(self.outputPath + '/' + 'StoreRecruitingHeadcount')

        ########################################################################################################################

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreRecHeadcountDiscoveryToRefined().loadRefined()
