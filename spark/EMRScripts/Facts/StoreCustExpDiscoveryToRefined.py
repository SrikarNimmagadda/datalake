from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3


class StoreCustExpDiscoveryToRefined(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.OutputPath = sys.argv[1]
        self.CdcBucketName = sys.argv[2]
        self.StoreCustExpInputPath = sys.argv[3]

    def loadRefined(self):
        todayyear = datetime.now().strftime('%Y')

        bkt = self.CdcBucketName
        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}

        pfx = "ATTDealerCode/year=" + todayyear
        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.logger.info("Files are : ")
        self.logger.info(revSortedFiles)

        numFiles = len(revSortedFiles)
        self.logger.info("Number of part files for ATT Dealer code is : ")
        self.logger.info(numFiles)

        if numFiles > 0:
            self.logger.info("ATT Dealer code Files found in Refined layer\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            ATTlastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.logger.info("Last Modified file is : ")
            self.logger.info(ATTlastModifiedFileName)
            self.logger.info("\n")

            self.spark.read.parquet(ATTlastModifiedFileName).registerTempTable("ATTDealerCodeTempTable")
        else:
            self.logger.info("No file found for ATT Dealer Codes in Refined layer, lookup will not succeed")

        #######################################################################################################
        # Finding last modified Store Dealer Code Association file in Refined layer to perform lookup

        bkt = self.CdcBucketName
        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}

        pfx = "StoreDealerAssociation/year=" + todayyear
        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.logger.info("Files are : ")
        self.logger.info(revSortedFiles)

        numFiles = len(revSortedFiles)
        self.logger.info("Number of part files for Store Dealer Code Association is : ")
        self.logger.info(numFiles)

        if numFiles > 0:
            self.logger.info("Store Dealer Code Association Files found in Refined layer\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            StoreDlrCdAssoclastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.logger.info("Last Modified file is : ")
            self.logger.info(StoreDlrCdAssoclastModifiedFileName)
            self.logger.info("\n")

            self.spark.read.parquet(StoreDlrCdAssoclastModifiedFileName).registerTempTable("StoreDealerCodeAssocTempTable")
        else:
            self.logger.info("No file found for Store Dealer Code Association in Refined layer, lookup will not succeed")

        # Finding last modified Store file in Refined layer to perform lookup

        bkt = self.CdcBucketName
        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}

        pfx = "Store/year=" + todayyear
        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.logger.info("Files are : ")
        self.logger.info(revSortedFiles)

        numFiles = len(revSortedFiles)
        self.logger.info("Number of part files for Store is : ")
        self.logger.info(numFiles)

        if numFiles > 0:
            self.logger.info("Store Files found in Refined layer\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            StoreModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.logger.info("Last Modified Store file is : ")
            self.logger.info(StoreModifiedFileName)
            self.logger.info("\n")

            self.spark.read.parquet(StoreModifiedFileName).registerTempTable("StoreTempTable")
        else:
            self.logger.info("No file found for Store in Refined layer, lookup will not succeed")
        self.spark.read.parquet(self.StoreCustExpInputPath).registerTempTable("StoreCustExpTempTable1")

        self.spark.sql("select location,reverse(split(reverse(location),' ')[0]) as dealer_code,"
                       "cast(cast(regexp_replace(five_key_behaviours,'%','') as float)/100 as decimal(8,4)) as five_key_behaviours, "
                       "cast(cast(regexp_replace(effective_solutioning,'%','') as float)/100 as decimal(8,4)) as effective_solutioning,"
                       "cast(cast(regexp_replace(integrated_experience,'%','') as float)/100 as decimal(8,4)) as integrated_experience,"
                       "'4' as companycd,report_date,"
                       "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                       "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                       "from StoreCustExpTempTable1").registerTempTable("StoreCustExpTempTable2")

        self.spark.sql("select cast(c.StoreNumber as int) as store_number,five_key_behaviours,effective_solutioning,integrated_experience,"
                       "b.attmarket as att_market,a.dealer_code as dealer_code,report_date,companycd,"
                       "b.attlocationname as att_location_name ,b.attregion as att_region,year,month"
                       " from StoreCustExpTempTable2 a "
                       " LEFT OUTER JOIN ATTDealerCodeTempTable b"
                       " on a.dealer_code = b.dealercode"
                       " LEFT OUTER JOIN StoreDealerCodeAssocTempTable c"
                       " on a.dealer_code = c.DealerCode"
                       " where c.AssociationType = 'Retail' and c.AssociationStatus = 'Active'").registerTempTable("StoreCustExpTempTable3")

        dfStoreCustExpFinal = self.spark.sql("select report_date,store_number,a.companycd as companycd,b.LocationName as location_name,"
                                             "b.SpringDistrict as spring_district,b.SpringRegion as spring_region,"
                                             "b.SpringMarket as spring_market,dealer_code,att_location_name,att_region,att_market,"
                                             "five_key_behaviours,effective_solutioning,integrated_experience,year,month"
                                             " from StoreCustExpTempTable3 a"
                                             " LEFT OUTER JOIN StoreTempTable b"
                                             " on a.store_number = b.StoreNumber")

        dfStoreCustExpFinal.coalesce(1).\
            select('report_date', 'store_number', 'companycd', 'location_name', 'spring_district', 'spring_region', 'spring_market', 'dealer_code', 'att_location_name', 'att_region', 'att_market', 'five_key_behaviours', 'effective_solutioning', 'integrated_experience').\
            write.mode("overwrite").\
            format('parquet').\
            save(self.OutputPath + '/' + 'StoreCustomerExperience' + '/' + 'Working')

        dfStoreCustExpFinal.coalesce(1).write.mode('append').partitionBy('year', 'month').\
            format('parquet').save(self.OutputPath + '/' + 'StoreCustomerExperience')

        self.spark.stop()


if __name__ == "__main__":
    StoreCustExpDiscoveryToRefined().loadRefined()
