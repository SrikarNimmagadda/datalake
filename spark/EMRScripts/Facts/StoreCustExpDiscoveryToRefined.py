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
        self.attDealerCodeName = 'ATTDealerCode'
        self.storeDealerAssoc = 'StoreDealerAssociation'
        self.storeName = 'Store'

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

        lastUpdatedAttDealerFile = self.findLastModifiedFile(refinedBucketNode, self.attDealerCodeName, self.CdcBucketName)
        self.spark.read.parquet(lastUpdatedAttDealerFile).registerTempTable("ATTDealerCodeTempTable")
        StoreDlrCdAssoclastModifiedFileName = self.findLastModifiedFile(refinedBucketNode, self.storeDealerAssoc, self.CdcBucketName)
        self.spark.read.parquet(StoreDlrCdAssoclastModifiedFileName).registerTempTable("StoreDealerCodeAssocTempTable")
        StoreModifiedFileName = self.findLastModifiedFile(refinedBucketNode, self.storeName, self.CdcBucketName)
        self.spark.read.parquet(StoreModifiedFileName).registerTempTable("StoreTempTable")

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
                                             " on a.store_number = b.StoreNumber").drop_duplicates()

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
