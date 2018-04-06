from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, substring, year
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

        self.inputWorkingPath = sys.argv[1]
        self.OutputWorkingPath = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/Refined/'

        self.discoveryBucket = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[0]
        self.refinedBucket = self.OutputWorkingPath[self.OutputWorkingPath.index('tb'):].split("/")[0]
        self.storeCustExpName = self.OutputWorkingPath[self.OutputWorkingPath.index('tb'):].split("/")[1]
        self.storeCustExpPartitionPath = 's3://' + self.refinedBucket + '/' + self.storeCustExpName

        self.attDealerCodeName = 'ATTDealerCode'
        self.storeDealerAssoc = 'StoreDealerAssociation'
        self.storeName = 'Store'
        self.regExForChar = "[^\d]"

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

        refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

        lastUpdatedAttDealerFile = self.findLastModifiedFile(refinedBucketNode, self.attDealerCodeName, self.refinedBucket)
        self.spark.read.parquet(lastUpdatedAttDealerFile).registerTempTable("ATTDealerCodeTempTable")

        storeDlrCdAssoclastModifiedFileName = self.findLastModifiedFile(refinedBucketNode, self.storeDealerAssoc, self.refinedBucket)
        self.spark.read.parquet(storeDlrCdAssoclastModifiedFileName).registerTempTable("StoreDealerCodeAssocTempTable")

        storeModifiedFileName = self.findLastModifiedFile(refinedBucketNode, self.storeName, self.refinedBucket)
        self.spark.read.parquet(storeModifiedFileName).registerTempTable("StoreTempTable")

        self.spark.read.parquet(self.inputWorkingPath).registerTempTable("StoreCustExpTempTable1")

        self.spark.sql("select location,reverse(split(reverse(location),' ')[0]) as dealer_code,"
                       "cast(cast(regexp_replace(five_key_behaviors,'%','') as float)/100 as decimal(8,4)) as five_key_behaviors, "
                       "cast(cast(regexp_replace(effective_solutioning,'%','') as float)/100 as decimal(8,4)) as effective_solutioning,"
                       "cast(cast(regexp_replace(integrated_experience,'%','') as float)/100 as decimal(8,4)) as integrated_experience,"
                       "'4' as companycd,report_date from StoreCustExpTempTable1").registerTempTable("StoreCustExpTempTable2")

        dfStoreCustExpFull = self.spark.sql("select cast(c.StoreNumber as int) as store_number,five_key_behaviors,effective_solutioning,integrated_experience,"
                                            "b.attmarket as att_market,a.dealer_code as dealer_code,report_date,companycd,"
                                            "b.attlocationname as att_location_name ,b.attregion as att_region"
                                            " from StoreCustExpTempTable2 a "
                                            " LEFT OUTER JOIN ATTDealerCodeTempTable b"
                                            " on a.dealer_code = b.dealercode"
                                            " LEFT OUTER JOIN StoreDealerCodeAssocTempTable c"
                                            " on a.dealer_code = c.DealerCode"
                                            " where c.AssociationType = 'Retail' and c.AssociationStatus = 'Active'")

        self.logger.info("Exception Handling of Store Customer Experience Refine starts")

        dfStoreCustExpErrorData = dfStoreCustExpFull.filter(dfStoreCustExpFull["store_number"].rlike(self.regExForChar))

        dfStoreCustExpFull.subtract(dfStoreCustExpErrorData).registerTempTable("StoreCustExpTempTable3")

        if (dfStoreCustExpErrorData is not None) and (dfStoreCustExpErrorData.count() > 0):
            self.logger.info("Store Customer Experience has erroneous records having invalid store number.")
            dfStoreCustExpErrorData.coalesce(1).write.mode("append").csv(self.dataProcessingErrorPath, header=True)

        self.logger.info("Exception Handling of Store Customer Experience Refine Ends")

        dfStoreCustExpFinal = self.spark.sql("select report_date,store_number,a.companycd as companycd,b.LocationName as location_name,"
                                             "b.SpringDistrict as spring_district,b.SpringRegion as spring_region,"
                                             "b.SpringMarket as spring_market,dealer_code,att_location_name,att_region,att_market,"
                                             "five_key_behaviors,effective_solutioning,integrated_experience"
                                             " from StoreCustExpTempTable3 a"
                                             " LEFT OUTER JOIN StoreTempTable b"
                                             " on a.store_number = b.StoreNumber").drop_duplicates()

        dfStoreCustExpFinal.coalesce(1).write.mode("overwrite").format('parquet').save(self.OutputWorkingPath)

        dfStoreCustExpFinal.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').\
            save(self.storeCustExpPartitionPath)

        self.spark.stop()


if __name__ == "__main__":
    StoreCustExpDiscoveryToRefined().loadRefined()
