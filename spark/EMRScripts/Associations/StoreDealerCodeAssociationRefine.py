from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, when, from_unixtime, unix_timestamp, year, substring, hash, lit


class StoreDealerCodeAssociationRefine(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryBucketWorking = sys.argv[1]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.refinedBucketWorking = sys.argv[2]
        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.storeDealerAssociationName = self.refinedBucketWorking[
                                          self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[2]

        self.storeAssociationWorkingPath = 's3://' + self.refinedBucket + '/' + self.storeDealerAssociationName + '/' \
                                           + self.workingName
        self.storeAssociationPartitonPath = 's3://' + self.refinedBucket + '/' + self.storeDealerAssociationName
        self.prefixDealerDiscoveryPath = 'Store/Dealer'
        self.prefixStoreAssocPath = self.storeDealerAssociationName

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.log.info("prefixPath is " + prefixPath)
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
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3n://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefine(self):

        s3 = boto3.resource('s3')
        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedDealerFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDealerDiscoveryPath,
                                                          self.discoveryBucket)
        dfDealerCode = self.sparkSession.read.parquet(lastUpdatedDealerFile)

        dfDealerCode.withColumnRenamed("Company", "CompanyCode").\
            withColumnRenamed("DF", "AssociationType").withColumnRenamed("DCstatus", "AssociationStatus")\
            .registerTempTable("Dealer")

        dfStoreAss = self.sparkSession.sql("select a.DealerCode,a.CompanyCode,a.AssociationType, a.AssociationStatus,"
                                           "a.TBLoc,a.SMFMapping,a.RankDescription from Dealer a where "
                                           "a.RankDescription='Open Standard' or a.RankDescription='SMF Only' or "
                                           "a.RankDescription='Bulk' or a.RankDescription Like '%Close%' or "
                                           "a.RankDescription Like 'Close%'")

        dfStoreAss = dfStoreAss.where(col('CompanyCode').like("Spring%"))
        dfStoreAsstemp = dfStoreAss
        dfOpenFil = dfStoreAss.filter(dfStoreAss.RankDescription != 'SMF Only')

#########################################################################################################
# Transformation starts #
#########################################################################################################

        dfStoreAssOpen = dfStoreAsstemp.\
            withColumn('StoreNumber', when((dfStoreAsstemp.RankDescription != 'SMF Only'), dfStoreAsstemp.TBLoc).
                       otherwise(dfStoreAsstemp.SMFMapping)).withColumn('dealercode1', dfStoreAsstemp.DealerCode).\
            withColumn('AssociationType1', when((dfStoreAsstemp.RankDescription != 'SMF Only'), 'Retail').
                       otherwise('SMF')).\
            withColumn('AssociationStatus1', when((dfStoreAsstemp.RankDescription == 'SMF Only')
                                                  | (dfStoreAsstemp.RankDescription == 'Open Standard')
                                                  | (dfStoreAsstemp.RankDescription == 'Bulk'), 'Active').
                       otherwise('Closed')).drop(dfStoreAsstemp.DealerCode).\
            drop(dfStoreAsstemp.AssociationType).drop(dfStoreAsstemp.AssociationStatus).\
            select(col('StoreNumber'), col('dealercode1').alias('DealerCode'),
                   col('AssociationType1').alias('AssociationType'),
                   col('AssociationStatus1').alias('AssociationStatus'), col('TBLoc'), col('SMFMapping'),
                   col('RankDescription'), col('CompanyCode'))

        dfStoreAssOpen.registerTempTable("storeAss1")

#########################################################################################################
# Code for new entry fields for Open Standard #
#########################################################################################################

        dfOpenFilNewField = dfOpenFil.withColumn('StoreNumber', dfOpenFil.SMFMapping). \
            withColumn('dealercode1', dfOpenFil.DealerCode). \
            withColumn('AssociationType1', lit('SMF')). \
            withColumn('AssociationStatus1', lit('Active')). \
            drop(dfOpenFil.DealerCode). \
            drop(dfOpenFil.AssociationType). \
            drop(dfOpenFil.AssociationStatus). \
            select(col('StoreNumber'), col('dealercode1').alias('DealerCode'), col('AssociationType1').
                   alias('AssociationType'),
                   col('AssociationStatus1').alias('AssociationStatus'),
                   col('TBLoc'), col('SMFMapping'), col('RankDescription'), col('CompanyCode'))

        dfOpenFilNewField.registerTempTable("storeAss2")

#########################################################################################################
# Code for union of two dataframes#
#########################################################################################################

        joined_DF = self.sparkSession.sql("select cast(StoreNumber as integer),DealerCode, 4 as CompanyCode,"
                                          + "AssociationType,AssociationStatus from storeAss1 union select StoreNumber"
                                          + ",DealerCode,4 as CompanyCode,AssociationType,AssociationStatus "
                                          + "from storeAss2")

        joined_DF.registerTempTable("store_assoc_source")
        self.sparkSession.sql("select StoreNumber, DealerCode, CompanyCode, AssociationType, AssociationStatus from"
                              + " store_assoc_source ").withColumn("Hash_Column",
                                                                   hash("StoreNumber", "DealerCode", "CompanyCode",
                                                                        "AssociationType", "AssociationStatus")).\
            registerTempTable("store_assoc_curr")

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)
        storeAssocPrevRefinedPath = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreAssocPath,
                                                              self.refinedBucket)

        if storeAssocPrevRefinedPath != '':
            self.sparkSession.read.parquet(storeAssocPrevRefinedPath).\
                withColumn("Hash_Column", hash("StoreNumber", "DealerCode", "CompanyCode", "AssociationType",
                                               "AssociationStatus")).\
                registerTempTable("store_assoc_prev")

            self.sparkSession.sql("select a.StoreNumber, a.DealerCode, a.CompanyCode, a.AssociationType, "
                                  + "a.AssociationStatus from store_assoc_prev a left join store_assoc_curr b on "
                                  + "a.StoreNumber = b.StoreNumber where a.Hash_Column = b.Hash_Column").\
                registerTempTable("store_assoc_no_change_data")

            dfStoreUpdated = self.sparkSession.sql("select a.StoreNumber, a.DealerCode, a.CompanyCode, "
                                                   + "a.AssociationType, a.AssociationStatus from store_assoc_curr a"
                                                   + " left join store_assoc_prev b on a.StoreNumber = b.StoreNumber"
                                                   + " where a.Hash_Column <> b.Hash_Column")
            updateRowsCount = dfStoreUpdated.count()
            dfStoreUpdated.registerTempTable("store_assoc_updated_data")

            dfStoreNew = self.sparkSession.sql("select a.StoreNumber, a.DealerCode, a.CompanyCode, a.AssociationType,"
                                               + " a.AssociationStatus from store_assoc_curr a left join "
                                               + "store_assoc_prev b on a.StoreNumber = b.StoreNumber where "
                                               + "b.StoreNumber = null")
            newRowsCount = dfStoreNew.count()
            dfStoreNew.registerTempTable("store_assoc_new_data")

            if updateRowsCount > 0 or newRowsCount > 0:
                dfStoreWithCDC = self.sparkSession.sql("select StoreNumber, DealerCode, CompanyCode, AssociationType, "
                                                       + "AssociationStatus from store_assoc_no_change_data union "
                                                       + "select StoreNumber, DealerCode, CompanyCode, AssociationType,"
                                                       + " AssociationStatus from store_assoc_updated_data union "
                                                       + "select StoreNumber, DealerCode, CompanyCode, AssociationType,"
                                                       + " AssociationStatus from store_assoc_new_data")
                self.log.info("Updated file has arrived..")
                dfStoreWithCDC.coalesce(1).write.mode("overwrite").parquet(self.storeAssociationWorkingPath)
                dfStoreWithCDC.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
                    withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
                    write.mode("append").partitionBy('year', 'month').format('parquet').\
                    save(self.storeAssociationPartitonPath)
            else:
                self.log.info(" The prev and current files are same. So no file will be generated in refined bucket.")
        else:
            self.log.info(" This is the first transaformation call, So keeping the file in refined bucket.")
            joined_DF.coalesce(1).write.mode("overwrite").parquet(self.storeAssociationWorkingPath)
            joined_DF.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
                withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
                write.mode('append').partitionBy('year', 'month').format('parquet').\
                save(self.storeAssociationPartitonPath)
            self.sparkSession.stop()


if __name__ == "__main__":
    StoreDealerCodeAssociationRefine().loadRefine()
