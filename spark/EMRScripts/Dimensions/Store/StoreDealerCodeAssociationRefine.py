from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, when, from_unixtime, unix_timestamp, year, substring


class StoreDealerCodeAssociationRefine(object):

    def __init__(self):

        self.discoveryBucketWithS3 = sys.argv[1]
        self.discoveryBucket = self.discoveryBucketWithS3[self.discoveryBucketWithS3.index('tb-us'):]
        self.refinedBucketWithS3 = sys.argv[2]
        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.storeAssociationWorkingPath = 's3://' + self.refinedBucket + '/StoreDealerAssociation/Working'
        self.storeAssociationPartitonPath = 's3://' + self.refinedBucket + '/StoreDealerAssociation'
        self.prefixDealerDiscoveryPath = 'Store/Dealer'

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        print("prefixPath is ", prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        all_values_dict = {}
        req_values_dict = {}
        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified
        for k, v in all_values_dict.iteritems():
            if 'part-0000' in k:
                req_values_dict[k] = v
        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        print("Number of part files is : ", numFiles)
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3n://" + bucket + "/" + lastModifiedFileName
            print("Last Modified ", prefixType, " file in s3 format is : ", lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefine(self):

        spark = SparkSession.builder.appName("StoreDealerCodeAssociationRefine").getOrCreate()

        s3 = boto3.resource('s3')
        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedDealerFile = self.findLastModifiedFile(discoveryBucketNode, self.prefixDealerDiscoveryPath,
                                                          self.discoveryBucket)
        dfDealerCode = spark.read.parquet(lastUpdatedDealerFile).withColumn("year",
                                                                            year(from_unixtime(unix_timestamp()))). \
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2))

        dfDealerCode = dfDealerCode.filter((col("TBLoc") != 0) & (col("SMFMapping") != 0))

        dfStoreAss = dfDealerCode.withColumnRenamed("Company", "CompanyCode").\
            withColumnRenamed("DF", "AssociationType").withColumnRenamed("DCstatus", "AssociationStatus").\
            registerTempTable("Dealer")

        dfStoreAss = spark.sql("select a.DealerCode,a.CompanyCode,a.AssociationType,"
                               + "a.AssociationStatus,a.TBLoc,a.SMFMapping,a.RankDescription from Dealer a "
                               + "where a.RankDescription='Open Standard' or a.RankDescription='SMF Only' or "
                               + "a.RankDescription='Bulk' or a.RankDescription Like '%Close%' or "
                               + "a.RankDescription Like 'Close%'")

        dfStoreAss = dfStoreAss.where(col('CompanyCode').like("Spring%"))
        dfStoreAsstemp = dfStoreAss
        dfOpenFil = dfStoreAss.filter(dfStoreAss.RankDescription == 'SMF Only')

#########################################################################################################
# Transformation starts #
#########################################################################################################

        dfStoreAssOpen = dfStoreAsstemp.\
            withColumn('StoreNumber', when((dfStoreAsstemp.RankDescription != 'SMF Only'), dfStoreAsstemp.TBLoc).
                       otherwise(dfStoreAsstemp.SMFMapping)).withColumn('dealercode1', dfStoreAsstemp.DealerCode).\
            withColumn('AssociationType1', when((dfStoreAsstemp.RankDescription != 'SMF Only'), 'Retail').
                       otherwise('SMF')).\
            withColumn('AssociationStatus1', when((dfStoreAsstemp.RankDescription == 'SMF Only') |
                                                     (dfStoreAsstemp.RankDescription == 'Open Standard') |
                                                     (dfStoreAsstemp.RankDescription == 'Bulk'), 'Active').
                       otherwise('Closed')).drop(dfStoreAsstemp.DealerCode).\
            drop(dfStoreAsstemp.AssociationType).drop(dfStoreAsstemp.AssociationStatus).\
            select(col('StoreNumber'), col('dealercode1').alias('DealerCode'),
                   col('AssociationType1').alias('AssociationType'),
                   col('AssociationStatus1').alias('AssociationStatus'),
                   col('TBLoc'), col('SMFMapping'), col('RankDescription'), col('CompanyCode'))

        dfStoreAssOpen.registerTempTable("storeAss1")

#########################################################################################################
# Code for new entry fields for Open Standard #
#########################################################################################################

        dfOpenFilNewField = dfOpenFil.withColumn('StoreNumber', dfOpenFil.SMFMapping). \
            withColumn('dealercode1', dfOpenFil.DealerCode). \
            withColumn('AssociationType1', when((dfOpenFil.RankDescription != 'SMF Only'), 'SMF')). \
            withColumn('AssociationStatus1', when((dfOpenFil.RankDescription != 'SMF Only'), 'Active')). \
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

        joined_DF = spark.sql("select cast(StoreNumber as integer),DealerCode, 4 as CompanyCode,AssociationType,"
                              + "AssociationStatus from storeAss1 union select StoreNumber,DealerCode,4 as CompanyCode,"
                              + "AssociationType,AssociationStatus from storeAss2")

        joined_DF.coalesce(1).select("*").write.parquet(self.associationFilePath)

        joined_DF.coalesce(1).write.mode("overwrite").parquet(self.storeAssociationWorkingPath)
        joined_DF.coalesce(1).write.mode('append').partitionBy('year', 'month').format('parquet').save(
            self.storeAssociationPartitonPath)

        spark.stop()


if __name__ == "__main__":
    StoreDealerCodeAssociationRefine().loadRefine()
