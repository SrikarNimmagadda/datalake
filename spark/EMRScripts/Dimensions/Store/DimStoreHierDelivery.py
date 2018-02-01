import sys
import datetime
import boto3
from pyspark.sql import SparkSession


class DimStoreHierDelivery(object):

    def __init__(self):
        self.dimStoreRefined = sys.argv[1]
        self.dimAttDealerCodeRefined = sys.argv[2]
        self.storeHierDeliveryOutput = sys.argv[3]

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb-us'):]

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
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            print("Last Modified ", prefixType, " file in s3 format is : ", lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadDelivery(self):

        spark = SparkSession.builder.appName("DimStoreHierDelivery").getOrCreate()

        spark.read.parquet(self.dimAttDealerCodeRefined).registerTempTable("att_dealer_code")

        spark.read.parquet(self.dimStoreRefined).registerTempTable("store_refine")

        dfStoreHier = spark.sql("select concat(a.ATTRegion,a.ATTMarket) as hier_id,'ATT HIERARCHY' as hier_nm,"
                                + "a.ATTRegion as lvl_1_cd,a.ATTRegion as lvl_1_nm,"
                                + " a.ATTMarketCode as lvl_2_cd, a.ATTMarket as lvl_2_nm, '' as lvl_3_cd,"
                                + " '' as lvl_3_nm,'' as lvl_4_cd,"
                                + " '' as lvl_4_nm, '' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm,"
                                + " 'I' as cdc_ind_cd from att_dealer_code a where a.ATTRegion != '' and "
                                + "a.ATTMarket != '' union select concat(b.spring_market,b.spring_region,"
                                + "b.spring_district) as hier_id,'STORE HIERARCHY' as hier_nm,b.spring_market as "
                                + "lvl_1_cd,b.spring_market as lvl_1_nm,"
                                + " b.spring_region as lvl_2_cd, b.spring_region as lvl_2_nm, b.spring_district as"
                                + " lvl_3_cd, b.spring_district as lvl_3_nm, '' as lvl_4_cd,"
                                + " '' as lvl_4_nm, '' as lvl_5_cd,'' as lvl_5_nm,'' as lvl_6_cd,'' as lvl_6_nm,"
                                + " 'I' as cdc_ind_cd from store_refine b where b.spring_market != '' and "
                                + "b.spring_region != '' and b.spring_district != ''")

        dfStoreHier.coalesce(1).select("*").write.mode("overwrite").csv(self.storeHierDeliveryOutput, header=True)
        spark.stop()


if __name__ == "__main__":

    DimStoreHierDelivery().loadDelivery()
