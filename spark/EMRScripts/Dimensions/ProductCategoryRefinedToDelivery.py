from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class ProductCategoryRefinedToDelivery:

    def __init__(self):

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb'):].split("/")[0]
        self.prodCatPrefixPath = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb'):].split("/")[1]

        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb'):].split("/")[0]
        self.deliveryName = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb'):].split("/")[1]

        self.prodCatCurrentPath = self.deliveryBucketWithS3
        self.prodCatPreviousPath = 's3://' + self.deliveryBucket + '/' + self.deliveryName + '/Previous'

        self.prodCatColumns = "CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME," \
                              "LVL_3_ID,LVL_3_NME,LVL_4_ID,LVL_4_NME,LVL_5_ID,LVL_5_NME, LVL_6_ID,LVL_6_NME,LVL_7_ID," \
                              "LVL_7_NME,LVL_8_ID,LVL_8_NME,LVL_9_ID,LVL_9_NME,LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND," \
                              "CDC_IND_CD"

    def findLastModifiedFile(self, bucketNode, prefixType, bucket, currentOrPrev=1):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        allValuesDict = {}
        reqValuesDict = {}
        for obj in partitionName:
            allValuesDict[obj.key] = obj.last_modified
        for k, v in allValuesDict.items():
            if 'part-0000' in k:
                reqValuesDict[k] = v
        revSortedFiles = sorted(reqValuesDict, key=reqValuesDict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastUpdatedFilePath)

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def makeZeroByteFile(self, destinationPath, fileName):

        newBucketWithPath = urlparse(destinationPath)
        newBucket = newBucketWithPath.netloc
        newBucketNode = self.s3.Bucket(name=newBucket)
        newPath = newBucketWithPath.path.lstrip('/')
        objs = newBucketNode.objects.filter(Prefix=newPath)
        for s3Object in objs:
            s3Object.delete()

        newFile = open(fileName, 'w')
        newFile.close()
        self.client.upload_file(fileName, newBucket, newPath + '/' + fileName)

    def loadDelivery(self):

        self.log.info('bucket name: ' + self.refinedBucket)
        refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

        lastUpdatedProdCatFile = self.findLastModifiedFile(refinedBucketNode, self.prodCatPrefixPath, self.refinedBucket)
        lastPrevUpdatedProdCatFile = self.findLastModifiedFile(
            refinedBucketNode, self.prodCatPrefixPath, self.refinedBucket, 0)

        if lastUpdatedProdCatFile != '':
            self.log.info("Last modified file name is : " + lastUpdatedProdCatFile)

            self.sparkSession.read.parquet(lastUpdatedProdCatFile). \
                registerTempTable("CurrentProdCatTableTmp")

            self.sparkSession.sql("select NVL(a.category_Id,'NA') as CAT_ID,NVL(a.company_cd,-1) as CO_CD,NVL(a.category_name,'NA') as "
                                  "CAT_NME, NVL(a.category_path,'NA') as CAT_PATH, NVL(a.parent_category_id,'NA') as PARNT_CAT_ID, "
                                  "NVL(a.level_one_id,'NA') as LVL_1_ID, NVL(a.level_one_name,'NA') as LVL_1_NME, a.level_two_id as "
                                  "LVL_2_ID, a.level_two_name as LVL_2_NME, a.level_three_id as LVL_3_ID, a.level_three_name as "
                                  "LVL_3_NME, a.level_four_id as LVL_4_ID, a.level_four_name as LVL_4_NME, a.level_five_id as LVL_5_ID, "
                                  "a.level_five_name as LVL_5_NME, a.level_six_id as LVL_6_ID, a.level_six_name as LVL_6_NME, "
                                  "a.level_seven_id as LVL_7_ID, a.level_seven_name as LVL_7_NME, a.level_eight_id as LVL_8_ID, "
                                  "a.level_eight_name as LVL_8_NME,  a.level_nine_id as LVL_9_ID, a.level_nine_name as LVL_9_NME, "
                                  "a.level_ten_id as LVL_10_ID, a.level_ten_name LVL_10_NME, a.active_indicator as PROD_CAT_ACT_IND from"
                                  " CurrentProdCatTableTmp a").registerTempTable("CurrentProdCatTableTmpRenamed")

            self.sparkSession.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                                  "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                                  "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND,"
                                  "hash(CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                                  "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                                  "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND) as hash_key "
                                  "from CurrentProdCatTableTmpRenamed a").registerTempTable("CurrentProdCatTable")

            currTableCount = self.sparkSession.sql("select count(*) from CurrentProdCatTable").show()
            self.log.info("Current Table count is : " + str(currTableCount))
            self.sparkSession.sql("select * from CurrentProdCatTable").show()

        if lastPrevUpdatedProdCatFile != '':
            self.log.info("Second Last modified file name is : " + lastPrevUpdatedProdCatFile)

            self.sparkSession.read.parquet(lastPrevUpdatedProdCatFile).\
                registerTempTable("PrevProdCatTableTmp")

            self.sparkSession.sql("select NVL(a.category_Id,'NA') as CAT_ID,NVL(a.company_cd,-1) as CO_CD,NVL(a.category_name,'NA') as "
                                  "CAT_NME, NVL(a.category_path,'NA') as CAT_PATH, NVL(a.parent_category_id,'NA') as PARNT_CAT_ID, "
                                  "NVL(a.level_one_id,'NA') as LVL_1_ID, NVL(a.level_one_name,'NA') as LVL_1_NME, a.level_two_id as "
                                  "LVL_2_ID, a.level_two_name as LVL_2_NME, a.level_three_id as LVL_3_ID, a.level_three_name as "
                                  "LVL_3_NME, a.level_four_id as LVL_4_ID, a.level_four_name as LVL_4_NME, a.level_five_id as LVL_5_ID, "
                                  "a.level_five_name as LVL_5_NME, a.level_six_id as LVL_6_ID, a.level_six_name as LVL_6_NME, "
                                  "a.level_seven_id as LVL_7_ID, a.level_seven_name as LVL_7_NME, a.level_eight_id as LVL_8_ID, "
                                  "a.level_eight_name as LVL_8_NME,  a.level_nine_id as LVL_9_ID, a.level_nine_name as LVL_9_NME, "
                                  "a.level_ten_id as LVL_10_ID, a.level_ten_name LVL_10_NME, a.active_indicator as PROD_CAT_ACT_IND from"
                                  " PrevProdCatTableTmp a").registerTempTable("PrevProdCatTableTmpRenamed")

            self.sparkSession.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                                  "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                                  "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND,"
                                  "hash(CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                                  "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                                  "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND) as hash_key "
                                  "from PrevProdCatTableTmpRenamed a").registerTempTable("PrevProdCatTable")

            prevTableCount = self.sparkSession.sql("select count(*) from PrevProdCatTable").show()
            self.log.info("Previous Table count is : " + str(prevTableCount))
            self.sparkSession.sql("select * from PrevProdCatTable").show()

        else:
            self.log.info("There is only 1 file, hence not computing the second latest file name")

        if lastUpdatedProdCatFile != '' and lastPrevUpdatedProdCatFile != '':
            self.log.info('Current and Previous refined data files are found. So processing for delivery layer starts')

            dfProdCatUpdated = self.sparkSession.sql("select a.CAT_ID,a.CO_CD,a.CAT_NME,a.CAT_PATH,a.PARNT_CAT_ID,a.LVL_1_ID,a.LVL_1_NME,a.LVL_2_ID,"
                                                     "a.LVL_2_NME, a.LVL_3_ID,a.LVL_3_NME,a.LVL_4_ID,a.LVL_4_NME,a.LVL_5_ID,a.LVL_5_NME, a. LVL_6_ID,"
                                                     "a.LVL_6_NME,a.LVL_7_ID,a.LVL_7_NME,a.LVL_8_ID,a.LVL_8_NME, a.LVL_9_ID,a.LVL_9_NME,a.LVL_10_ID,"
                                                     "a.LVL_10_NME,a.PROD_CAT_ACT_IND, 'C' as CDC_IND_CD from CurrentProdCatTable a LEFT OUTER JOIN "
                                                     "PrevProdCatTable b on a.CAT_ID = b.CAT_ID where a.hash_key <> b.hash_key")

            rowCountUpdateRecords = dfProdCatUpdated.count()

            self.log.info("Number of update records are : " + str(rowCountUpdateRecords))

            dfProdCatUpdated.registerTempTable("prod_cat_updated_data")

            dfProdCatNew = self.sparkSession.sql(
                "select a.CAT_ID, a.CO_CD, a.CAT_NME, a.CAT_PATH, a.PARNT_CAT_ID, a.LVL_1_ID, a.LVL_1_NME, a.LVL_2_ID, "
                "a.LVL_2_NME, a.LVL_3_ID, a.LVL_3_NME, a.LVL_4_ID, a.LVL_4_NME, a.LVL_5_ID, a.LVL_5_NME, a. LVL_6_ID, "
                "a.LVL_6_NME, a.LVL_7_ID, a.LVL_7_NME, a.LVL_8_ID, a.LVL_8_NME, a.LVL_9_ID, a.LVL_9_NME, a.LVL_10_ID, "
                "a.LVL_10_NME, a.PROD_CAT_ACT_IND, 'I' as CDC_IND_CD from CurrentProdCatTable a LEFT OUTER JOIN "
                "PrevProdCatTable b on a.CAT_ID = b.CAT_ID where b.CAT_ID IS NULL")

            rowCountNewRecords = dfProdCatNew.count()

            self.log.info("Number of insert records are : " + str(rowCountNewRecords))

            dfProdCatNew.registerTempTable("prod_cat_new_data")

            dfProdCatDelta = self.sparkSession.sql(
                "select " + self.prodCatColumns + " from prod_cat_updated_data union all select " + self.prodCatColumns + " from prod_cat_new_data")

            self.log.info('Updated prod_cat ids are')
            self.sparkSession.sql("select CAT_ID from prod_cat_updated_data")

            self.log.info('New added prod_cat ids are')
            self.sparkSession.sql("select CAT_ID from prod_cat_new_data")

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                dfProdCatDelta.coalesce(1).write.mode("overwrite").csv(self.prodCatCurrentPath, header=True)
                dfProdCatDelta.coalesce(1).write.mode("append").csv(self.prodCatPreviousPath, header=True)
            else:
                self.makeZeroByteFile(self.prodCatCurrentPath, 'wt_prod_cat.csv')
                self.log.info("The prev and current files are same. So no delta file will be generated in delivery bucket.")

        elif lastUpdatedProdCatFile != '' and lastPrevUpdatedProdCatFile == '':
            self.log.info("This is the first transformation call, So keeping the refined file in delivery bucket.")

            dfProdCatCurr = self.sparkSession.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,"
                                                  "LVL_2_NME,LVL_3_ID,LVL_3_NME,LVL_4_ID,LVL_4_NME,LVL_5_ID,LVL_5_NME, LVL_6_ID,"
                                                  "LVL_6_NME,LVL_7_ID,LVL_7_NME,LVL_8_ID,LVL_8_NME,LVL_9_ID,LVL_9_NME,LVL_10_ID,"
                                                  "LVL_10_NME,PROD_CAT_ACT_IND,'I' as CDC_IND_CD from CurrentProdCatTable")

            self.log.info("Current Path is : " + self.prodCatCurrentPath)
            self.log.info("Previous Path is : " + self.prodCatPreviousPath)

            newRow = self.sparkSession.createDataFrame([[20, 4, 'Coupons', '', '', 20, 'Inventory Tree', 20, 'Coupons', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']])

            dfProdCatCurr.union(newRow).coalesce(1).write.mode("overwrite").csv(self.prodCatCurrentPath, header=True)
            dfProdCatCurr.union(newRow).coalesce(1).write.mode("append").csv(self.prodCatPreviousPath, header=True)

        else:
            self.log.info("This should not be printed. Please check.")

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCategoryRefinedToDelivery().loadDelivery()
