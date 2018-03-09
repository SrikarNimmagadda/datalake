from pyspark.sql import SparkSession
import sys
from datetime import datetime
import boto3


class ProductCategoryRefinedToDelivery:
    def __init__(self):

        self.refinedBucketWithS3 = sys.argv[1]
        self.deliveryBucketWithS3 = sys.argv[2]

        self.refinedBucket = self.refinedBucketWithS3[self.refinedBucketWithS3.index('tb-us'):]
        self.deliveryBucket = self.deliveryBucketWithS3[self.deliveryBucketWithS3.index('tb-us'):]

        self.prodCatCurrentPath = 's3://' + self.deliveryBucket + '/Current'
        self.prodCatPreviousPath = 's3://' + self.deliveryBucket + '/Previous'

        self.prodCatColumns = "CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME," \
                              "LVL_3_ID,LVL_3_NME,LVL_4_ID,LVL_4_NME,LVL_5_ID,LVL_5_NME, LVL_6_ID,LVL_6_NME,LVL_7_ID," \
                              "LVL_7_NME,LVL_8_ID,LVL_8_NME,LVL_9_ID,LVL_9_NME,LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND," \
                              "CDC_IND_CD"

    def findLastModifiedFile(self, bucketNode, prefixPath, bucket, currentOrPrev=1):

        print("prefixPath is ", prefixPath)
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
        print("Number of part files is : ", numFiles)
        lastUpdatedFilePath = ''
        lastPreviousRefinedPath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            print("Last Modified file in s3 format is : ", lastUpdatedFilePath)

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName
            print("Second Last Modified file in s3 format is : ", lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        spark = SparkSession.builder.appName("ProductCategoryDelivery").getOrCreate()
        s3 = boto3.resource('s3')

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)
        todayyear = datetime.now().strftime('%Y')

        prodCatPrefixPath = "ProductCategory/year=" + todayyear
        lastUpdatedProdCatFile = self.findLastModifiedFile(
            refinedBucketNode, prodCatPrefixPath, self.refinedBucket)

        lastPrevUpdatedProdCatFile = self.findLastModifiedFile(
            refinedBucketNode, prodCatPrefixPath, self.refinedBucket, 0)

        if lastUpdatedProdCatFile != '':
            print("Last modified file name is : ", lastUpdatedProdCatFile)

            spark.read.parquet(lastUpdatedProdCatFile). \
                registerTempTable("CurrentProdCatTableTmp")

            spark.sql(
                "select NVL(a.category_Id,'NA') as CAT_ID,NVL(a.company_cd,-1) as CO_CD,NVL(a.category_name,'NA') as "
                "CAT_NME, NVL(a.category_path,'NA') as CAT_PATH, NVL(a.parent_category_id,'NA') as PARNT_CAT_ID, "
                "NVL(a.level_one_id,'NA') as LVL_1_ID, NVL(a.level_one_name,'NA') as LVL_1_NME, a.level_two_id as "
                "LVL_2_ID, a.level_two_name as LVL_2_NME, a.level_three_id as LVL_3_ID, a.level_three_name as "
                "LVL_3_NME, a.level_four_id as LVL_4_ID, a.level_four_name as LVL_4_NME, a.level_five_id as LVL_5_ID, "
                "a.level_five_name as LVL_5_NME, a.level_six_id as LVL_6_ID, a.level_six_name as LVL_6_NME, "
                "a.level_seven_id as LVL_7_ID, a.level_seven_name as LVL_7_NME, a.level_eight_id as LVL_8_ID, "
                "a.level_eight_name as LVL_8_NME,  a.level_nine_id as LVL_9_ID, a.level_nine_name as LVL_9_NME, "
                "a.level_ten_id as LVL_10_ID, a.level_ten_name LVL_10_NME, a.active_indicator as PROD_CAT_ACT_IND from"
                " CurrentProdCatTableTmp a").registerTempTable("CurrentProdCatTableTmpRenamed")

            spark.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                      "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                      "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND,"
                      "hash(CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                      "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                      "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND) as hash_key "
                      "from CurrentProdCatTableTmpRenamed a").registerTempTable("CurrentProdCatTable")

            currTableCount = spark.sql("select count(*) from CurrentProdCatTable").show()
            print("Current Table count is : ", currTableCount)
            spark.sql("select * from CurrentProdCatTable").show()

        if lastPrevUpdatedProdCatFile != '':
            print("Second Last modified file name is : ", lastPrevUpdatedProdCatFile)

            spark.read.parquet(lastPrevUpdatedProdCatFile).\
                registerTempTable("PrevProdCatTableTmp")

            spark.sql("select NVL(a.category_Id,'NA') as CAT_ID,NVL(a.company_cd,-1) as CO_CD,NVL(a.category_name,'NA') as "
                      "CAT_NME, NVL(a.category_path,'NA') as CAT_PATH, NVL(a.parent_category_id,'NA') as PARNT_CAT_ID, "
                      "NVL(a.level_one_id,'NA') as LVL_1_ID, NVL(a.level_one_name,'NA') as LVL_1_NME, a.level_two_id as "
                      "LVL_2_ID, a.level_two_name as LVL_2_NME, a.level_three_id as LVL_3_ID, a.level_three_name as "
                      "LVL_3_NME, a.level_four_id as LVL_4_ID, a.level_four_name as LVL_4_NME, a.level_five_id as LVL_5_ID, "
                      "a.level_five_name as LVL_5_NME, a.level_six_id as LVL_6_ID, a.level_six_name as LVL_6_NME, "
                      "a.level_seven_id as LVL_7_ID, a.level_seven_name as LVL_7_NME, a.level_eight_id as LVL_8_ID, "
                      "a.level_eight_name as LVL_8_NME,  a.level_nine_id as LVL_9_ID, a.level_nine_name as LVL_9_NME, "
                      "a.level_ten_id as LVL_10_ID, a.level_ten_name LVL_10_NME, a.active_indicator as PROD_CAT_ACT_IND from"
                      " PrevProdCatTableTmp a").registerTempTable("PrevProdCatTableTmpRenamed")

            spark.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                      "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                      "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND,"
                      "hash(CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,LVL_2_NME,LVL_3_ID, "
                      "LVL_3_NME, LVL_4_ID, LVL_4_NME, LVL_5_ID,LVL_5_NME, LVL_6_ID, LVL_6_NME, LVL_7_ID, LVL_7_NME, "
                      "LVL_8_ID, LVL_8_NME,  LVL_9_ID, LVL_9_NME, LVL_10_ID,LVL_10_NME,PROD_CAT_ACT_IND) as hash_key "
                      "from PrevProdCatTableTmpRenamed a").registerTempTable("PrevProdCatTable")

            prevTableCount = spark.sql("select count(*) from PrevProdCatTable").show()
            print("Previous Table count is : ", prevTableCount)
            spark.sql("select * from PrevProdCatTable").show()

        else:
            print("There is only 1 file, hence not computing the second latest file name")

        if lastUpdatedProdCatFile != '' and lastPrevUpdatedProdCatFile != '':
            print('Current and Previous refined data files are found. So processing for delivery layer starts')

            dfProdCatUpdated = spark.sql(
                "select a.CAT_ID,a.CO_CD,a.CAT_NME,a.CAT_PATH,a.PARNT_CAT_ID,a.LVL_1_ID,a.LVL_1_NME,a.LVL_2_ID,"
                "a.LVL_2_NME, a.LVL_3_ID,a.LVL_3_NME,a.LVL_4_ID,a.LVL_4_NME,a.LVL_5_ID,a.LVL_5_NME, a. LVL_6_ID,"
                "a.LVL_6_NME,a.LVL_7_ID,a.LVL_7_NME,a.LVL_8_ID,a.LVL_8_NME, a.LVL_9_ID,a.LVL_9_NME,a.LVL_10_ID,"
                "a.LVL_10_NME,a.PROD_CAT_ACT_IND, 'C' as CDC_IND_CD from CurrentProdCatTable a LEFT OUTER JOIN "
                "PrevProdCatTable b on a.CAT_ID = b.CAT_ID where a.hash_key <> b.hash_key")

            rowCountUpdateRecords = dfProdCatUpdated.count()

            print("Number of update records are : ", rowCountUpdateRecords)

            dfProdCatUpdated.registerTempTable("prod_cat_updated_data")

            dfProdCatNew = spark.sql(
                "select a.CAT_ID, a.CO_CD, a.CAT_NME, a.CAT_PATH, a.PARNT_CAT_ID, a.LVL_1_ID, a.LVL_1_NME, a.LVL_2_ID, "
                "a.LVL_2_NME, a.LVL_3_ID, a.LVL_3_NME, a.LVL_4_ID, a.LVL_4_NME, a.LVL_5_ID, a.LVL_5_NME, a. LVL_6_ID, "
                "a.LVL_6_NME, a.LVL_7_ID, a.LVL_7_NME, a.LVL_8_ID, a.LVL_8_NME, a.LVL_9_ID, a.LVL_9_NME, a.LVL_10_ID, "
                "a.LVL_10_NME, a.PROD_CAT_ACT_IND, 'I' as CDC_IND_CD from CurrentProdCatTable a LEFT OUTER JOIN "
                "PrevProdCatTable b on a.CAT_ID = b.CAT_ID where b.CAT_ID IS NULL")

            rowCountNewRecords = dfProdCatNew.count()

            print("Number of insert records are : ", rowCountNewRecords)

            dfProdCatNew.registerTempTable("prod_cat_new_data")

            dfProdCatDelta = spark.sql(
                "select " + self.prodCatColumns + " from prod_cat_updated_data union all select " + self.prodCatColumns + " from prod_cat_new_data")

            print('Updated prod_cat ids are')
            dfProdCatUpdatedPrint = spark.sql("select CAT_ID from prod_cat_updated_data")
            print(dfProdCatUpdatedPrint.show(100, truncate=False))

            print('New added prod_cat ids are')
            dfProdCatNewPrint = spark.sql("select CAT_ID from prod_cat_new_data")
            print(dfProdCatNewPrint.show(100, truncate=False))

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                print("Updated file has arrived..")
                dfProdCatDelta.coalesce(1).write.mode("overwrite").csv(self.prodCatCurrentPath, header=True)
                dfProdCatDelta.coalesce(1).write.mode("append").csv(self.prodCatPreviousPath, header=True)
            else:
                print("The prev and current files are same. So no delta file will be generated in delivery bucket.")

        elif lastUpdatedProdCatFile != '' and lastPrevUpdatedProdCatFile == '':
            print("This is the first transformation call, So keeping the refined file in delivery bucket.")

            dfProdCatCurr = spark.sql("select CAT_ID,CO_CD,CAT_NME,CAT_PATH,PARNT_CAT_ID,LVL_1_ID,LVL_1_NME,LVL_2_ID,"
                                      "LVL_2_NME,LVL_3_ID,LVL_3_NME,LVL_4_ID,LVL_4_NME,LVL_5_ID,LVL_5_NME, LVL_6_ID,"
                                      "LVL_6_NME,LVL_7_ID,LVL_7_NME,LVL_8_ID,LVL_8_NME,LVL_9_ID,LVL_9_NME,LVL_10_ID,"
                                      "LVL_10_NME,PROD_CAT_ACT_IND,'I' as CDC_IND_CD from CurrentProdCatTable")

            print("Current Path is : ", self.prodCatCurrentPath)
            print("Previous Path is : ", self.prodCatPreviousPath)

            dfProdCatCurr.coalesce(1).write.mode("overwrite").csv(self.prodCatCurrentPath, header=True)
            dfProdCatCurr.coalesce(1).write.mode("append").csv(self.prodCatPreviousPath, header=True)

        else:
            print("This should not be printed. Please check.")

        spark.stop()


if __name__ == "__main__":
    ProductCategoryRefinedToDelivery().loadDelivery()

########################################################################################################################
