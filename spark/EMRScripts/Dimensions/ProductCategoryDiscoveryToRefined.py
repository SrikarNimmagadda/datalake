from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import sys
from datetime import datetime
from pyspark.sql.functions import udf, regexp_replace, col, hash as hash_
import boto3


def getCategoryIds(categoryIdInt):
    categoryId = str(categoryIdInt)
    categoryList = list()
    count = 2
    for i in range(2, len(categoryId) + 2, 2):
        if count % 2 == 0:
            categoryList.append(categoryId[0:count])
            count = count + 2
    return '>>'.join(categoryList)


class ProductCategoryDiscoveryToRefined(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.prodCategoryInputPath = sys.argv[1]
        self.outputPath = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/refined'
        self.productCategoryName = self.outputPath[self.outputPath.index('tb'):].split("/")[1]
        self.cdcBucketName = self.outputPath[self.outputPath.index('tb'):].split("/")[0]
        self.productCategoryFilePartitionPath = 's3://' + self.cdcBucketName + '/' + self.productCategoryName
        self.getCategoryIdsUDF = udf(lambda z: getCategoryIds(z), StringType())

    def loadRefined(self):

        dfProdCatIdList = self.sparkSession.read.parquet(self.prodCategoryInputPath).dropDuplicates(['id']).\
            withColumn("categorypath", regexp_replace(col('categorypath'), '([0-9]).', '')).\
            withColumn('cat_id_list', self.getCategoryIdsUDF(col('id'))).cache()

        dfProdCatIdList.registerTempTable("ProdCategoryTempTable")

        SourceDataDFTmp = self.sparkSession.sql("select cast(a.id as string) as category_Id, a.description as category_name, a.categorypath"
                                                " as category_path, cast(a.Parentid as string) as parent_category_id, CASE WHEN a.enabled "
                                                "= TRUE THEN 1 ELSE 0 END as active_indicator,'4' as company_cd, split(a.cat_id_list,'>>')[0] as "
                                                "level_one_id,'Inventory Tree' as level_one_name, split(a.cat_id_list,'>>')[1] as level_two_id, "
                                                "case when a.Parentid is null then '' else 'Products' end as level_two_name,split(a.cat_id_list,'>>')[2] as level_three_id, "
                                                "trim(split(trim(a.categorypath),'>>')[1]) as level_three_name,split(a.cat_id_list,'>>')[3] as "
                                                "level_four_id, trim(split(trim(a.categorypath),'>>')[2]) as level_four_name, "
                                                "split(a.cat_id_list,'>>')[4] as level_five_id, trim(split(trim(a.categorypath),'>>')[3]) as "
                                                "level_five_name, split(a.cat_id_list,'>>')[5] as level_six_id, "
                                                "trim(split(trim(a.categorypath),'>>')[4]) as level_six_name, split(a.cat_id_list,'>>')[6] as "
                                                "level_seven_id, trim(split(trim(a.categorypath),'>>')[5]) as level_seven_name, "
                                                "split(a.cat_id_list,'>>')[7] as level_eight_id, trim(split(trim(a.categorypath),'>>')[6]) as "
                                                "level_eight_name, '' as level_nine_id, ' ' as level_nine_name, '' as level_ten_id, "
                                                "' ' as level_ten_name, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, "
                                                "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdCategoryTempTable a")

        todayyear = datetime.now().strftime('%Y')
        # CDC Logic

        bkt = self.cdcBucketName

        my_bucket = self.s3.Bucket(name=bkt)

        all_values_dict = {}
        req_values_dict = {}

        pfx = "ProductCategory/year=" + todayyear

        partitionName = my_bucket.objects.filter(Prefix=pfx)

        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified

        for k, v in all_values_dict.items():
            if 'part-0000' in k:
                req_values_dict[k] = v

        self.log.info("Required values dictionary contents are : " + str(req_values_dict))

        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
        self.log.info("Files are : " + str(revSortedFiles))

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))

        # cols_list = ["category_Id", "category_name", "category_path", "parent_category_id", "active_indicator", "company_cd",
        #              "level_one_id", "level_one_name", "level_two_id", "level_two_name", "level_three_id", "level_three_name",
        #              "level_four_id", "level_four_name", "level_five_id", "level_five_name", "level_six_id", "level_six_name",
        #              "level_seven_id", "level_seven_name", "level_eight_id", "level_eight_name", "level_nine_id",
        #              "level_nine_name", "level_ten_id", "level_ten_name"]

        sourceDataDF = SourceDataDFTmp.withColumn("hash_key", hash_("category_Id", "category_name", "category_path",
                                                                    "parent_category_id", "active_indicator", "company_cd",
                                                                    "level_one_id", "level_one_name", "level_two_id",
                                                                    "level_two_name", "level_three_id", "level_three_name",
                                                                    "level_four_id", "level_four_name", "level_five_id",
                                                                    "level_five_name", "level_six_id", "level_six_name",
                                                                    "level_seven_id", "level_seven_name", "level_eight_id",
                                                                    "level_eight_name", "level_nine_id", "level_nine_name",
                                                                    "level_ten_id", "level_ten_name"))

        sourceDataDF.registerTempTable("CurrentSourceTempTable")

        if numFiles > 0:
            self.log.info("Files found in Refined layer, CDC can be performed\n")
            lastModifiedFileNameTmp = str(revSortedFiles[0])
            lastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
            self.log.info("Last Modified file is : " + lastModifiedFileName)

            self.sparkSession.read.parquet(lastModifiedFileName).registerTempTable("PreviousDataRefinedTable")

            # selects target data with no change
            self.sparkSession.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,a.active_indicator,"
                                  "a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,a.level_two_name,a.level_three_id,"
                                  "a.level_three_name, a.level_four_id,a.level_four_name,a.level_five_id,a.level_five_name,a.level_six_id,"
                                  "a.level_six_name, a.level_seven_id,a.level_seven_name,a.level_eight_id,a.level_eight_name,"
                                  "a.level_nine_id,a.level_nine_name, a.level_ten_id,a.level_ten_name,a.hash_key, b.year,b.month from "
                                  "PreviousDataRefinedTable a LEFT OUTER JOIN CurrentSourceTempTable b  on a.category_Id = b.category_Id "
                                  "where a.hash_key = b.hash_key").registerTempTable("target_no_change_data")

            # selects source data which got updated
            dfProdCatUpdated = self.sparkSession.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,"
                                                     "a.active_indicator,a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,"
                                                     "a.level_two_name,a.level_three_id,a.level_three_name, a.level_four_id,"
                                                     "a.level_four_name,a.level_five_id,a.level_five_name,a.level_six_id,a.level_six_name,"
                                                     " a.level_seven_id,a.level_seven_name,a.level_eight_id,a.level_eight_name,"
                                                     "a.level_nine_id,a.level_nine_name, a.level_ten_id,a.level_ten_name,a.hash_key, "
                                                     "a.year,a.month FROM CurrentSourceTempTable a LEFT OUTER JOIN PreviousDataRefinedTable"
                                                     " b on a.category_Id = b.category_Id  where a.hash_key <> b.hash_key")

            rowCountUpdateRecords = dfProdCatUpdated.count()
            dfProdCatUpdated.registerTempTable("src_updated_data")

            # selects new records from source
            dfProdCatNew = self.sparkSession.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,"
                                                 "a.active_indicator,a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,"
                                                 "a.level_two_name,a.level_three_id,a.level_three_name, a.level_four_id,a.level_four_name,"
                                                 "a.level_five_id,a.level_five_name,a.level_six_id,a.level_six_name, a.level_seven_id,"
                                                 "a.level_seven_name,a.level_eight_id,a.level_eight_name,a.level_nine_id,a.level_nine_name,"
                                                 " a.level_ten_id,a.level_ten_name,a.hash_key, a.year,a.month FROM CurrentSourceTempTable a"
                                                 " LEFT OUTER JOIN PreviousDataRefinedTable b on a.category_Id = b.category_Id  where "
                                                 "b.category_Id is null")

            rowCountNewRecords = dfProdCatNew.count()
            dfProdCatNew.registerTempTable("src_new_data")

            self.log.info('Updated prod_cat ids are')
            self.sparkSession.sql("select category_Id from src_updated_data")

            self.log.info('New added prod_cat ids are')
            self.sparkSession.sql("select category_Id from src_new_data")

            # union all extracted records
            final_cdc_data = self.sparkSession.sql(" SELECT category_Id,category_name,category_path,parent_category_id,active_indicator,"
                                                   "company_cd,level_one_id,level_one_name,level_two_id,level_two_name,level_three_id,"
                                                   "level_three_name,level_four_id,level_four_name,level_five_id,level_five_name,"
                                                   "level_six_id,level_six_name,level_seven_id,level_seven_name,level_eight_id,"
                                                   "level_eight_name,level_nine_id,level_nine_name,level_ten_id,level_ten_name,hash_key,"
                                                   "year,month FROM target_no_change_data UNION ALL SELECT category_Id,category_name,"
                                                   "category_path,parent_category_id,active_indicator,company_cd,level_one_id,"
                                                   "level_one_name,level_two_id,level_two_name,level_three_id,level_three_name,"
                                                   "level_four_id,level_four_name,level_five_id,level_five_name,level_six_id,"
                                                   "level_six_name,level_seven_id,level_seven_name,level_eight_id,level_eight_name,"
                                                   "level_nine_id,level_nine_name,level_ten_id,level_ten_name,hash_key,year,month FROM "
                                                   "src_updated_data UNION ALL SELECT category_Id,category_name,category_path,"
                                                   "parent_category_id,active_indicator,company_cd,level_one_id,level_one_name,"
                                                   "level_two_id,level_two_name,level_three_id,level_three_name,level_four_id,"
                                                   "level_four_name,level_five_id,level_five_name,level_six_id,level_six_name,"
                                                   "level_seven_id,level_seven_name,level_eight_id,level_eight_name,level_nine_id,"
                                                   "level_nine_name,level_ten_id,level_ten_name,hash_key,year,month FROM src_new_data")

            # Write final CDC data to output path
            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                print("Changes noticed in the source file, creating a new file in the Refined layer partition")

                final_cdc_data.coalesce(1).select('category_Id', 'category_name', 'category_path', 'parent_category_id',
                                                  'active_indicator', 'company_cd', 'level_one_id', 'level_one_name',
                                                  'level_two_id', 'level_two_name', 'level_three_id', 'level_three_name',
                                                  'level_four_id', 'level_four_name', 'level_five_id', 'level_five_name',
                                                  'level_six_id', 'level_six_name', 'level_seven_id', 'level_seven_name',
                                                  'level_eight_id', 'level_eight_name', 'level_nine_id', 'level_nine_name',
                                                  'level_ten_id', 'level_ten_name', 'hash_key').\
                    write.mode("overwrite").\
                    format('parquet').\
                    save(self.outputPath)

                final_cdc_data.coalesce(1).select('category_Id', 'category_name', 'category_path', 'parent_category_id',
                                                  'active_indicator', 'company_cd', 'level_one_id', 'level_one_name',
                                                  'level_two_id', 'level_two_name', 'level_three_id', 'level_three_name',
                                                  'level_four_id', 'level_four_name', 'level_five_id', 'level_five_name',
                                                  'level_six_id', 'level_six_name', 'level_seven_id', 'level_seven_name',
                                                  'level_eight_id', 'level_eight_name', 'level_nine_id', 'level_nine_name',
                                                  'level_ten_id', 'level_ten_name', 'hash_key', 'year', 'month').write.\
                    mode("append").partitionBy('year', 'month').format('parquet').save(self.productCategoryFilePartitionPath)
            else:
                print("No changes in the source file, not creating any files in the Refined layer")

        else:
            print("No files found in refined layer, writing the source data to refined")

            sourceDataDF.coalesce(1).\
                select('category_Id', 'category_name', 'category_path', 'parent_category_id', 'active_indicator', 'company_cd',
                       'level_one_id', 'level_one_name', 'level_two_id', 'level_two_name', 'level_three_id', 'level_three_name',
                       'level_four_id', 'level_four_name', 'level_five_id', 'level_five_name', 'level_six_id', 'level_six_name',
                       'level_seven_id', 'level_seven_name', 'level_eight_id', 'level_eight_name', 'level_nine_id',
                       'level_nine_name', 'level_ten_id', 'level_ten_name', 'hash_key').\
                write.mode("overwrite").\
                format('parquet').\
                save(self.outputPath)

            sourceDataDF.coalesce(1).select('category_Id', 'category_name', 'category_path', 'parent_category_id',
                                            'active_indicator', 'company_cd', 'level_one_id', 'level_one_name', 'level_two_id',
                                            'level_two_name', 'level_three_id', 'level_three_name', 'level_four_id',
                                            'level_four_name', 'level_five_id', 'level_five_name', 'level_six_id',
                                            'level_six_name', 'level_seven_id', 'level_seven_name', 'level_eight_id',
                                            'level_eight_name', 'level_nine_id', 'level_nine_name', 'level_ten_id',
                                            'level_ten_name', 'hash_key', 'year', 'month').write.mode('append').\
                partitionBy('year', 'month').format('parquet').save(self.productCategoryFilePartitionPath)

        ########################################################################################################################

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCategoryDiscoveryToRefined().loadRefined()
