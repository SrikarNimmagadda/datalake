#!/usr/bin/python
#######################################################################################################################
# Author           : Harikesh R
# Date Created     : 12/01/2018
# Purpose          : To transform and move data from Discovery to Refined layer for Product Category
# Version          : 2.0
# Revision History : Modified code for level 'n' id and name as per latest STM
########################################################################################################################


from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import hash as hash_
import boto3
s3 = boto3.resource('s3')
client = boto3.client('s3')


# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
    appName("ProductCategoryRefinary").getOrCreate()


OutputPath = sys.argv[1]
CdcBucketName = sys.argv[2]
ProdCategoryInputPath = sys.argv[3]

ProdCategoryInp_DF = spark.read.parquet(ProdCategoryInputPath).dropDuplicates(['id']).\
    registerTempTable("ProdCategoryTempTable")

####################################################################################################################
#                                           Final Spark Transformaions                                             #
####################################################################################################################

SourceDataDFTmp = spark.sql("select cast(a.id as string) as category_Id, a.description as category_name, a.categorypath"
                            " as category_path, cast(a.Parentid as string) as parent_category_id, CASE WHEN a.enabled "
                            "= TRUE THEN 1 ELSE 0 END as active_indicator,'4' as company_cd, cast(a.id as string) as "
                            "level_one_id,'Inventory Tree' as level_one_name, cast(a.id as string) as level_two_id, "
                            "'Products' as level_two_name,cast(a.id as string) as level_three_id, "
                            "trim(split(trim(a.categorypath),'>>')[1]) as level_three_name,cast(a.id as string) as "
                            "level_four_id, trim(split(trim(a.categorypath),'>>')[2]) as level_four_name, "
                            "cast(a.id as string) as level_five_id, trim(split(trim(a.categorypath),'>>')[3]) as "
                            "level_five_name, cast(a.id as string) as level_six_id, "
                            "trim(split(trim(a.categorypath),'>>')[4]) as level_six_name, cast(a.id as string) as "
                            "level_seven_id, trim(split(trim(a.categorypath),'>>')[5]) as level_seven_name, "
                            "cast(a.id as string) as level_eight_id, trim(split(trim(a.categorypath),'>>')[6]) as "
                            "level_eight_name, ' ' as level_nine_id, ' ' as level_nine_name, ' ' as level_ten_id, "
                            "' ' as level_ten_name, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, "
                            "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdCategoryTempTable a")

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

# CDC Logic

bkt = CdcBucketName

my_bucket = s3.Bucket(name=bkt)

all_values_dict = {}
req_values_dict = {}

pfx = "ProductCategory/year=" + todayyear

partitionName = my_bucket.objects.filter(Prefix=pfx)

for obj in partitionName:
    all_values_dict[obj.key] = obj.last_modified

for k, v in all_values_dict.items():
    if 'part-0000' in k:
        req_values_dict[k] = v

print("Required values dictionary contents are : ", req_values_dict)

revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)
print("Files are : ", revSortedFiles)

numFiles = len(revSortedFiles)
print("Number of part files is : ", numFiles)

cols_list = ["category_Id", "category_name", "category_path", "parent_category_id", "active_indicator", "company_cd",
             "level_one_id", "level_one_name", "level_two_id", "level_two_name", "level_three_id", "level_three_name",
             "level_four_id", "level_four_name", "level_five_id", "level_five_name", "level_six_id", "level_six_name",
             "level_seven_id", "level_seven_name", "level_eight_id", "level_eight_name", "level_nine_id",
             "level_nine_name", "level_ten_id", "level_ten_name"]

SourceDataDF = SourceDataDFTmp.withColumn("hash_key", hash_("category_Id", "category_name", "category_path",
                                                            "parent_category_id", "active_indicator", "company_cd",
                                                            "level_one_id", "level_one_name", "level_two_id",
                                                            "level_two_name", "level_three_id", "level_three_name",
                                                            "level_four_id", "level_four_name", "level_five_id",
                                                            "level_five_name", "level_six_id", "level_six_name",
                                                            "level_seven_id", "level_seven_name", "level_eight_id",
                                                            "level_eight_name", "level_nine_id", "level_nine_name",
                                                            "level_ten_id", "level_ten_name"))

SourceDataDFTmpTable = SourceDataDF.registerTempTable("CurrentSourceTempTable")

if numFiles > 0:
    print("Files found in Refined layer, CDC can be performed\n")
    lastModifiedFileNameTmp = str(revSortedFiles[0])
    lastModifiedFileName = 's3://' + bkt + '/' + lastModifiedFileNameTmp
    print("Last Modified file is : ", lastModifiedFileName)
    print("\n")

    PreviousDataDF = spark.read.parquet(lastModifiedFileName).\
        registerTempTable("PreviousDataRefinedTable")

    # selects target data with no change
    spark.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,a.active_indicator,"
              "a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,a.level_two_name,a.level_three_id,"
              "a.level_three_name, a.level_four_id,a.level_four_name,a.level_five_id,a.level_five_name,a.level_six_id,"
              "a.level_six_name, a.level_seven_id,a.level_seven_name,a.level_eight_id,a.level_eight_name,"
              "a.level_nine_id,a.level_nine_name, a.level_ten_id,a.level_ten_name,a.hash_key, b.year,b.month from "
              "PreviousDataRefinedTable a LEFT OUTER JOIN CurrentSourceTempTable b  on a.category_Id = b.category_Id "
              "where a.hash_key = b.hash_key").registerTempTable("target_no_change_data")

    # selects source data which got updated
    dfProdCatUpdated = spark.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,"
                                 "a.active_indicator,a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,"
                                 "a.level_two_name,a.level_three_id,a.level_three_name, a.level_four_id,"
                                 "a.level_four_name,a.level_five_id,a.level_five_name,a.level_six_id,a.level_six_name,"
                                 " a.level_seven_id,a.level_seven_name,a.level_eight_id,a.level_eight_name,"
                                 "a.level_nine_id,a.level_nine_name, a.level_ten_id,a.level_ten_name,a.hash_key, "
                                 "a.year,a.month FROM CurrentSourceTempTable a LEFT OUTER JOIN PreviousDataRefinedTable"
                                 " b on a.category_Id = b.category_Id  where a.hash_key <> b.hash_key")

    rowCountUpdateRecords = dfProdCatUpdated.count()
    dfProdCatUpdated = dfProdCatUpdated.registerTempTable("src_updated_data")

    # selects new records from source
    dfProdCatNew = spark.sql("SELECT a.category_Id,a.category_name,a.category_path,a.parent_category_id,"
                             "a.active_indicator,a.company_cd, a.level_one_id,a.level_one_name,a.level_two_id,"
                             "a.level_two_name,a.level_three_id,a.level_three_name, a.level_four_id,a.level_four_name,"
                             "a.level_five_id,a.level_five_name,a.level_six_id,a.level_six_name, a.level_seven_id,"
                             "a.level_seven_name,a.level_eight_id,a.level_eight_name,a.level_nine_id,a.level_nine_name,"
                             " a.level_ten_id,a.level_ten_name,a.hash_key, a.year,a.month FROM CurrentSourceTempTable a"
                             " LEFT OUTER JOIN PreviousDataRefinedTable b on a.category_Id = b.category_Id  where "
                             "b.category_Id is null")

    rowCountNewRecords = dfProdCatNew.count()
    dfProdCatNew = dfProdCatNew.registerTempTable("src_new_data")

    print('Updated prod_cat ids are')
    dfProdCatUpdatedPrint = spark.sql("select category_Id from src_updated_data")
    print(dfProdCatUpdatedPrint.show(100, truncate=False))

    print('New added prod_cat ids are')
    dfProdCatNewPrint = spark.sql("select category_Id from src_new_data")
    print(dfProdCatNewPrint.show(100, truncate=False))

    # union all extracted records
    final_cdc_data = spark.sql(" SELECT category_Id,category_name,category_path,parent_category_id,active_indicator,"
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
            save(OutputPath + '/' + 'ProductCategory' + '/' + 'Working')

        final_cdc_data.coalesce(1).select('category_Id', 'category_name', 'category_path', 'parent_category_id',
                                          'active_indicator', 'company_cd', 'level_one_id', 'level_one_name',
                                          'level_two_id', 'level_two_name', 'level_three_id', 'level_three_name',
                                          'level_four_id', 'level_four_name', 'level_five_id', 'level_five_name',
                                          'level_six_id', 'level_six_name', 'level_seven_id', 'level_seven_name',
                                          'level_eight_id', 'level_eight_name', 'level_nine_id', 'level_nine_name',
                                          'level_ten_id', 'level_ten_name', 'hash_key', 'year', 'month').write.\
            mode("append").partitionBy('year', 'month').format('parquet').save(OutputPath + '/' + 'ProductCategory')
    else:
        print("No changes in the source file, not creating any files in the Refined layer")

else:
    lastModifiedFileName = ''

    print("No files found in refined layer, writing the source data to refined")

    SourceDataDF.coalesce(1).\
        select('category_Id', 'category_name', 'category_path', 'parent_category_id', 'active_indicator', 'company_cd',
               'level_one_id', 'level_one_name', 'level_two_id', 'level_two_name', 'level_three_id', 'level_three_name',
               'level_four_id', 'level_four_name', 'level_five_id', 'level_five_name', 'level_six_id', 'level_six_name',
               'level_seven_id', 'level_seven_name', 'level_eight_id', 'level_eight_name', 'level_nine_id',
               'level_nine_name', 'level_ten_id', 'level_ten_name', 'hash_key').\
        write.mode("overwrite").\
        format('parquet').\
        save(OutputPath + '/' + 'ProductCategory' + '/' + 'Working')

    SourceDataDF.coalesce(1).select('category_Id', 'category_name', 'category_path', 'parent_category_id',
                                    'active_indicator', 'company_cd', 'level_one_id', 'level_one_name', 'level_two_id',
                                    'level_two_name', 'level_three_id', 'level_three_name', 'level_four_id',
                                    'level_four_name', 'level_five_id', 'level_five_name', 'level_six_id',
                                    'level_six_name', 'level_seven_id', 'level_seven_name', 'level_eight_id',
                                    'level_eight_name', 'level_nine_id', 'level_nine_name', 'level_ten_id',
                                    'level_ten_name', 'hash_key', 'year', 'month').write.mode('append').\
        partitionBy('year', 'month').format('parquet').save(OutputPath + '/' + 'ProductCategory')

########################################################################################################################

spark.stop()
