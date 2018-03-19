from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, year, from_unixtime, unix_timestamp
import sys
import os
import csv
from urlparse import urlparse
import boto3


class ProductCategoryCSVToParquet(object):

    def __init__(self):

        self.prodCatInputPath = sys.argv[1]
        self.outputPath = sys.argv[2]
        self.dataProcessingErrorPath = sys.argv[3] + '/discovery'
        self.discoveryBucket = self.outputPath[self.outputPath.index('tb'):].split("/")[0]

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.fileHasDataFlag = 0
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.fileFormat = ".csv"
        self.productCategoryName = self.outputPath[self.outputPath.index('tb'):].split("/")[1]
        self.productCategoryFileColumnCount = 5
        self.productCategoryFile, self.productCategoryHeader = self.searchFile(self.prodCatInputPath)
        self.log.info(self.productCategoryFile)
        self.log.info(self.productCategoryHeader)

        self.productCategoryFilePartitionPath = 's3://' + self.discoveryBucket + '/' + self.productCategoryName

    def searchFile(self, strS3url):

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = self.s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        filePath = ''
        fileName = ''
        file = ''
        body = ''
        header = ''
        for s3Object in objs:
            path, filename = os.path.split(s3Object.key)
            filePath = path
            fileName = filename
            file = "s3://" + bucket + "/" + s3Object.key
            body = s3Object.get()['Body'].read()
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 0:
                header = line
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def isValidFormatInSource(self):

        productCategoryFileName, productCategoryFileExtension = os.path.splitext(os.path.basename(self.productCategoryFile))

        isValidProductCategoryFormat = self.fileFormat in productCategoryFileExtension

        if all([isValidProductCategoryFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("Product Category column count " + str(self.productCategoryHeader.__len__()))

        isValidProductCategorySchema = False

        if self.productCategoryHeader.__len__() >= self.productCategoryFileColumnCount:
            isValidProductCategorySchema = True

        self.log.info("isValidProductCategorySchema " + isValidProductCategorySchema.__str__())

        if all([isValidProductCategorySchema]):
            return True

        return False

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("ProductCategory Source file not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("ProductCategory Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.productCategoryFile, self.dataProcessingErrorPath + '/' + self.productCategoryName +
                          self.fileFormat)
            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        dfProductCatg = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            load(self.prodCatInputPath).toDF(*self.productCategoryHeader)

        dfProductCatg = dfProductCatg.withColumnRenamed("ID", "id"). \
            withColumnRenamed("Description", "description"). \
            withColumnRenamed("CategoryPath", "categorypath"). \
            withColumnRenamed("ParentID", "parentid"). \
            withColumnRenamed("Enabled", "enabled")

        # if dfProductCatg.count() > 1:
        #     print("The product category file has data")
        #     fileHasDataFlag = 1
        # else:
        #     print("The product category file does not have data")
        #     fileHasDataFlag = 0

        # if fileHasDataFlag == 1:
        #     print("Csv file loaded into dataframe properly")

        dfProductCatg.dropDuplicates(['id']).registerTempTable("ProdCatTempTable")

        dfProductCatgFinal = self.sparkSession.sql("select cast(id as string),description,categorypath,parentid,enabled"
                                                   " from ProdCatTempTable where id is not null")

        dfProductCatgFinal.coalesce(1). \
            select('id', 'description', 'categorypath', 'parentid', 'enabled'). \
            write.mode("overwrite"). \
            format('parquet'). \
            save(self.outputPath)

        dfProductCatgFinal.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month'). \
            format('parquet').\
            save(self.productCategoryFilePartitionPath)

        # else:
        #     print("ERROR : Loading csv file into dataframe")

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCategoryCSVToParquet().loadParquet()
