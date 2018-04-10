from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DoubleType
from pyspark.sql.functions import regexp_replace
import boto3
from urlparse import urlparse
import csv


class SalesDetailsCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.salesDetailsMasterList = sys.argv[1]
        self.salesDetailsOutput = sys.argv[2]
        self.discoveryBucketWorking = sys.argv[3]
        self.dataProcessingErrorPath = sys.argv[4]

        self.rawBucket = self.salesDetailsMasterList[self.salesDetailsMasterList.index('tb'):].split("/")[0]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.salesDetailsName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.dataProcessingErrorPath = sys.argv[4]

        self.salesDetailsName = "SalesDetails"
        self.fileFormat = ".csv"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.salesDetailsFileColumnCount = 17

        self.salesDetailsMasterListFile, self.salesDetailsHeader = self.searchFile(self.salesDetailsMasterList, self.salesDetailsName)
        self.log.info(self.salesDetailsMasterListFile)
        self.log.info("SalesDetails Columns:" + ','.join(self.salesDetailsHeader))

        self.salesDetailsPartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.salesDetailsName + '/'
        self.salesDetailsWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.salesDetailsName + '/' + self.workingName

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url, name):

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
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
                if i == 0:
                    header = line

        return file, header

    def isValidFormatInSource(self):

        salesDetailsFileName, salesDetailsFileExtension = os.path.splitext(os.path.basename(self.salesDetailsMasterListFile))

        isValidSalesDetailsFormat = self.fileFormat in salesDetailsFileExtension

        if all([isValidSalesDetailsFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        self.log.info("Customer column count " + str(self.salesDetailsHeader.__len__()))

        isValidSalesDetailsSchema = False

        if self.salesDetailsHeader.__len__() >= self.salesDetailsFileColumnCount:
            isValidSalesDetailsSchema = True

        self.log.info("isValidSalesDetailsSchema " + isValidSalesDetailsSchema.__str__())

        if all([isValidSalesDetailsSchema]):
            return True

        return False

    def loadParquet(self):
        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("SalesDetails Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("SalesDetails Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.salesDetailsMasterListFile, self.dataProcessingErrorPath + '/' + self.salesDetailsName +
                          self.fileFormat)

            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        schema = StructType([StructField('ChannelName', StringType(), True), StructField('StoreID', IntegerType(), True), StructField('StoreName', StringType(), True), StructField('CustomerID', IntegerType(), False), StructField('CustomerName', StringType(), False), StructField('EmployeeID', IntegerType(), True), StructField('DateCreated', StringType(), False), StructField('InvoiceNumber', StringType(), True), StructField('LineID', IntegerType(), False), StructField('ProductSKU', StringType(), True), StructField('Price', DoubleType(), False), StructField('Cost', DoubleType(), False), StructField('RQPriority', IntegerType(), False), StructField('Quantity', IntegerType(), False), StructField('EmployeeName', StringType(), True), StructField('SerialNumber', StringType(), False), StructField('SpecialProductID', IntegerType(), False)])

        # dfSalesDetails = self.sparkSession.createDataFrame(self.sparkSession.sparkContext.emptyRDD(), schema)
        dfSalesDetails = self.sparkSession.read.format("com.databricks.spark.csv").\
            option("header", "true").\
            option("treatEmptyValuesAsNulls", "true").\
            load(self.salesDetailsMasterListFile, schema=schema)

        dfSalesDetails = dfSalesDetails.withColumnRenamed("ChannelName", "channelname").\
            withColumnRenamed("StoreID", "storeid").\
            withColumnRenamed("StoreName", "storename").\
            withColumnRenamed("CustomerID", "customerid").\
            withColumnRenamed("CustomerName", "customername").\
            withColumnRenamed("EmployeeID", "employeeid").\
            withColumnRenamed("DateCreated", "datecreated").\
            withColumnRenamed("InvoiceNumber", "invoicenumber").\
            withColumnRenamed("LineID", "lineid").\
            withColumnRenamed("ProductSKU", "productsku").\
            withColumnRenamed("Price", "price").\
            withColumnRenamed("Cost", "cost").\
            withColumnRenamed("RQPriority", "rqpriority").\
            withColumnRenamed("Quantity", "quantity").\
            withColumnRenamed("EmployeeName", "employeename").\
            withColumnRenamed("SerialNumber", "serialnumber").\
            withColumnRenamed("SpecialProductID", "specialproductid")
        dfSalesDetails.registerTempTable("SalesDetails")
        dfSalesDetailsFinal2 = self.sparkSession.sql("select a.channelname, a.storeid, a.storename, a.customerid, a.customername, a.employeeid, SUBSTRING(a.datecreated, 1, 19) as datecreated2, a.invoicenumber, a.lineid, a.productsku, a.price, a.cost, a.rqpriority, a.quantity, a.employeename, a.serialnumber, a.specialproductid,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()), 6, 2) as month from SalesDetails a")
        dfSalesDetailsFinal2.printSchema()
        dfSalesDetailsFinal2 = dfSalesDetailsFinal2.withColumn('datecreated', regexp_replace('datecreated2', 'T', ' '))
        dfSalesDetailsFinal = dfSalesDetailsFinal2.drop('datecreated2')
        dfSalesDetailsFinal.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(self.salesDetailsPartitionFilePath)

        dfSalesDetailsFinal.coalesce(1).select("*").write.mode("overwrite").parquet(self.salesDetailsWorkingFilePath)
        self.sparkSession.stop()


if __name__ == "__main__":

    SalesDetailsCSVToParquet().loadParquet()
