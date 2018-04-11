from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DecimalType, BooleanType
from pyspark.sql.functions import regexp_replace, rtrim
import csv
import boto3
from urlparse import urlparse
from datetime import datetime


class EmpOprEffCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.OperationalEfficiencyScoreCard = sys.argv[1]
        self.OperationalEfficiencyScoreCardOutputArg = sys.argv[2]
        self.EmpOprEffMasterList = sys.argv[1]
        self.EmpOprEffOutput = sys.argv[2]
        self.discoveryBucketWorking = sys.argv[3]
        self.rawBucket = self.EmpOprEffMasterList[self.EmpOprEffMasterList.index('tb'):].split("/")[0]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index(
            'tb'):].split("/")[0]
        self.EmpOprEffName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.dataProcessingErrorPath = sys.argv[4] + '/Discovery/'
        self.EmpOprEffName = "EmployeeOperationalEfficiency"
        self.fileFormat = ".csv"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.EmpOprEffFileColumnCount = 22
        # #
        self.EmpOprEffFile, self.EmpOprEffHeader = self.searchFile(self.EmpOprEffMasterList, 0)
        self.EmpOprEffFileName, self.EmpOprEffFileExtension = os.path.splitext(os.path.basename(self.EmpOprEffFile))
        if self.fileFormat not in self.EmpOprEffFileExtension:
            self.log.error("EmpOprEff Source file not in csv format.")
            self.copyFile(self.EmpOprEffFile, self.dataProcessingErrorPath + self.EmpOprEffName + datetime.now().strftime('%Y%m%d%H%M') + self.EmpOprEffFileExtension)
            sys.exit()
        # #
        self.sourceExpectedSourceColumns = ['market', 'region', 'district', 'location', 'sales person', 'workday id', 'total loss', 'total issues', 'action taken', 'hr consulted before termination', 'transaction errors', 'total errors', 'next trades', 'total devices', 'hyla loss', 'total devices', 'denied rma devices', 'total devices', 'cash deposits', 'total missing deposits', 'total short deposits', 'shrinkage', 'comments', 'date']
        self.EmpOprEffMasterListFile, self.EmpOprEffHeader = self.searchFile(self.EmpOprEffMasterList, self.EmpOprEffName)
        self.log.info(self.EmpOprEffMasterListFile)
        self.log.info("EmployeeOperationalEfficiency Columns:" + ','.join(self.EmpOprEffHeader))
        self.EmpOprEffPartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.EmpOprEffName + '/'
        self.EmpOprEffWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.EmpOprEffName + '/' + self.workingName

    def copyFile(self, strS3url, newS3PathURL):

        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url, isFileHeader=1):

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
        if isFileHeader == 0:
            return file, header
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 1:
                header = line
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def isValidFormatInSource(self):

        EmpOprEffFileName, EmpOprEffFileExtension = os.path.splitext(os.path.basename(self.EmpOprEffMasterListFile))

        isValidEmpOprEffFormat = self.fileFormat in EmpOprEffFileExtension

        if all([isValidEmpOprEffFormat]):
            return True
        return False

    def isValidSchemaInSource(self):
        isValidSchema = False
        header = [x.lower().strip() for x in self.EmpOprEffHeader]

        sourceColumnsMissing = [item for item in self.sourceExpectedSourceColumns if item not in header]
        if sourceColumnsMissing.__len__() <= 0:
            isValidSchema = True

        self.log.info("============EmployeeOperationalEfficiency isValidSchema " + isValidSchema.__str__())

        if all([isValidSchema]):
            return True

        return False

    def remove_some_chars(col_name):
                        return regexp_replace(col_name, '\\(|\\)|$', '').cast(DecimalType())

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error(" Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error(" Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.error("Copy the source files to data processing error path and return.")
            self.copyFile(self.EmpOprEffMasterListFile, self.dataProcessingErrorPath + self.EmpOprEffName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)

            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        schema = StructType([StructField('Market', StringType(), True), StructField('Region', StringType(), True), StructField('District', StringType(), True), StructField('Location', StringType(), True), StructField('Sales Person', StringType(), True), StructField('WorkDay ID', StringType(), False), StructField('Total Loss', StringType(), False), StructField('Total Issues', StringType(), False), StructField('Action Taken', StringType(), False), StructField('HR Consulted Before Termination', StringType(), False), StructField('Transaction Errors', StringType(), False), StructField('Total Errors', StringType(), False), StructField('NEXT Trades', StringType(), False), StructField('Total Devices12', StringType(), False), StructField('Hyla Loss', StringType(), False), StructField('Total Devices14', StringType(), False), StructField('Denied RMA Devices', StringType(), False), StructField('Total Devices16', StringType(), False), StructField('Cash Deposits', StringType(), False), StructField('Total Missing Deposits', StringType(), False), StructField('Total Short Deposits', StringType(), False), StructField('Shrinkage', StringType(), False), StructField('Comments', StringType(), False), StructField('Date', StringType(), False)])
        df1 = self.sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("multiLine", "true").option("delimiter", ",").option("quotechar", '"').option("treatEmptyValuesAsNulls", "true").option("escape", ",").option("escape", '"').option("encoding", "UTF-8").option("inferSchema", "false").load(self.OperationalEfficiencyScoreCard, schema=schema)
        df1 = df1.filter(~(df1.Market.like('%%Marke%%')))
        df1 = df1.filter(~(df1.Market.like('%%Operational%%')))
        df1 = df1.filter(~(df1.Market.like(' Regio%%')))
        df1 = df1.where(df1.Market != '')
        df1 = df1.where(df1.Region != '')
        df1 = df1.where(df1.District != '')
        df1 = df1.withColumnRenamed("Market", "market").withColumnRenamed("Region", "region").withColumnRenamed("District", "district").withColumnRenamed("Location", "location").withColumnRenamed("Sales Person", "salesperson").withColumnRenamed("WorkDay ID", "workdayid").withColumnRenamed("Total Loss", "total_loss").withColumnRenamed("Total Issues", "total_issues").withColumnRenamed("Action Taken", "actiontaken").withColumnRenamed("HR Consulted Before Termination", "hrconsultedbefore_termination").withColumnRenamed("Transaction Errors", "transaction_errors").withColumnRenamed("Total Errors", "total_errors").withColumnRenamed("NEXT Trades", "next_trades").withColumnRenamed("Total Devices12", "total_devices12").withColumnRenamed("Hyla Loss", "hyla_loss").withColumnRenamed("Total Devices14", "total_devices14").withColumnRenamed("Denied RMA Devices", "denied_rmadevices").withColumnRenamed("Total Devices16", "total_device16").withColumnRenamed("Cash Deposits", "cash_deposits").withColumnRenamed("Total Missing Deposits", "totalmissing_deposits").withColumnRenamed("Total Short Deposits", "totalshort_deposits").withColumnRenamed("Shrinkage", "Shrinkage").withColumnRenamed("Comments", "comments").withColumnRenamed("Date", "reportdate")

        df1 = df1.withColumn("totalloss", rtrim(regexp_replace("total_loss", '\\(|\\)|\\$|\\,', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totalissues", df1["total_issues"].cast(IntegerType()))
        df1 = df1.withColumn("hrconsultedbeforetermination", df1["hrconsultedbefore_termination"].cast(BooleanType()))
        df1 = df1.withColumn("transactionerrors", rtrim(regexp_replace("transaction_errors", '\\(|\\)|\\$', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totalerrors", df1["total_errors"].cast(IntegerType()))
        df1 = df1.withColumn("nexttrades", rtrim(regexp_replace("next_trades", '\\(|\\)|\\$', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totaldevices12", df1["total_devices12"].cast(IntegerType()))
        df1 = df1.withColumn("hylaloss", rtrim(regexp_replace("hyla_loss", '\\(|\\)|\\$', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totaldevices14", df1["total_devices14"].cast(IntegerType()))
        df1 = df1.withColumn("deniedrmadevices", rtrim(regexp_replace("denied_rmadevices", '\\(|\\)|\\$', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totaldevice16", df1["total_device16"].cast(IntegerType()))
        df1 = df1.withColumn("cashdeposits", rtrim(regexp_replace("cash_deposits", '\\(|\\)|\\$', '')).cast(DecimalType(15, 2)))
        df1 = df1.withColumn("totalmissingdeposits", df1["totalmissing_deposits"].cast(IntegerType()))
        df1 = df1.withColumn("totalshortdeposits", df1["totalshort_deposits"].cast(IntegerType()))
        df1 = df1.withColumn("shrinkage", rtrim(regexp_replace("Shrinkage", '\\(|\\)|\\$|\\,', '')).cast(DecimalType(15, 2)))

        df1.registerTempTable("empopreff")
        df2 = self.sparkSession.sql("select e.market , e.region,e.district, e.location, e.salesperson, e.workdayid, e.totalloss, e.totalissues, e.actiontaken,e.hrconsultedbeforetermination , e.transactionerrors, e.totalerrors, e.nexttrades, e.totaldevices12, e.hylaloss, e.totaldevices14, e.deniedrmadevices, e.totaldevice16, e.cashdeposits, e.totalmissingdeposits, e.totalshortdeposits, e.shrinkage, e.comments, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month, e.reportdate from empopreff e ")

        df2.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(self.OperationalEfficiencyScoreCardOutputArg)
        df2.coalesce(1).select("*").write.mode("overwrite").parquet(self.OperationalEfficiencyScoreCardOutputArg + '/' + 'Working')
        self.sparkSession.stop()


if __name__ == "__main__":
        EmpOprEffCSVToParquet().loadParquet()
