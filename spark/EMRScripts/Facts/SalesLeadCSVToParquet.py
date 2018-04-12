from pyspark.sql.functions import regexp_replace
from datetime import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql import SparkSession, SQLContext
import sys
import os
import boto3
from urlparse import urlparse
import csv


class SalesLeadCSVToParquet(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.salesLeadMasterList = sys.argv[1]
        self.salesLeadOutput = sys.argv[2]
        self.discoveryBucketWorking = sys.argv[3]
        # self.dataProcessingErrorPath = sys.argv[4]
        self.rawBucket = self.salesLeadMasterList[self.salesLeadMasterList.index('tb'):].split("/")[0]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index(
            'tb'):].split("/")[0]
        self.salesLeadName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.dataProcessingErrorPath = sys.argv[4] + '/Discovery/'
        self.salesLeadName = "SalesLeads"
        self.fileFormat = ".csv"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        # #
        self.salesLeadFile, self.salesLeadHeader = self.searchFile(self.salesLeadMasterList, 0)
        self.salesLeadFileName, self.salesLeadFileExtension = os.path.splitext(os.path.basename(self.salesLeadFile))
        if self.fileFormat not in self.salesLeadFileExtension:
            self.log.error("salesLead Source file not in csv format.")
            self.copyFile(self.salesLeadFile, self.dataProcessingErrorPath + self.salesLeadName + datetime.now().strftime('%Y%m%d%H%M') + self.salesLeadFileExtension)
            sys.exit()
        # #
        self.sourceExpectedSourceColumns = ['master_dealer', 'region', 'market', 'dos', 'arsm', 'store_name', 'store_id', 'account', 'description', 'contact_info', 'win_the_neighborhood', 'customer_zip', 'existing_customer', 'fan', 'rep_name', 'rep_dealer_code', 'status', 'gross_adds', 'sb_assistance_requested', 'enter_date', 'close_date', 'follow_up_notes', 'firstnet', 'last_update', 'dealer code', 'spring region', 'spring market', 'spring district', 'bae']

        self.salesLeadMasterListFile, self.salesLeadHeader = self.searchFile(self.salesLeadMasterList, self.salesLeadName)
        self.log.info(self.salesLeadMasterListFile)
        self.log.info("salesLead Columns:" + ','.join(self.salesLeadHeader))
        self.salesLeadPartitionFilePath = 's3://' + self.discoveryBucket + '/' + self.salesLeadName + '/'
        self.salesLeadWorkingFilePath = 's3://' + self.discoveryBucket + '/' + self.salesLeadName + '/' + self.workingName

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
            if i == 9:
                header = line
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def isValidFormatInSource(self):

        salesLeadFileName, salesLeadFileExtension = os.path.splitext(os.path.basename(self.salesLeadMasterListFile))

        isValidsalesLeadFormat = self.fileFormat in salesLeadFileExtension

        if all([isValidsalesLeadFormat]):
            return True
        return False

    def isValidSchemaInSource(self):

        isValidSchema = False
        header = [x.lower().strip() for x in self.salesLeadHeader]

        sourceColumnsMissing = [item for item in self.sourceExpectedSourceColumns if item not in header]
        if sourceColumnsMissing.__len__() <= 0:
            isValidSchema = True

        self.log.info("============salesLead isValidSchema " + isValidSchema.__str__())

        if all([isValidSchema]):
            return True

        return False

    def loadParquet(self):

        self.log.info('Exception Handling starts')

        validSourceFormat = self.isValidFormatInSource()

        if not validSourceFormat:
            self.log.error("Salesleads Source files not in csv format.")

        validSourceSchema = self.isValidSchemaInSource()
        if not validSourceSchema:
            self.log.error("Salesleads Source schema does not have all the required columns.")

        if not validSourceFormat or not validSourceSchema:
            self.log.info("Copy the source files to data processing error path and return.")
            self.copyFile(self.salesLeadMasterListFile, self.dataProcessingErrorPath + self.salesLeadName + datetime.now().strftime('%Y%m%d%H%M') + self.fileFormat)

            return

        self.log.info('Source format and schema validation successful.')
        self.log.info('Reading the input parquet file')

        ###################################
        schema = StructType([StructField('Master_Dealer', StringType(), False), StructField('Region', StringType(), True), StructField('Market', StringType(), True), StructField('DOS', StringType(), True), StructField('ARSM', StringType(), True), StructField('Store_Name', StringType(), True), StructField('Store_ID', StringType(), True), StructField('Account', StringType(), True), StructField('Description', StringType(), True), StructField('Contact_Info', StringType(), True), StructField('Win_The_Neighborhood', StringType(), True), StructField('Customer_ZIP', StringType(), True), StructField('Existing_Customer', StringType(), True), StructField('FAN', StringType(), True), StructField('Rep_Name', StringType(), True), StructField('Rep_Dealer_Code', StringType(), True), StructField('Status', StringType(), True), StructField('Gross_Adds', StringType(), True), StructField('SB_Assistance_Requested', StringType(), True), StructField('Enter_Date', StringType(), True), StructField('Close_Date', StringType(), True), StructField('Follow_Up_Notes', StringType(), True), StructField('FirstNet', StringType(), True), StructField('Last_Update', StringType(), True), StructField('Dealer Code', StringType(), True), StructField('Spring Market', StringType(), True), StructField('Spring Region', StringType(), True), StructField('Spring District', StringType(), True), StructField('BAE', StringType(), True)])
        sqlContext = SQLContext(self.sparkSession.sparkContext)
        dfSalesLead = (sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("multiLine", "true").option("delimiter", ",").option("quotechar", '"').option("escape", ",").option("escape", '"').option("encoding", "UTF-8").load(self.salesLeadMasterList, schema=schema))

        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('Navigat%%')))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('1. Click on buttons to drill down %%')))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('2. To Click on multiple%%')))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('Click on 1st button%%')))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('3. To clear your %%')))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.Account.like('Account')))
        dfSalesLead = dfSalesLead.where(dfSalesLead.Master_Dealer != '')

        dfSalesLead = dfSalesLead.withColumn('Master_Dealer', regexp_replace(col('Master_Dealer'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Region', regexp_replace(col('Region'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Market', regexp_replace(col('Market'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('DOS', regexp_replace(col('DOS'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('ARSM', regexp_replace(col('ARSM'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Store_Name', regexp_replace(col('Store_Name'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Store_ID', regexp_replace(col('Store_ID'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Account', regexp_replace(col('Account'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Description', regexp_replace(col('Description'), '[\\r\\n\\\\\/\\"\\\"\\\""\\"""""]', ' '))
        dfSalesLead = dfSalesLead.withColumn('Contact_Info', regexp_replace(col('Contact_Info'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Win_The_Neighborhood', regexp_replace(col('Win_The_Neighborhood'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Customer_ZIP', regexp_replace(col('Customer_ZIP'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Existing_Customer', regexp_replace(col('Existing_Customer'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('FAN', regexp_replace(col('FAN'), "[\	\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Rep_Name', regexp_replace(col('Rep_Name'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Rep_Dealer_Code', regexp_replace(col('Rep_Dealer_Code'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Status', regexp_replace(col('Status'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Gross_Adds', regexp_replace(col('Gross_Adds'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('SB_Assistance_Requested', regexp_replace(col('SB_Assistance_Requested'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Enter_Date', regexp_replace(col('Enter_Date'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Close_Date', regexp_replace(col('Close_Date'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Follow_Up_Notes', regexp_replace(col('Follow_Up_Notes'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('FirstNet', regexp_replace(col('FirstNet'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Last_Update', regexp_replace(col('Last_Update'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Spring Market', regexp_replace(col('Spring Market'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Spring Region', regexp_replace(col('Spring Region'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Spring District', regexp_replace(col('Spring District'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('Dealer Code', regexp_replace(col('Dealer Code'), "[\\r\\n]", ' '))
        dfSalesLead = dfSalesLead.withColumn('BAE', regexp_replace(col('BAE'), "[\\r\\n]", ' '))

        dfSalesLead.printSchema()
        dfSalesLead = dfSalesLead.withColumnRenamed("Master_Dealer", "masterdealer").withColumnRenamed("Region", "region").withColumnRenamed("Market", "market").withColumnRenamed("DOS", "dos").withColumnRenamed("ARSM", "arsm").withColumnRenamed("Store_Name", "storename").withColumnRenamed("Store_ID", "store_id").withColumnRenamed("Account", "account").   withColumnRenamed("Description", "description").withColumnRenamed("Contact_Info", "contactinfo").withColumnRenamed("Win_The_Neighborhood", "wintheneighborhood").withColumnRenamed("Customer_ZIP", "customerzip").withColumnRenamed("Existing_Customer", "existingcustomer").withColumnRenamed("FAN", "fan").withColumnRenamed("Rep_Name", "repname").withColumnRenamed("Rep_Dealer_Code", "repdealercode").withColumnRenamed("Status", "status").withColumnRenamed("Gross_Adds", "gross_adds").withColumnRenamed("SB_Assistance_Requested", "sbassistance_requested").withColumnRenamed("Enter_Date", "enterdate").withColumnRenamed("Close_Date", "closedate").withColumnRenamed("Follow_Up_Notes", "followupnotes").withColumnRenamed("FirstNet", "first_net").withColumnRenamed("Last_Update", "lastupdate").withColumnRenamed("Dealer Code", "dealercode").withColumnRenamed("Spring Market", "springmarket").withColumnRenamed("Spring Region", "springregion").withColumnRenamed("Spring District", "springdistrict").withColumnRenamed("BAE", "bae")

        today = datetime.now().strftime('%m/%d/%Y')
        dfSalesLead = dfSalesLead.withColumn('report_date', lit(today))
        dfSalesLead = dfSalesLead.withColumn("storeid", dfSalesLead["store_id"].cast(IntegerType()))
        dfSalesLead = dfSalesLead.withColumn("firstnet", dfSalesLead["first_net"].cast(IntegerType()))
        dfSalesLead = dfSalesLead.withColumn("grossadds", dfSalesLead["gross_adds"].cast(IntegerType()))
        dfSalesLead = dfSalesLead.withColumn("sbassistancerequested", dfSalesLead["sbassistance_requested"].cast(IntegerType()))
        dfSalesLead = dfSalesLead.filter(~(dfSalesLead.dealercode.like('#N/A')))
        dfSalesLead.printSchema()
        dfSalesLead.registerTempTable("SalesLead")
        dfSalesLeadFinal1 = self.sparkSession.sql("select * from SalesLead")
        dfSalesLeadFinal1.registerTempTable("SalesLeadFinal1")
        dfSalesLeadFinal = self.sparkSession.sql("select a.report_date as reportdate, a.masterdealer, a.region,a.market, a.dos,a.arsm, a.storename,cast(a.storeid as	int), a.account, a.description, a.contactinfo, a.customerzip, a.existingcustomer, a.fan, a.repname, a.repdealercode, a.status,cast(a.grossadds as int), cast(a.sbassistancerequested as int), a.enterdate, a.closedate, a.firstnet, a.lastupdate, a.dealercode, a.springmarket, a.springregion, a.springdistrict, a.bae,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from SalesLeadFinal1 a")
        dfSalesLeadFinal.printSchema()

        dfSalesLeadFinal.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(self.salesLeadPartitionFilePath)
        dfSalesLeadFinal.coalesce(1).select("*").write.mode("overwrite").parquet(self.salesLeadWorkingFilePath)

        # ##################################
        self.log.info('SalesLeads: Processing completed Successfully!!!')
        self.sparkSession.stop()


if __name__ == "__main__":
    SalesLeadCSVToParquet().loadParquet()
