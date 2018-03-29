from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime
from pyspark.sql.functions import lit
import boto3
import csv
from urlparse import urlparse


class EmpTransAdj(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.s3 = boto3.resource('s3')
        self.EmpTransAdjOut = sys.argv[1]
        self.EmpTransAdjIn = sys.argv[2]

    def searchFile(self, strS3url):

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = self.s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        file = ''
        body = ''
        header = ''
        for s3Object in objs:
            file = "s3://" + bucket + "/" + s3Object.key
            body = s3Object.get()['Body'].read()
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 1:
                header = line

        return file, header

    def loadParquet(self):

        dfEmpTrnasAdj = self.sparkSession.read.format("com.databricks.spark.csv").\
            option("header", "true").\
            option("treatEmptyValuesAsNulls", "true").\
            option("inferSchema", "true").\
            load(self.EmpTransAdjIn)

        dfEmpTrnasAdj = dfEmpTrnasAdj.withColumnRenamed("Market", "market").\
            withColumnRenamed("Region", "region").\
            withColumnRenamed("District", "district").\
            withColumnRenamed("Location", "location").\
            withColumnRenamed("Loc", "loc").\
            withColumnRenamed("SalesPerson", "salesperson").\
            withColumnRenamed("SalesPersonID", "salespersonid").\
            withColumnRenamed(" DF Accessory Adjustment ", "DFAccessoryAdjustment_wxyz").\
            withColumnRenamed(" Feature Adjustment ", "FeatureAdjustment_wxyz").\
            withColumnRenamed(" Incorrect ActUpg ", "IncorrectActUpg_wxyz").\
            withColumnRenamed(" Incorrect BYOD ", "IncorrectBYOD_wxyz").\
            withColumnRenamed(" Incorrect DF ", "IncorrectDF_wxyz").\
            withColumnRenamed(" Incorrect Plan ", "IncorrectPlan_wxyz").\
            withColumnRenamed("Feature Disputes", "FeatureDisputes_wxyz").\
            withColumnRenamed(" Incorrect Prepaid ", "IncorrectPrepaid_wxyz").\
            withColumnRenamed(" Incorrect Transactions ", "IncorrectTransactions_wxyz").\
            withColumnRenamed(" DirecTV NOW Adjustment ", "DirecTVNOWAdjustment_wxyz").\
            withColumnRenamed(" TV Disputes ", "TVDisputes_wxyz").\
            withColumnRenamed(" TV Extras Adjustment ", "TVExtrasAdjustment_wxyz").\
            withColumnRenamed(" Wired Disputes ", "WiredDisputes_wxyz").\
            withColumnRenamed(" Wired Extras Adjustment ", "WiredExtrasAdjustment_wxyz").\
            withColumnRenamed(" Deposit Adjustment ", "DepositAdjustment_wxyz").\
            withColumnRenamed(" Total Adjustment ", "TotalAdjustment_wxyz")

        file, fileHeader = self.searchFile(self.EmpTransAdjIn)
        fileName, fileExtension = os.path.splitext(os.path.basename(file))
        fields = fileName.split('\\')
        newformat = ''
        if len(fields) > 0:
            filenamefield = fields[len(fields) - 1].split('_')
            if len(filenamefield) > 0:
                self.logger.info("###############################")
                self.logger.info(filenamefield)
                oldformat1 = filenamefield[1]
                self.logger.info(oldformat1)
                oldformat = oldformat1.replace('TransAdjEMP', '')
                newformat = datetime.strptime(str(oldformat), '%Y%m%d').strftime('%m/%d/%Y')

        dfEmpTrnasAdj = dfEmpTrnasAdj.withColumn('reportdate', lit(newformat))

        dfEmpTrnasAdj.registerTempTable("EmpTransAdj")

        FinalDF = self.sparkSession.sql("select market,region,district,location,loc,salesperson,salespersonid,DFAccessoryAdjustment_wxyz,"
                                        "FeatureAdjustment_wxyz,IncorrectActUpg_wxyz,IncorrectBYOD_wxyz,IncorrectDF_wxyz,"
                                        "IncorrectPrepaid_wxyz,IncorrectTransactions_wxyz,DirecTVNOWAdjustment_wxyz,FeatureDisputes_wxyz,"
                                        "TVDisputes_wxyz,TVExtrasAdjustment_wxyz,WiredDisputes_wxyz,IncorrectPlan_wxyz,"
                                        "WiredExtrasAdjustment_wxyz,DepositAdjustment_wxyz,TotalAdjustment_wxyz,reportdate,"
                                        "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from EmpTransAdj")

        FinalDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.EmpTransAdjOut)

        FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.EmpTransAdjOut + '/' + 'Working')

        self.sparkSession.stop()


if __name__ == "__main__":
    EmpTransAdj().loadParquet()
