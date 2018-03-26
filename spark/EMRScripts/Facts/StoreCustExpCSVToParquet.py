import sys
import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import boto3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class StoreCustExpCSVToParquet(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')

        self.OutputPath = sys.argv[1]
        self.InputPath = sys.argv[2]

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
            body = s3Object.get()['Body'].read().decode('utf-8')
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 1:
                header = line
        self.logger.info('File name ' + fileName + ' exists in path  ' + filePath)
        return file, header

    def loadParquet(self):

        storeCustExpFileCheck = 0

        workingPath = 's3://' + self.InputPath + '/StoreCustomerExperience/Working'
        file, fileHeader = self.searchFile(workingPath)
        fileName, fileExtension = os.path.splitext(os.path.basename(file))

        newformat = datetime.strptime(str(fileName.split("_")[-1]), '%Y%m%d%H%M').strftime('%m/%d/%Y')
        self.logger.info(newformat)

        if fileExtension == ".csv":
            storeCustExpFileCheck = 1

        if storeCustExpFileCheck == 1:
            self.logger.info("Raw file is in csv format, proceeding with the logic")

            dfStoreCustExp = self.spark.read.format("com.databricks.spark.csv"). \
                option("header", "true"). \
                option("treatEmptyValuesAsNulls", "true"). \
                option("inferSchema", "true"). \
                load('s3://' + self.InputPath + '/StoreCustomerExperience/Working')

            dfStoreCustExpCnt = dfStoreCustExp.count()

            if dfStoreCustExpCnt > 1:
                self.logger.info("The store customer experience file has data")
                fileHasDataFlag = 1
            else:
                self.logger.info("The store customer experience file does not have data")
                fileHasDataFlag = 0

            if fileHasDataFlag == 1:
                self.logger.info("Csv file loaded into dataframe properly")

                dfStoreCustExp.withColumnRenamed("Location", "location").\
                    withColumnRenamed("5 Key Behaviors", "five_key_behaviours"). \
                    withColumnRenamed("Effective Solutioning", "effective_solutioning"). \
                    withColumnRenamed("Integrated Experience", "integrated_experience"). \
                    withColumnRenamed("GroupID", "group_id").\
                    withColumn('report_date', lit(newformat)).\
                    registerTempTable("StoreCustExpTempTable")

                dfStoreCustExpFinal = self.spark.sql(
                    "select location,five_key_behaviours,effective_solutioning,integrated_experience,group_id,report_date,"
                    "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
                    " from StoreCustExpTempTable "
                )

                dfStoreCustExpFinal.coalesce(1).select('location', 'five_key_behaviours', 'effective_solutioning',
                                                       'integrated_experience', 'group_id', 'report_date'). \
                    write.mode("overwrite"). \
                    parquet(self.OutputPath + '/' + 'StoreCustomerExperience' + '/' + 'Working')

                dfStoreCustExpFinal.coalesce(1). \
                    write.mode('append').partitionBy('year', 'month'). \
                    format('parquet').save(self.OutputPath + '/' + 'StoreCustomerExperience')

            else:
                self.logger.error("ERROR : Loading csv file into dataframe")

        else:
            self.logger.error("ERROR : Raw file is not in csv format")

        self.spark.stop()


if __name__ == "__main__":
    StoreCustExpCSVToParquet().loadParquet()
