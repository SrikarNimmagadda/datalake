from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import sys
import os
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField, FloatType
from pyspark.sql.functions import col, lit, regexp_replace
import boto3
import csv
from urlparse import urlparse


class StoreTransactionAdjustments(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.s3 = boto3.resource('s3')
        self.storeTransAdjustmentsOut = sys.argv[1]
        self.storeTransAdjustmentsIn = sys.argv[2]
        self.storeTransAdjustmentsMisc = sys.argv[3]

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
        CONSTANT = 100.0
        schema = StructType([StructField('Market', StringType(), True),
                             StructField('Region', StringType(), True),
                             StructField('District', StringType(), False),
                             StructField('Location', StringType(), True),
                             StructField('Loc', StringType(), True),
                             StructField('ATT|FeaturesCommission', StringType(), True),
                             StructField('RetailIQ|FeaturesCommission', StringType(), True),
                             StructField('Uncollected|FeaturesCommission|1', StringType(), True),
                             StructField('Uncollected|FeaturesCommission%|23', StringType(), True),
                             StructField('Dispute|FeaturesCommission', StringType(), True),
                             StructField('Uncollected|Features', StringType(), True),
                             StructField('Uncollected|%Features|2', StringType(), True),
                             StructField('Wired|Disputes', StringType(), True),
                             StructField('TV|Disputes', StringType(), True),
                             StructField('TV|Extras', StringType(), True),
                             StructField('Wired|Extras', StringType(), True),
                             StructField('AT&T|DTVNOW', StringType(), True),
                             StructField('RetailIQ|DTVNow', StringType(), True),
                             StructField('DirecTVNow|Disputes', StringType(), True),
                             StructField('Uncollected|DirecTVNow|2', StringType(), True),
                             StructField('Incorrect|ActivationorUpgrade', StringType(), True),
                             StructField('Incorrect|DF', StringType(), True),
                             StructField('Incorrect|Prepaid', StringType(), True),
                             StructField('Incorrect|BYOD', StringType(), True),
                             StructField('Incorrect|Total', StringType(), True),
                             StructField('DF|Accessories', StringType(), True),
                             StructField('Total|Adjustment', StringType(), True),
                             StructField('Uncollected|Deposits', StringType(), True),
                             StructField('Total|AdjustmentwithDeposits', StringType(), True)])

        dfStoreTransAdj = self.sparkSession.sparkContext.textFile(self.storeTransAdjustmentsIn).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: line[0] != 'Market' or len(line[1]) != 0).toDF(schema)

        dfStoreTransAdj.printSchema()
        dfStoreTransAdj = dfStoreTransAdj.withColumn('abc1', regexp_replace('Uncollected|FeaturesCommission%|23', '%', '').cast(FloatType()))
        dfStoreTransAdj = dfStoreTransAdj.withColumn('xyz1', regexp_replace('Uncollected|%Features|2', '%', '').cast(FloatType()))
        dfStoreTransAdj = dfStoreTransAdj.withColumn('abc', col('abc1') / CONSTANT)
        dfStoreTransAdj = dfStoreTransAdj.withColumn('xyz', col('xyz1') / CONSTANT)

        dfStoreTransAdj = dfStoreTransAdj.withColumnRenamed("abc", "Uncollected|FeaturesCommission%|2").withColumnRenamed("xyz", "Uncollected|%Features")

        dfStoreTransAdjMisc = self.sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").load(self.storeTransAdjustmentsMisc)

        split_col = split(dfStoreTransAdjMisc['Location'], ' ')

        dfStoreTransAdjMisc = dfStoreTransAdjMisc.withColumn('storenumber', split_col.getItem(0))

        dfStoreTransAdjMisc = dfStoreTransAdjMisc.withColumnRenamed("Location", "location_Miscellaneous").withColumnRenamed("GP Adjustment", "gpadjustments_Miscellaneous").withColumnRenamed("CRU Adjustment", "cruadjustments_Miscellaneous").withColumnRenamed("Acc Elig Opps Adjustment", "acceligoppsadjustment_Miscellaneous").withColumnRenamed("Total Opps Adjustment", "totaloppsadjustment_Miscellaneous")

        dfStoreTransAdjMisc = dfStoreTransAdjMisc.drop("location_zyxwv")

        dfStoreTransAdj1 = dfStoreTransAdj.where(dfStoreTransAdj['Market'] != 'Oct-17')
        dfStoreTransAdj2 = dfStoreTransAdj1.where(dfStoreTransAdj['Market'] != 'Market')

        dfStoreTransAdj3 = dfStoreTransAdj2.drop('abc1')
        dfStoreTransAdj4 = dfStoreTransAdj3.drop('xyz1')
        dfStoreTransAdj5 = dfStoreTransAdj4.drop('Uncollected|FeaturesCommission%|23')
        dfStoreTransAdj6 = dfStoreTransAdj5.drop('Uncollected|%Features|2')

        file, fileHeader = self.searchFile(self.storeTransAdjustmentsIn)
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
                oldformat = oldformat1.replace('TransAdjStore', '')
                newformat = datetime.strptime(str(oldformat), '%Y%m%d').strftime('%m/%d/%Y')

        dfStoreTransAdj6 = dfStoreTransAdj6.withColumn('reportdate', lit(newformat))

        dfStoreTransAdj6.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTransAdjustmentsOut + '/' + 'Working1')
        dfStoreTransAdjMisc.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTransAdjustmentsOut + '/' + 'Working2')

        dfStoreTransAdj6.coalesce(1).select("*").write.mode("append").parquet(self.storeTransAdjustmentsOut + '/' + 'Previous1')
        dfStoreTransAdjMisc.coalesce(1).select("*").write.mode("append").parquet(self.storeTransAdjustmentsOut + '/' + 'Previous2')

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreTransactionAdjustments().loadParquet()
