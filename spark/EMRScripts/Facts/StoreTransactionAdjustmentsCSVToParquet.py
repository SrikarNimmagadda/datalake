from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import sys
import os
from datetime import datetime
from pyspark.sql.functions import lit, regexp_replace
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
        self.separator = '|'
        self.replaceChar = '%'

    def searchFile(self, strS3url):

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = self.s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        file = ''
        body = ''
        header1 = ''
        header2 = ''
        for s3Object in objs:
            file = "s3://" + bucket + "/" + s3Object.key
            body = s3Object.get()['Body'].read()
        for i, line in enumerate(csv.reader(body.splitlines(), delimiter=',', quotechar='"')):
            if i == 0:
                header1 = line
            if i == 1:
                header2 = line
        self.logger.info("Header1 :" + ''.join(header1))
        self.logger.info("Header2 :" + ''.join(header2))
        header1 = [col.strip(' ') for col in header1]
        header2 = [col.strip(' ') for col in header2]
        header = [self.separator.join(w).lstrip(self.separator) for w in zip(header1, header2)]
        self.logger.info("Header :" + ','.join(header))
        return file, header

    def loadParquet(self):

        file, fileHeader = self.searchFile(self.storeTransAdjustmentsIn)
        storeTrnasAdjCols = [column.replace(' ', '').replace('&', '').replace('/', 'OR') for column in fileHeader]
        storeTrnasAdjCols = [column.split(self.separator)[1] if index == 0 else column for index, column in enumerate(storeTrnasAdjCols)]

        self.logger.info("##################################Store Transaction Columns :" + ','.join(storeTrnasAdjCols))
        dateStrInHeader = datetime.now().strftime('%b %Y')
        self.sparkSession.sparkContext.textFile(self.storeTransAdjustmentsIn).mapPartitions(lambda partition: csv.reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: ''.join(line).strip() != '' and line[0].strip() != dateStrInHeader and line[0].strip() != 'Market').toDF(storeTrnasAdjCols).registerTempTable("StoreTransactionAdjustment")

        percentColumns = [x for x in storeTrnasAdjCols if self.replaceChar in x]
        withoutPercentColumns = ["`" + x + "`" for x in storeTrnasAdjCols if x not in percentColumns]
        selectSubQuery = ','.join(["cast(cast(regexp_replace(`" + x + "`,'%','') as float)/100 as decimal(20,8)) as `" + x + "`" for x in percentColumns])
        selectQuery = "select " + ','.join(withoutPercentColumns) + "," + selectSubQuery + " from StoreTransactionAdjustment"
        dfStoreTransAdj = self.sparkSession.sql(selectQuery)

        dfStoreTransAdjMisc1 = self.sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("encoding", "UTF-8").option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", ",").option('escape', '"').load(self.storeTransAdjustmentsMisc)

        split_col = split(dfStoreTransAdjMisc1['Location'], ' ')

        dfStoreTransAdjMisc1 = dfStoreTransAdjMisc1.withColumn('storenumber', split_col.getItem(0))

        dfStoreTransAdjMisc1 = dfStoreTransAdjMisc1.withColumnRenamed("Location", "location_Miscellaneous").withColumnRenamed("GP Adjustment", "gpadjustments_Miscellaneous").withColumnRenamed("CRU Adjustment", "cruadjustments_Miscellaneous").withColumnRenamed("Acc Elig Opps Adjustment", "acceligoppsadjustment_Miscellaneous").withColumnRenamed("Total Opps Adjustment", "totaloppsadjustment_Miscellaneous")

        dfStoreTransAdjMisc1 = dfStoreTransAdjMisc1.drop("location_zyxwv")

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

        dfStoreTransAdj = dfStoreTransAdj.withColumn('reportdate', lit(newformat))
        dfStoreTransAdjMisc1.registerTempTable("MISC")
        dfStoreTransAdjMisc = self.sparkSession.sql("select location_Miscellaneous, gpadjustments_Miscellaneous as gpadjustments_Miscellaneous1, cruadjustments_Miscellaneous, acceligoppsadjustment_Miscellaneous, totaloppsadjustment_Miscellaneous, storenumber, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from MISC")
        dfStoreTransAdjMisc.printSchema()
        dfStoreTransAdjMisc = dfStoreTransAdjMisc.withColumn('gpadjustments_Miscellaneous', regexp_replace('gpadjustments_Miscellaneous1', '$', ''))
        dfStoreTransAdjMisc = dfStoreTransAdjMisc.drop('gpadjustments_Miscellaneous1')
        dfStoreTransAdjMisc.printSchema()
        dfStoreTransAdj.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTransAdjustmentsOut + '/' + 'Working1')
        dfStoreTransAdjMisc.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTransAdjustmentsOut + '/' + 'Working2')
        dfStoreTransAdjMisc.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.storeTransAdjustmentsOut)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreTransactionAdjustments().loadParquet()
