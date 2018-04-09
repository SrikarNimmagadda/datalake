from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime
from pyspark.sql.functions import lit, year, from_unixtime, unix_timestamp, substring
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
            if i == 0:
                header = line

        return file, header

    def loadParquet(self):

        file, fileHeader = self.searchFile(self.EmpTransAdjIn)
        nonTransposeCols = ['Market', 'Region', 'District', 'Location', 'Loc', 'SalesPerson', 'SalesPersonID']
        empTrnasAdjCols = list(filter(None, fileHeader))
        empTrnasAdjCols = [column.replace(' ', '') for column in empTrnasAdjCols]
        empTrnasAdjCols = [column + '_wxyz' if column not in nonTransposeCols else column.lower() for index, column in enumerate(empTrnasAdjCols)]

        self.logger.info("Employee Transaction Columns : " + ','.join(empTrnasAdjCols))

        dfEmpTrnasAdj = self.sparkSession.read.format("com.databricks.spark.csv").\
            option("header", "true").\
            option("treatEmptyValuesAsNulls", "true").\
            option("inferSchema", "true").\
            load(self.EmpTransAdjIn).toDF(*empTrnasAdjCols)

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

        dfEmpTrnasAdj.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).write.mode('append').partitionBy('year', 'month').format('parquet').save(self.EmpTransAdjOut)

        dfEmpTrnasAdj.coalesce(1).select("*").write.mode("overwrite").parquet(self.EmpTransAdjOut + '/' + 'Working')

        self.sparkSession.stop()


if __name__ == "__main__":
    EmpTransAdj().loadParquet()
