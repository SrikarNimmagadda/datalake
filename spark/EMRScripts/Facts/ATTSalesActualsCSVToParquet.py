from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField, DecimalType
from pyspark.sql.functions import lit, regexp_replace, rtrim, when
import boto3
from urlparse import urlparse


class ATTSalesActualsCSVToParquet(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.AttSalesActualsOutput = sys.argv[1]
        self.AttSalesActualsInput = sys.argv[2]
        self.ATTMyResultsInp2 = sys.argv[3]

    def loadParquet(self):
        print("-------------------Fetching the dates------------------------------------")
        print(self.AttSalesActualsInput)
        s3keys = self.AttSalesActualsInput.split(':')[1].split('//')[1].split('/')
        key = ""
        filefolderPrefix = s3keys[1] + "/" + s3keys[2]
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3keys[0])
        listofObj = list(bucket.objects.filter(Prefix=filefolderPrefix))
        schema = StructType([StructField('Location', StringType(), False), StructField('Loc Id', StringType(), False), StructField('Dlr1 Code', StringType(), False), StructField('KPI', StringType(), False), StructField('KPI ID', StringType(), False), StructField('Dec 2017 Actual', StringType(), True), StructField('Dec 2017 Projected', StringType(), True), StructField('Dec 2017 Target', StringType(), True)])
        dfATTSalesActuals = self.sparkSession.createDataFrame(self.sparkSession.sparkContext.emptyRDD(), schema)
        bucketWithPath = urlparse(self.AttSalesActualsInput)
        bucket = bucketWithPath.netloc
        spark = SparkSession.builder.appName("ATTSalesActuals").getOrCreate()
        for obj in listofObj:
                key = obj.key
                file = "s3://" + bucket + "/" + obj.key
                df1 = self.sparkSession.read.format("com.databricks.spark.csv").\
                    option("header", "true").\
                    option("treatEmptyValuesAsNulls", "true").\
                    option("inferSchema", "false").\
                    load(file, schema=schema)
                df1 = df1.filter(~(df1.Location.like('Histori%%')))
                df1 = df1.filter(~(df1.Location.like('AT&T MyRe%%')))
                df1 = df1.filter(~(df1.Location.like('AR : SPRI%%')))
                df1 = df1.filter(~(df1.Location.like('Generated%%')))
                df1 = df1.filter(~(df1.Location.like('Locat%%')))
                dfATTSalesActuals = df1.unionAll(dfATTSalesActuals)
                dfATTSalesActuals = dfATTSalesActuals.withColumnRenamed("Location", "attlocationname").\
                    withColumnRenamed("Loc Id", "locid").\
                    withColumnRenamed("Dlr1 Code", "dealercode").\
                    withColumnRenamed("KPI", "kpiname").\
                    withColumnRenamed("KPI ID", "kpiid").\
                    withColumnRenamed("Dec 2017 Actual", "actual_value").\
                    withColumnRenamed("Dec 2017 Projected", "projected_value").\
                    withColumnRenamed("Dec 2017 Target", "target_value")

        filedate = key.split("/")[2].split("_")[3].split(".")[1]
        filedate = filedate[0:8]
        filedateS = datetime.strptime(filedate, '%Y%m%d').strftime('%m/%d/%Y')

        #########################################################################################################
        #                                 Reading the source data files                                         #
        #########################################################################################################

        FinalHistDF1 = dfATTSalesActuals
        FinalHistDF1 = FinalHistDF1.withColumn("actualvalue", when(FinalHistDF1.actual_value.contains('%'), rtrim(regexp_replace("actual_value", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("actual_value", '\\$|\\%|\\,', ''))))

        FinalHistDF1 = FinalHistDF1.withColumn("projectedvalue", when(FinalHistDF1.actual_value.contains('%'), rtrim(regexp_replace("projected_value", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("projected_value", '\\$|\\%|\\,', ''))))

        FinalHistDF1 = FinalHistDF1.withColumn("targetvalue", when(FinalHistDF1.actual_value.contains('%'), rtrim(regexp_replace("target_value", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("target_value", '\\$|\\%|\\,', ''))))

        dfATTSalesActualsRPT = spark.read.format("com.databricks.spark.csv").\
            option("header", "true").\
            option("treatEmptyValuesAsNulls", "true").\
            option("inferSchema", "false").\
            load(self.ATTMyResultsInp2, schema=schema)

        dfATTSalesActualsRPT1 = dfATTSalesActualsRPT.filter(~(dfATTSalesActualsRPT.Location.like('Histori%%')))
        dfATTSalesActualsRPT2 = dfATTSalesActualsRPT1.filter(~(dfATTSalesActualsRPT.Location.like('AT&T MyRe%%')))
        dfATTSalesActualsRPT3 = dfATTSalesActualsRPT2.filter(~(dfATTSalesActualsRPT.Location.like('AR : SPRI%%')))
        dfATTSalesActualsRPT4 = dfATTSalesActualsRPT3.filter(~(dfATTSalesActualsRPT.Location.like('Generated%%')))
        dfAttSalesActualsInputRPT = dfATTSalesActualsRPT4.filter(~(dfATTSalesActualsRPT.Location.like('Locat%%')))

        FinalRPTDF1 = dfAttSalesActualsInputRPT.withColumnRenamed("Location", "attlocationname2").\
            withColumnRenamed("Loc Id", "locid2").\
            withColumnRenamed("Dlr1 Code", "dealercode2").\
            withColumnRenamed("KPI", "kpiname2").\
            withColumnRenamed("KPI ID", "kpiid2").\
            withColumnRenamed("Dec 2017 Actual", "actual_value2").\
            withColumnRenamed("Dec 2017 Projected", "projected_value2").\
            withColumnRenamed("Dec 2017 Target", "target_value2")

        FinalRPTDF1 = FinalRPTDF1.withColumn("actualvalue2", when(FinalRPTDF1.actual_value2.contains('%'), rtrim(regexp_replace("actual_value2", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("actual_value2", '\\$|\\%|\\,', ''))))

        FinalRPTDF1 = FinalRPTDF1.withColumn("projectedvalue2", when(FinalRPTDF1.actual_value2.contains('%'), rtrim(regexp_replace("projected_value2", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("projected_value2", '\\$|\\%|\\,', ''))))

        FinalRPTDF1 = FinalRPTDF1.withColumn("targetvalue2", when(FinalRPTDF1.actual_value2.contains('%'), rtrim(regexp_replace("target_value2", '\\%|\\,', '')).cast(DecimalType()) / 100).otherwise(rtrim(regexp_replace("target_value2", '\\$|\\%|\\,', ''))))

        FinalHistDF1 = FinalHistDF1.withColumn('reportdate', lit(filedateS))
        FinalHistDF1.registerTempTable("HIST")
        FinalRPTDF1 = FinalRPTDF1.withColumn('reportdate2', lit(filedateS))
        FinalRPTDF1.registerTempTable("RPT")

        FinalHistDF = self.sparkSession.sql("select attlocationname, locid, dealercode, kpiname, kpiid, actualvalue, projectedvalue, targetvalue, reportdate, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from HIST")

        FinalRPTDF = self.sparkSession.sql("select attlocationname2,locid2,dealercode2,kpiname2,kpiid2,actualvalue2 ,projectedvalue2,targetvalue2,reportdate2,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from RPT")
        '''
        FinalHistDF = spark.sql("select attlocationname,locid,dealercode,kpiname,kpiid,actualvalue ,projectedvalue,"
                                "targetvalue,reportdate,"
                                "SUBSTR(reportdate,7,4) as year,SUBSTR(reportdate,1,2) as month  from HIST")
        FinalRPTDF = spark.sql("select attlocationname2,locid2,dealercode2,kpiname2,kpiid2,"
                               "actualvalue2 ,projectedvalue2,targetvalue2,reportdate2,"
                               "SUBSTR(reportdate2,7,4) as year,SUBSTR(reportdate2,1,2)  as month from RPT")
        '''
        FinalHistDF.show(100, False)
        FinalRPTDF.show(100, False)
        FinalHistDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.AttSalesActualsOutput + '/' + 'Working1')

        FinalRPTDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.AttSalesActualsOutput + '/' + 'Working2')

        FinalHistDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.AttSalesActualsOutput)
        FinalRPTDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.AttSalesActualsOutput)

        spark.stop()


if __name__ == "__main__":
    ATTSalesActualsCSVToParquet().loadParquet()
