import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import boto3


class StoreRecHeadcountCSVToParquet(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.s3 = boto3.resource('s3')

        self.inputWorkingPath = sys.argv[1]
        self.outputWorkingPath = sys.argv[2]
        self.discoveryBucket = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[0]
        self.storeRecHeadcountName = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[1]
        self.workingName = self.inputWorkingPath[self.inputWorkingPath.index('tb'):].split("/")[2]
        self.outputPartitionPath = "s3://" + self.discoveryBucket + '/' + self.storeRecHeadcountName
        self.outputCSVPath = "s3://" + self.discoveryBucket + '/' + self.storeRecHeadcountName + "/csv"
        self.errorBucketPath = sys.argv[3] + '/Discovery'

    def loadParquet(self):

        SRHCSchema = StructType([StructField("Store Number", StringType()),
                                 StructField("Store Name", StringType()),
                                 StructField("Headcount", StringType()),
                                 StructField("Store Manager", StringType()),
                                 StructField("BAM 1 & 2", StringType()),
                                 StructField("FTEC", StringType()),
                                 StructField("PTEC", StringType()),
                                 StructField("Other", StringType()),
                                 StructField("DLSC", StringType()),
                                 StructField("MIT", StringType()),
                                 StructField("Seasonal", StringType()),
                                 StructField("Approved HC", StringType()),
                                 StructField("Date", StringType())
                                 ])

        storeRecHCFileCheck = 1

        if storeRecHCFileCheck == 1:
            self.log.info("Raw file is in csv format, proceeding with the logic")
            dfStoreRecHC = self.sparkSession.read.format("com.databricks.spark.csv"). \
                option("header", "true"). \
                option("treatEmptyValuesAsNulls", "true"). \
                schema(SRHCSchema). \
                load(self.inputWorkingPath)

            dfStoreRecHCCnt = dfStoreRecHC.count()

            if dfStoreRecHCCnt > 1:
                self.log.info("The store recruiting headcount file has data")
                fileHasDataFlag = 1
            else:
                self.log.info("The store recruiting headcount file does not have data")
                fileHasDataFlag = 0

            if fileHasDataFlag == 1:
                self.log.info("Csv file loaded into dataframe properly")

                dfStoreRecHC.withColumnRenamed("Store Number", "store_number").\
                    withColumnRenamed("Store Name", "store_name"). \
                    withColumnRenamed("Headcount", "actual_headcount"). \
                    withColumnRenamed("Store Manager", "store_manager"). \
                    withColumnRenamed("BAM 1 & 2", "business_assistant_manager_count"). \
                    withColumnRenamed("FTEC", "fulltime_equivalent_count"). \
                    withColumnRenamed("PTEC", "parttime_equivalent_count"). \
                    withColumnRenamed("Other", "fulltime_floater_count"). \
                    withColumnRenamed("DLSC", "district_lead_sales_consultant_count"). \
                    withColumnRenamed("MIT", "mit_count"). \
                    withColumnRenamed("Seasonal", "seasonal_count"). \
                    withColumnRenamed("Approved HC", "approved_headcount"). \
                    withColumnRenamed("Date", "date"). \
                    registerTempTable("StoreRecHCTempTable")

                dfStoreRecHCFinal = self.sparkSession.sql(
                    "select store_number,store_name,actual_headcount,store_manager,business_assistant_manager_count,"
                    "fulltime_equivalent_count,parttime_equivalent_count,fulltime_floater_count,"
                    "district_lead_sales_consultant_count,mit_count,seasonal_count,approved_headcount,date,"
                    "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month"
                    " from StoreRecHCTempTable "
                )

                dfStoreRecHCFinal.coalesce(1).select('store_number', 'store_name', 'actual_headcount', 'store_manager',
                                                     'business_assistant_manager_count', 'fulltime_equivalent_count',
                                                     'parttime_equivalent_count', 'fulltime_floater_count',
                                                     'district_lead_sales_consultant_count', 'mit_count', 'seasonal_count',
                                                     'approved_headcount', 'date').write.mode("overwrite").\
                    parquet(self.outputWorkingPath)

                dfStoreRecHCFinal.coalesce(1). \
                    write.mode('append').partitionBy('year', 'month'). \
                    format('parquet').save(self.outputPartitionPath)

                # dfStoreRecHCFinal.coalesce(1).write.mode("overwrite").csv(self.outputCSVPath, header=True)

            else:
                self.log.error("ERROR : Loading csv file into dataframe")

        else:
            self.log.error("ERROR : Raw file is not in csv format")

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreRecHeadcountCSVToParquet().loadParquet()
