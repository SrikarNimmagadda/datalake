from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import year, unix_timestamp, from_unixtime, substring


class StoreDailyGoalsForecastCSVToParquet(object):
    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.storeDailyGoalForecastPath = sys.argv[1]
        self.discoveryBucketWorking = sys.argv[2]

        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.tableName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[2]
        self.storeDailyGoalForecastPartitionPath = 's3://' + self.discoveryBucket + '/' + self.tableName
        self.storeDailyGoalForecastWorkingPath = self.discoveryBucketWorking

    def loadDiscovery(self):
        dfStoreDailyGoalForecast = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            option("spark.read.simpleMode", "true"). \
            option("useHeader", "true"). \
            load(self.storeDailyGoalForecastPath)

        dfStoreDailyGoalForecast.withColumnRenamed("Date", "date"). \
            withColumnRenamed("Day % to Forecast", "daypercentforecast"). \
            withColumnRenamed("Daily Forecast", "dailyforecast").registerTempTable("StoreDailyGoalForecast")

        dfStoreDailyGoalForecastFinal = self.sparkSession.sql("select date, daypercentforecast, dailyforecast from "
                                                              "StoreDailyGoalForecast")

        dfStoreDailyGoalForecastFinal.coalesce(1).write.mode('overwrite').format('parquet').\
            save(self.storeDailyGoalForecastWorkingPath)

        dfStoreDailyGoalForecastFinal.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(
            self.storeDailyGoalForecastPartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreDailyGoalsForecastCSVToParquet().loadDiscovery()
