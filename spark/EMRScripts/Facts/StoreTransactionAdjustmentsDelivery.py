from pyspark.sql import SparkSession
import sys


class StoreTransactionAdjustments(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.finalDf_Rpt = sys.argv[1]
        self.dimStoreTransAdjRefinedInput = sys.argv[2]

    def loadParquet(self):

        self.sparkSession.read.parquet(self.dimStoreTransAdjRefinedInput).registerTempTable("DimStoreTransAdjRefinedTable")

        Final_Joined_DF = self.sparkSession.sql("select reportdate as RPT_DT, a.storenumber as STORE_NUM, a.adjustmentcategory as ADJMNT_CAT, a.adjustmenttype as ADJMNT_TYP, a.companycd as CO_CD, a.adjustmentamount as ADJMNT_AMT from DimStoreTransAdjRefinedTable a")

        Final_Joined_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.finalDf_Rpt + '/' + 'Current')

        Final_Joined_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(self.finalDf_Rpt + '/' + 'Previous')

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreTransactionAdjustments().loadParquet()
