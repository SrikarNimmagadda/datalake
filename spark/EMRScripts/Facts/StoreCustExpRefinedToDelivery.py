from pyspark.sql import SparkSession
import sys


class StoreCustExpRefinedToDelivery(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)

        self.OutputPath = sys.argv[1]
        self.StoreCustExpInputPath = sys.argv[2]

    def loadDelivery(self):
        self.spark.read.parquet(self.StoreCustExpInputPath).registerTempTable("StoreCustExp")

        dfStoreCustExpFinal = self.spark.sql("select report_date as RPT_DT,store_number as STORE_NUM,companycd as CO_CD,"
                                             "five_key_behaviours as FIVE_KEY_BHVS,effective_solutioning as EFC_SLTNING,"
                                             "integrated_experience as NTGRTD_EXPRC,cast(dealer_code as string) as DLR_CD"
                                             " from StoreCustExp a")

        dfStoreCustExpFinal.coalesce(1). \
            write.format("com.databricks.spark.csv"). \
            option("header", "true").mode("overwrite").save(self.OutputPath + '/' + 'Current')

        dfStoreCustExpFinal.coalesce(1). \
            write.format("com.databricks.spark.csv"). \
            option("header", "true").mode("append").save(self.OutputPath + '/' + 'Previous')

        self.spark.stop()


if __name__ == "__main__":
    StoreCustExpRefinedToDelivery().loadDelivery()
