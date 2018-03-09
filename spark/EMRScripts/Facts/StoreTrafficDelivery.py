from pyspark.sql import SparkSession
import sys


class StoreTrafficDelivery(object):

        def __init__(self):

                self.appName = self.__class__.__name__
                self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
                self.storeTrafficInput = sys.argv[1]
                self.storeTrafficOutput = sys.argv[2]

                #########################################################################################################
                #                                 Reading the source data files                                         #
                #########################################################################################################

        def loadCsv(self):

                storeTrafficTable_Df = self.sparkSession.read.parquet(self.storeTrafficInput)
                storeTrafficTable_Df.registerTempTable("StoreTrafficTable")

                final_Df = self.sparkSession.sql("select a.reportdate as RPT_DT,a.storenumber as STORE_NUM , a.trafficdate TRAFFIC_DT, a.traffictime TRAFFIC_TM, a.companycd CO_CD,"
                                                 "a.sourcesystemlocationid  as SRC_SYS_LOC_ID, a.sourcesystemname SRC_SYS_NM , a.traffictype TRAFFIC_TYP, a.trafficcount TRAFFIC_CNT "
                                                 "from StoreTrafficTable a")

                final_Df.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("overwrite").save(self.storeTrafficOutput + '/' + 'Current')

                self.sparkSession.stop()


if __name__ == "__main__":
        StoreTrafficDelivery().loadCsv()
