from pyspark.sql import SparkSession
import sys


class ATTSalesActualsDelivery(object):

    def __init__(self):
        self.ATTSalesActualOutputArg = sys.argv[1]
        self.ATTSalesActualRefineInp = sys.argv[2]

    def loadDelivery(self):
        spark = SparkSession.builder.appName("ATTSalesActualDelivery").getOrCreate()

        #########################################################################################################
        #                                 Read the 2 source files                                              #
        #########################################################################################################

        dfATTSalesActual = spark.read.parquet(self.ATTSalesActualRefineInp)

        dfATTSalesActual.registerTempTable("attsalesactual")

        #########################################################################################################
        #                                 Spark Transformation begins here                                      #
        #########################################################################################################

        dfOutput = spark.sql("select distinct a.reportdate as RPT_DT,a.storenumber as STORE_NUM,"
                             "a.dealercode as DLR_CD,a.companycode as CO_CD,a.kpiname as KPI_NM,"
                             "a.actualvalue as ACTL_VAL,a.projectedvalue as PROJ_VAL,"
                             "a.currentkpiindicator as CRNT_KPI_IND from attsalesactual a")

        dfOutput.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.ATTSalesActualOutputArg + '/' + 'Current')

        dfOutput.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(self.ATTSalesActualOutputArg + '/' + 'Previous')

        spark.stop()


if __name__ == "__main__":
    ATTSalesActualsDelivery().loadDelivery()
