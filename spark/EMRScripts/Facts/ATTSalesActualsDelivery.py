from pyspark.sql import SparkSession
import sys


class AttSalesActualsDelivery(object):

        def __init__(self):

                self.appName = self.__class__.__name__
                self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
                self.attSalesActualOutputArg = sys.argv[1]
                self.attSalesActualRefineInp = sys.argv[2]

                #########################################################################################################
                #                                 Read the 2 source files                                              #
                #########################################################################################################

        def loadCsv(self):

                dfAttSalesActual = self.sparkSession.read.parquet(self.attSalesActualRefineInp)

                dfAttSalesActual.registerTempTable("attsalesactual")

                #########################################################################################################
                #                                 Spark Transformation begins here                                      #
                #########################################################################################################

                dfOutput = self.sparkSession.sql("select distinct a.reportdate as RPT_DT,a.storenumber as STORE_NUM,"
                                                 "a.dealercode as DLR_CD,a.companycode as CO_CD,a.kpiname as KPI_NM,"
                                                 "a.actualvalue as ACTL_VAL,a.projectedvalue as PROJ_VAL,"
                                                 "a.currentkpiindicator as CRNT_KPI_IND from attsalesactual a")

                dfOutput.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.attSalesActualOutputArg + '/' + 'Current')

                self.sparkSession.stop()


if __name__ == "__main__":
        AttSalesActualsDelivery().loadCsv()
