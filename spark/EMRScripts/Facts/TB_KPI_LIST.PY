from __future__ import print_function
from pyspark.sql import SparkSession
import sys


class SalesKPI(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.kpiList = sys.argv[1]
        self.kpiOutput = sys.argv[2]

    def loadParquet(self):
        dfkpiList = self.sparkSession.read.format("com.crealytics.spark.excel").\
            option("location", self.kpiList).\
            option("sheetName", "KPIs for DL-Calculation").\
            option("treatEmptyValuesAsNulls", "true").\
            option("addColorColumns", "false").\
            option("inferSchema", "true").\
            option("spark.read.simpleMode", "true"). \
            option("useHeader", "true").\
            load("com.databricks.spark.csv")

        dfkpiList.registerTempTable("KPI")

        finalDf = self.sparkSession.sql("select KPIName as KPI_NM,Companycd as CO_CD, Classification as KPI_CLSIFCATION, Description as KPI_DESC, Expression as KPI_XPRSN, FilterCondition as KPI_FLTR_COND, FromClause as KPI_FROM_CLAS, GroupByColumns as KPI_GRPBYCOLLIINVC, CalculationLevel as KPI_CALC_LVL, '1' as KPI_ACTV_IND, CalculationIndicator as KPI_CALC_IND, PublishIndicator as KPI_PBLSH_IND, PrimaryIndicator as PRI_KPI_IND, '' as KPI_INTRADAY_IND, 'I' as CDC_IND_CD from KPI")

        finalDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.kpiOutput + '/' + 'Current')
        self.sparkSession.stop()


if __name__ == "__main__":
    SalesKPI().loadParquet()
