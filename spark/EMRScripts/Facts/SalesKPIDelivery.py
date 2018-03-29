from __future__ import print_function
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import regexp_replace


class SalesKPIDelivery(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.salesKpiOutput = sys.argv[1]
        self.salesKpiRefineInp = sys.argv[2]

    def loadParquet(self):

        dfSalesKpi = self.sparkSession.read.parquet(self.salesKpiRefineInp)
        dfSalesKpi = dfSalesKpi.withColumn("RPT_DT", regexp_replace("report_date", '\\-', '/'))
        dfSalesKpi.drop('report_date')
        dfSalesKpi.registerTempTable("saleskpi")

        dfSalesKpi = self.sparkSession.sql("select RPT_DT,storenumber as STORE_NUM,sourceemployeeid as SRC_EMP_ID,kpiname as KPI_NM,companycd as CO_CD, 'RQ4' as SRC_SYS_NM,kpivalue as KPI_VAL_ACTL from saleskpi")

        dfSalesKpi.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.salesKpiOutput + '/' + 'Current')
        dfSalesKpi.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(self.salesKpiOutput + '/' + 'Previous')
        self.sparkSession.stop()


if __name__ == "__main__":
    SalesKPIDelivery().loadParquet()
