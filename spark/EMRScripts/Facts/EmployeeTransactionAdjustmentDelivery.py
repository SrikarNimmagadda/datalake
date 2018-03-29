from pyspark.sql import SparkSession
import sys


class EmpTransAdj(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.EmpTransAdjOut = sys.argv[1]
        self.EmpTransAdjIn = sys.argv[2]

    def loadParquet(self):
        dfEmpTransAdj = self.sparkSession.read.parquet(self.EmpTransAdjIn)
        dfEmpTransAdj.registerTempTable("EmpTransAdj")
        dfEmpTransAdj = self.sparkSession.sql("select reportdate as RPT_DT, sourceemployeeid as SRC_EMP_ID, adjustmenttype as ADJMNT_TYP, companycd as CO_CD, sourcesystemname as SRC_SYS_NM, adjustmentamount as ADJMNT_AMT from EmpTransAdj")
        dfEmpTransAdj.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(self.EmpTransAdjOut + '/' + 'Current')
        dfEmpTransAdj.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(self.EmpTransAdjOut + '/' + 'Previous')

        self.sparkSession.stop()


if __name__ == "__main__":
    EmpTransAdj().loadParquet()
