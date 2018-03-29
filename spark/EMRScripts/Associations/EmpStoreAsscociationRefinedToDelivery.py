from pyspark.sql import SparkSession
import sys


class EmpStoreAssociationDelivery(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j

        self.empStoreAssociationRefineInp = sys.argv[1]
        self.empStoreAssociationdeliveryOP = sys.argv[2]

    def loadDelivery(self):
        dfEmpStoreAssociation = self.spark.read.parquet(self.empStoreAssociationRefineInp)
        dfEmpStoreAssociation.registerTempTable("employee")

        #  Spark Transformation begins here
        dfEmpStoreAssociation = self.spark.sql("select a.storenumber as STORE_NUM," +
                                               "a.sourceemployeeid as SRC_EMP_ID," +
                                               "a.companycd as CO_CD," +
                                               "a.sourcesystenname as SRC_SYS_NM," +
                                               " a.primarylocationindicator as PRI_LOC_IN," +
                                               " a.CDC_IND_CD as CDC_IND_CD " +
                                               "from employee a")

        dfEmpStoreAssociation.coalesce(1).\
            write.format("com.databricks.spark.csv").option("header", "true").\
            mode("overwrite").save(self.empStoreAssociationdeliveryOP + '/' + 'Current')

        dfEmpStoreAssociation.coalesce(1).\
            write.format("com.databricks.spark.csv").option("header", "true").\
            mode("append").save(self.empStoreAssociationdeliveryOP + '/' + 'Previous')

        self.spark.stop()


if __name__ == "__main__":
    EmpStoreAssociationDelivery().loadDelivery()
