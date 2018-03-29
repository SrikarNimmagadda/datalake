from pyspark.sql import SparkSession
import sys


class TBGoalPointsDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.spark.sparkContext._jvm.org.apache.log4j

        self.TBGoalPointDiscovery = sys.argv[1]
        self.GoalPointOp = sys.argv[2]

    def loadDelivery(self):

        dfGoalDelivery = self.spark.read.parquet(self.TBGoalPointDiscovery)

        dfGoalDelivery.registerTempTable("goalpoint")

        #                                 Spark Transformation begins here                                      #
        dfGoalDelivery = self.spark.sql("select a.reportdate AS RPT_DT, a.classification AS CLS, a.kpiname as KPI_NM, a.companycd as CO_CD, a.goalpoints as GOAL_PT, a.bonuspoints as BNS_PT, a.deceleratorpoints as DECELERATOR_PT from goalpoint a")

        dfGoalDelivery.coalesce(1).select("*").write.format("com.databricks.spark.csv").\
            option("quoteMode", "All").option("header", "true").mode("overwrite").\
            save(self.GoalPointOp + '/' + 'Current')
        dfGoalDelivery.coalesce(1).select("*").write.format("com.databricks.spark.csv").\
            option("quoteMode", "All").option("header", "true").mode("append").\
            save(self.GoalPointOp + '/' + 'Previous')
        self.spark.stop()


if __name__ == "__main__":
    TBGoalPointsDelivery().loadDelivery()
