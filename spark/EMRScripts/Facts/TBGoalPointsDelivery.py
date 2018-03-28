from pyspark.sql import SparkSession

import sys

TBGoalPointDiscovery = sys.argv[1]
GoalPointOp = sys.argv[2]

spark = SparkSession.builder.appName("TBGoalPointDelivery").getOrCreate()


#                                 Read the 2 source files                                              #

dfGoalDelivery = spark.read.parquet(TBGoalPointDiscovery)

dfGoalDelivery.registerTempTable("goalpoint")

#                                 Spark Transformation begins here                                      #
dfGoalDelivery = spark.sql("select a.reportdate AS RPT_DT, a.classification AS CLS, a.kpiname as KPI_NM, a.companycd as CO_CD, a.goalpoints as GOAL_PT, a.bonuspoints as BNS_PT, a.deceleratorpoints as DECELERATOR_PT from goalpoint a")

dfGoalDelivery.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("overwrite").save(GoalPointOp + '/' + 'Current')
dfGoalDelivery.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("append").save(GoalPointOp + '/' + 'Previous')
spark.stop()
