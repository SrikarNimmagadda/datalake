from pyspark.sql import SparkSession
import sys

EmpGoalRefineInp = sys.argv[1]  # attscoredgoal
EmpGoalDeliveryOP = sys.argv[2]


spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()

dfEmpGoal = spark.read.parquet(EmpGoalRefineInp)

dfEmpGoal.registerTempTable("empgoal")

#  Spark Transformation begins here

dfEmpGoal = spark.sql("select a.report_date as RPT_DT, " +
                      "a.sourceemployeeid as SRC_EMP_ID, " +
                      "a.kpiname as KPI_NM, a.companycd as CO_CD, " +
                      " a.sourcesystemname as SRC_SYS_NM, " +
                      "a.goalvalue as GOAL_VAL from empgoal a")


dfEmpGoal.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("overwrite").\
    save(EmpGoalDeliveryOP + '/' + 'Current')

dfEmpGoal.coalesce(1). \
    write.format("com.databricks.spark.csv").\
    option("header", "true").mode("append").\
    save(EmpGoalDeliveryOP + '/' + 'Previous')


spark.stop()
