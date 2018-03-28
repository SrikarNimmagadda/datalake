from pyspark.sql import SparkSession
import sys

TBGoalPointDiscoveryStore = sys.argv[1]
TBGoalPointDiscoveryEmployee = sys.argv[2]
GoalPointOp = sys.argv[3]

spark = SparkSession.builder.appName("TBGoalPointsRefine").getOrCreate()

dfSpringMobileDiscoveryStore = spark.read.parquet(TBGoalPointDiscoveryStore).registerTempTable("goalpointStore")
dfSpringMobileDiscoveryEmployee = spark.read.parquet(TBGoalPointDiscoveryEmployee).registerTempTable("goalpointEmployee")
dfSpringMobileDiscoveryStore = spark.sql("select a.kpiname as kpiname, a.goalpoints as goalpoints, a.bonuspoints as bonuspoints, a.decelerator as deceleratorpoints, a.reportdate,'Store' as classification, '4' as companycd, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from goalpointStore a")

dfSpringMobileDiscoveryEmployee = spark.sql("select a.kpiname as kpiname, a.goalpoints as goalpoints, a.bonuspoints as bonuspoints, a.decelerator as deceleratorpoints, a.reportdate, 'Employee' as classification, '4' as companycd , YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from goalpointEmployee a")

df1SpringMobileDiscoveryStore = dfSpringMobileDiscoveryStore.dropDuplicates(['reportdate', 'classification', 'kpiname', 'companycd'])
df1SpringMobileDiscoveryEmployee = dfSpringMobileDiscoveryEmployee.dropDuplicates(['reportdate', 'classification', 'kpiname', 'companycd'])

FinaldfSpringMobileDiscovery = df1SpringMobileDiscoveryEmployee.union(df1SpringMobileDiscoveryStore)

FinaldfSpringMobileDiscovery.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(GoalPointOp)
FinaldfSpringMobileDiscovery.coalesce(1).select("*").write.mode("overwrite").parquet(GoalPointOp + '/' + 'Working')

spark.stop()
