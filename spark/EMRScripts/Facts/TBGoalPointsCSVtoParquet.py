from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.functions import lit

TBGoalPointsInpStore = sys.argv[1]
TBGoalPointsInpEmployee = sys.argv[2]
TBGoalPointsOpStore = sys.argv[3]
TBGoalPointsOpEmployee = sys.argv[4]

spark = SparkSession.builder.appName("ParquetTBGoalPoints").getOrCreate()

schema = StructType([StructField('Category', StringType(), True), StructField('Points', IntegerType(), False), StructField('Bonus', IntegerType(), False), StructField('Decelerator', IntegerType(), False)])

dfTBGoalPoints = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

dfTBGoalPoints = spark.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").load(TBGoalPointsInpStore)

dfTBGoalPoints = dfTBGoalPoints.withColumnRenamed("Category", "kpiname").withColumnRenamed("Points", "goalpoints").withColumnRenamed("Bonus", "bonuspoints").withColumnRenamed("Decelerator", "decelerator")

today = datetime.now().strftime('%m/%d/%Y')
dfTBGoalPoints = dfTBGoalPoints.withColumn('reportdate', lit(today))

dfTBGoalPoints.registerTempTable("TBGP")
dfTBGoalPointsFinalStore = spark.sql("select a.kpiname,a.goalpoints, a.bonuspoints,a.decelerator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month,a.reportdate from TBGP a ")

dfTBGoalPointsFinalStore.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(TBGoalPointsOpStore)

dfTBGoalPointsFinalStore.coalesce(1).select("*").write.mode("overwrite").parquet(TBGoalPointsOpStore + '/' + 'Working')

dfTBGoalPoints = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

dfTBGoalPoints = spark.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").load(TBGoalPointsInpEmployee)

dfTBGoalPoints = dfTBGoalPoints.withColumnRenamed("Category", "kpiname").withColumnRenamed("Points", "goalpoints").withColumnRenamed("Bonus", "bonuspoints").withColumnRenamed("Decelerator", "decelerator")

today = datetime.now().strftime('%m/%d/%Y')
dfTBGoalPoints = dfTBGoalPoints.withColumn('reportdate', lit(today))

dfTBGoalPoints.registerTempTable("TBGP")
dfTBGoalPointsFinalEmployee = spark.sql("select a.kpiname,a.goalpoints, a.bonuspoints,a.decelerator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month,a.reportdate from TBGP a ")

dfTBGoalPointsFinalEmployee.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(TBGoalPointsOpEmployee)

dfTBGoalPointsFinalEmployee.coalesce(1).select("*").write.mode("overwrite").parquet(TBGoalPointsOpEmployee + '/' + 'Working')

spark.stop()
