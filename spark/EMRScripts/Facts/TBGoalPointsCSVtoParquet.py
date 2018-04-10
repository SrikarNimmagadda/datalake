from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.functions import lit


class TBGoalPointsCSVToParquet(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()

        self.TBGoalPointsInpStore = sys.argv[1]
        self.TBGoalPointsInpEmployee = sys.argv[2]
        self.TBGoalPointsOpStore = sys.argv[3]
        self.TBGoalPointsOpEmployee = sys.argv[4]

    def loadParquet(self):
        schema = StructType([StructField('Category', StringType(), True), StructField('Points', IntegerType(), False),
                             StructField('Bonus', IntegerType(), False),
                             StructField('Decelerator', IntegerType(), False)])

        dfTBGoalPoints = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        dfTBGoalPoints = self.spark.read.format("com.databricks.spark.csv").option("header", "true").\
            option("treatEmptyValuesAsNulls", "true").load(self.TBGoalPointsInpStore)

        dfTBGoalPoints = dfTBGoalPoints.\
            withColumnRenamed("Category", "kpiname").\
            withColumnRenamed("Points", "goalpoints").\
            withColumnRenamed("Bonus", "bonuspoints").\
            withColumnRenamed("Decelerator", "decelerator")

        today = datetime.now().strftime('%m/%d/%Y')
        dfTBGoalPoints = dfTBGoalPoints.withColumn('reportdate', lit(today))

        dfTBGoalPoints.registerTempTable("TBGP")
        dfTBGoalPointsFinalStore = self.spark.sql("select a.kpiname,a.goalpoints, a.bonuspoints,a.decelerator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month,a.reportdate from TBGP a ")

        dfTBGoalPointsFinalStore.coalesce(1).select("*").write.mode("append").\
            partitionBy('year', 'month').parquet(self.TBGoalPointsOpStore)

        dfTBGoalPointsFinalStore.coalesce(1).select("*").write.mode("overwrite").\
            parquet(self.TBGoalPointsOpStore + '/' + 'Working')

        dfTBGoalPoints = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
        dfTBGoalPoints = self.spark.read.format("com.databricks.spark.csv").option("header", "true").option(
            "treatEmptyValuesAsNulls", "true").load(self.TBGoalPointsInpEmployee)

        dfTBGoalPoints = dfTBGoalPoints.withColumnRenamed("Category", "kpiname").\
            withColumnRenamed("Points", "goalpoints").\
            withColumnRenamed("Bonus", "bonuspoints").\
            withColumnRenamed("Decelerator", "decelerator")

        today = datetime.now().strftime('%m/%d/%Y')
        dfTBGoalPoints = dfTBGoalPoints.withColumn('reportdate', lit(today))

        dfTBGoalPoints.registerTempTable("TBGP")
        dfTBGoalPointsFinalEmployee = self.spark.sql("select a.kpiname,a.goalpoints, a.bonuspoints,a.decelerator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month,a.reportdate from TBGP a ")

        dfTBGoalPointsFinalEmployee.coalesce(1).select("*").write.mode("append").\
            partitionBy('year', 'month').parquet(self.TBGoalPointsOpEmployee)

        dfTBGoalPointsFinalEmployee.coalesce(1).select("*").write.mode("overwrite").\
            parquet(self.TBGoalPointsOpEmployee + '/' + 'Working')

        self.spark.stop()


if __name__ == "__main__":
    TBGoalPointsCSVToParquet().loadParquet()
