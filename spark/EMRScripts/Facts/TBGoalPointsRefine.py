from pyspark.sql import SparkSession
import sys


class TBGoalPointsRefine(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.appName).getOrCreate()

        self.TBGoalPointDiscoveryStore = sys.argv[1]
        self.TBGoalPointDiscoveryEmployee = sys.argv[2]
        self.GoalPointOp = sys.argv[3]

    def loadRefined(self):

        dfSpringMobileDiscoveryStore = self.spark.read.parquet(self.TBGoalPointDiscoveryStore).\
            registerTempTable("goalpointStore")
        dfSpringMobileDiscoveryEmployee = self.spark.read.parquet(self.TBGoalPointDiscoveryEmployee).\
            registerTempTable("goalpointEmployee")
        dfSpringMobileDiscoveryStore = self.spark.sql("select a.kpiname as kpiname, a.goalpoints as goalpoints, a.bonuspoints as bonuspoints, a.decelerator as deceleratorpoints, a.reportdate,'Store' as classification, '4' as companycd, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from goalpointStore a")

        dfSpringMobileDiscoveryEmployee = self.spark.sql("select a.kpiname as kpiname, a.goalpoints as goalpoints, a.bonuspoints as bonuspoints, a.decelerator as deceleratorpoints, a.reportdate, 'Employee' as classification, '4' as companycd , YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from goalpointEmployee a")

        df1SpringMobileDiscoveryStore = dfSpringMobileDiscoveryStore.\
            dropDuplicates(['reportdate', 'classification', 'kpiname', 'companycd'])
        df1SpringMobileDiscoveryEmployee = dfSpringMobileDiscoveryEmployee.\
            dropDuplicates(['reportdate', 'classification', 'kpiname', 'companycd'])

        FinaldfSpringMobileDiscovery = df1SpringMobileDiscoveryEmployee.union(df1SpringMobileDiscoveryStore)

        FinaldfSpringMobileDiscovery.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').\
            parquet(self.GoalPointOp)
        FinaldfSpringMobileDiscovery.coalesce(1).select("*").write.mode("overwrite").\
            parquet(self.GoalPointOp + '/' + 'Working')

        self.spark.stop()


if __name__ == "__main__":
    TBGoalPointsRefine().loadRefined()
