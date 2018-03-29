from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col


class EmpTransAdj(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.EmpTransAdjOut = sys.argv[1]
        self.EmpTransAdjIn = sys.argv[2]
        self.EmployeeIn = sys.argv[3]

    def loadParquet(self):

        dfEmpTransAdj3 = self.sparkSession.read.parquet(self.EmpTransAdjIn)
        dfEmployee = self.sparkSession.read.parquet(self.EmployeeIn)

        dfEmpTransAdj2 = dfEmpTransAdj3.drop('year')
        dfEmpTransAdj = dfEmpTransAdj2.drop('month')

        def rowExpander(row):
            rowDict = row.asDict()
            MarketVal = rowDict.pop('market')
            RegionVal = rowDict.pop('region')
            DistrictVal = rowDict.pop('district')
            LocationVal = rowDict.pop('location')
            reportdateVal = rowDict.pop('reportdate')
            S1Val = rowDict.pop('salesperson')
            S2Val = rowDict.pop('salespersonid')
            LocVal = rowDict.pop('loc')
            for k in rowDict:
                a = k.split("_")
                yield Row(**{'market': MarketVal, 'reportdate': reportdateVal, 'region': RegionVal, 'district': DistrictVal, 'location': LocationVal, 'loc': LocVal, 'salesperson': S1Val, 'salespersonid': S2Val, 'adjustmenttype': a[0], 'adjustmentamount': row[k]})

        FinalDF1 = self.sparkSession.createDataFrame(dfEmpTransAdj.rdd.flatMap(rowExpander))
        FinalDF1.registerTempTable("EmpTransAdj1")

        FinalDF_EmpTransAdj1 = self.sparkSession.sql("select market as springmarket, region as springregion, district as springdistrict, salesperson as employeename, salespersonid, reportdate, adjustmenttype, adjustmentamount from EmpTransAdj1")

        dfEmployee.registerTempTable("Employee")

        FinalDF_EmpTransAdj1.registerTempTable("EmpTransAdj")

        FinalDF = self.sparkSession.sql("select a.springmarket, a.springregion, a.springdistrict, a.employeename, a.reportdate, a.adjustmenttype, a.adjustmentamount as adjustmentamount1, b.sourceemployeeid, 'TBSFTP' as sourcesystemname, '4' as companycd, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from EmpTransAdj a inner join Employee b on a.salespersonid=b.workdayid")

        finalDfChangedType = FinalDF.withColumn("adjustmentamount", FinalDF["adjustmentamount1"].cast(DecimalType()))

        finalDfChangedType.drop('adjustmentamount1')
        finalDfChangedType = finalDfChangedType.where(col("sourceemployeeid").isNotNull())
        finalDfChangedType.coalesce(1).select("*").write.mode("overwrite").parquet(self.EmpTransAdjOut + '/' + 'Working')
        finalDfChangedType.coalesce(1).select("*").write.mode("append").parquet(self.EmpTransAdjOut + '/' + 'Previous')
        finalDfChangedType.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.EmpTransAdjOut)
        self.sparkSession.stop()


if __name__ == "__main__":
    EmpTransAdj().loadParquet()
