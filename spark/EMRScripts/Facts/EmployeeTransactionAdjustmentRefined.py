from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys

spark = SparkSession.builder.appName("StoreTransactionAdjustments").getOrCreate()

EmpTransAdjOut = sys.argv[1]
EmpTransAdjIn = sys.argv[2]
EmployeeIn = sys.argv[3]

dfEmpTransAdj3 = spark.read.parquet(EmpTransAdjIn)
dfEmployee = spark.read.parquet(EmployeeIn)

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


FinalDF1 = spark.createDataFrame(dfEmpTransAdj.rdd.flatMap(rowExpander))
FinalDF1.registerTempTable("EmpTransAdj1")

FinalDF_EmpTransAdj1 = spark.sql("select market as springmarket,region as springregion,district as springdistrict,salesperson as employeename,reportdate,adjustmenttype,adjustmentamount from EmpTransAdj1")

dfEmployee.registerTempTable("Employee")

FinalDF_EmpTransAdj1.registerTempTable("EmpTransAdj")
FinalDF = spark.sql(" select a.springmarket,a.springregion,a.springdistrict,b.name as employeename,"
                    "a.reportdate,a.adjustmenttype,a.adjustmentamount,b.sourceemployeeid,"
                    "'TBSFTP' as sourcesystemname,'4' as companycd,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                    "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from EmpTransAdj a inner join Employee b on a.employeename=b.name")

FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(EmpTransAdjOut + '/' + 'Working')
FinalDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(EmpTransAdjOut)
spark.stop()
