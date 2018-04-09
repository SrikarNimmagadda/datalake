from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField
import pyspark.sql.functions as sf
from pyspark.sql.functions import col

StoreGoalRefineInp = sys.argv[1]
EmployeeAssInp = sys.argv[2]
EmployeerefineInp = sys.argv[3]
StoreRecruitHeadCountRefineInp = sys.argv[4]
EmpGoalOP = sys.argv[5]


spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()


dfStoreGoal = spark.read.parquet(StoreGoalRefineInp)
dfEmpAss = spark.read.parquet(EmployeeAssInp)
dfEmpRefine = spark.read.parquet(EmployeerefineInp)
dfStoreRecruitHeadCountRefine = spark.read.\
    parquet(StoreRecruitHeadCountRefineInp)

empSchema = StructType([StructField("pte", StringType(), True),
                        StructField("gpgoal", StringType(), True)])
dfEmpGPGoal = spark.createDataFrame([[0, 10000], [1, 7000]],
                                    schema=empSchema)

dfEmpGPGoal.show()

dfStoreGoal.registerTempTable("storegoal")
dfEmpAss.registerTempTable("empass")
dfEmpRefine.registerTempTable("emprefine")
dfStoreRecruitHeadCountRefine.registerTempTable("storerecruitheadcountrefine")
dfEmpGPGoal.registerTempTable("empgpgoal")

#   Spark Transformation begins here

dfEmpGoal = spark.sql("select a.ReportDate as report_date," +
                      "b.sourceemployeeid,a.KPIName as kpiname," +
                      "b.companycd ," +
                      "'GoogleSheet' as sourcesystemname," +
                      " c.name as employeename," +
                      "CASE WHEN a.KPIName NOT IN ('Gross Profit','WTR'," +
                      "'Accessory Attach Rate', 'GoPhone Auto Enrollment %')" +
                      " and e.pte=0 THEN a.GoalValue/d.approved_headcount " +
                      "WHEN a.KPIName NOT IN ('Gross Profit','WTR'," +
                      "'Accessory Attach Rate','GoPhone Auto Enrollment %')" +
                      " and e.pte=1 THEN ((a.GoalValue/d.approved_headcount)" +
                      "*.75) WHEN a.KPIName ='WTR' then a.GoalValue WHEN " +
                      "a.KPIName ='Accessory Attach Rate' then a.GoalValue " +
                      "WHEN a.KPIName ='GoPhone Auto Enrollment %' then " +
                      "a.GoalValue " +
                      " WHEN a.KPIName ='Gross Profit' and e.pte=0 " +
                      "then e.gpgoal" +
                      " WHEN a.KPIName ='Gross Profit' and e.pte=1 " +
                      "then e.gpgoal END as goalvalue, " +
                      " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                      "as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) " +
                      "as month " +
                      "from storegoal a inner join empass b" +
                      " on a.StoreNumber = b.storenumber " +
                      "inner join emprefine c " +
                      "on b.sourceemployeeid = c.sourceemployeeid " +
                      "inner join storerecruitheadcountrefine d " +
                      "on a.StoreNumber = d.store_number " +
                      "inner join empgpgoal e on " +
                      "c.parttimeindicator = e.pte " +
                      "where b.primarylocationindicator=1")


dfEmpGoal = dfEmpGoal.where(col("report_date").isNotNull())
dfEmpGoal = dfEmpGoal.where(col("sourceemployeeid").isNotNull())
dfEmpGoal = dfEmpGoal.where(col("kpiname").isNotNull())
dfEmpGoal = dfEmpGoal.where(col("companycd").isNotNull())
dfEmpGoal = dfEmpGoal.where(col("sourcesystemname").isNotNull())

dfEmpGoal = dfEmpGoal.withColumn("goalvalue",
                                 sf.round(dfEmpGoal["goalvalue"], 2))


dfEmpGoal = dfEmpGoal.dropDuplicates(
    ['report_date', 'sourceemployeeid', 'companycd', 'kpiname',
     'sourcesystemname'])

dfEmpGoal.coalesce(1).select("*"). write.\
    mode("overwrite").parquet(EmpGoalOP + '/' + 'Working')

dfEmpGoal.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(EmpGoalOP)


spark.stop()
