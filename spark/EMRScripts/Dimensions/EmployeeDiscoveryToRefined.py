from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

EmployeeInp = sys.argv[1]
EmployeeOutputArg = sys.argv[2]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()


#  Reading the source data file


dfEmployee = spark.read.parquet(EmployeeInp)


#  Read the 2 source files

dfEmployee.registerTempTable("emp")

#  Spark Transformation begins here

dfEmployee = spark.sql("select a.employeeid as sourceemployeeid, "
                       + "4 as companycd, 'RQ4' as sourcesystemname,"
                       + "a.specialidentifier as workdayid,"
                       + "a.employeename as name,"
                       + "a.gender, a.emailpersonal as workemail,"
                       + "CASE WHEN a.disabled = 'TRUE' THEN 1 ELSE 0 END "
                       + "as statusindicator,"
                       + "CASE WHEN a.parttime = 'TRUE' THEN 1 ELSE 0 END "
                       + "as parttimeindicator,"
                       + "a.title, a.language, a.securityrole as jobrole,"
                       + "a.city, a.stateprovince, a.country,"
                       + "a.usermanageremployeeid as employeemanagerid,"
                       + " a.supervisor as employeemanagername,"
                       + " a.startdate,"
                       + "CASE WHEN a.terminationdate = 'TODAY' THEN '' ELSE "
                       + "a.terminationdate END as terminationdate,"
                       + "a.commissiongroup, a.compensationtype,"
                       + "a.username, a.phone as worknumber, "
                       + "a.email as email,"
                       + "CASE WHEN a.rowevent = 'Inserted' "
                       + "THEN 'I' WHEN a.rowevent = 'Added' THEN 'I' "
                       + "WHEN a.rowevent = 'Updated' THEN 'C' END "
                       + "as CDC_IND_CD,"
                       + "a.rowinserted as datecreatedatsource, "
                       + "a.rowupdated as employeelastmodifieddate,"
                       + " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) "
                       + "as YEAR,"
                       + "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) "
                       + "as MONTH "
                       + "from emp a")


dfEmployee = dfEmployee.withColumn("startdate",
                                   dfEmployee["startdate"].cast(DateType()))
dfEmployee = dfEmployee.withColumn(
    "terminationdate", dfEmployee["terminationdate"].cast(DateType()))
dfEmployee = dfEmployee.where(col("sourceemployeeid").isNotNull())
dfEmployee = dfEmployee.where(col("companycd").isNotNull())
dfEmployee = dfEmployee.where(col("sourcesystemname").isNotNull())


dfEmployee.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(EmployeeOutputArg + '/' + 'Working')


dfEmployee.coalesce(1).write.mode('append').partitionBy(
    'YEAR', 'MONTH').format('parquet').save(EmployeeOutputArg)


spark.stop()
