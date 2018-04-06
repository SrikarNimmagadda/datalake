from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, regexp_extract
import sys
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


EmployeeMasterListInp = sys.argv[1]  # employee source parquet
EmployeeMasterListOp = sys.argv[2]


spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()

dfEmployeeMasterList = spark.read.parquet(EmployeeMasterListInp)

dfEmployeeMasterList.registerTempTable("employee")

# Spark Transformation begins here

dfEmpStoreAssociationInd = spark.sql("select a.assignedlocations " +
                                     "as storenumber," +
                                     " a.primarylocation, 4 as companycd," +
                                     " a.sourceemployeeid as sourceemployeeid, " +
                                     "'RQ4' as sourcesystenname," +
                                     "a.CDC_IND_CD as CDC_IND_CD, " +
                                     "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                                     "as year," +
                                     "SUBSTR(FROM_UNIXTIME" +
                                     "(UNIX_TIMESTAMP())," +
                                     "6,2) " +
                                     "as month from employee a")


dfEmpStoreAssociationInd.registerTempTable("empstore1")

dfEmpStoreAssociationInd = spark.sql("select CASE when storenumber " +
                                     "is null then primarylocation else " +
                                     "storenumber end as storenumber, " +
                                     "primarylocation, companycd, " +
                                     "sourceemployeeid, sourcesystenname, " +
                                     "CDC_IND_CD, year," +
                                     " month  from empstore1 ")

dfEmpStoreAssociationInd.registerTempTable("empstore2")

dfEmpStoreAssociationInd = spark.sql("select case when " +
                                     "primarylocation is null " +
                                     "then storenumber" +
                                     " else concat(storenumber,', " +
                                     "',primarylocation) " +
                                     "end as storenumber, primarylocation, " +
                                     "companycd, sourceemployeeid, " +
                                     "sourcesystenname, CDC_IND_CD, " +
                                     "year, month  from empstore2")

dfEmpStoreAssociationInd = dfEmpStoreAssociationInd.\
    withColumn('storenumber', explode(split('storenumber', ',')))

dfEmpStoreAssociationInd.registerTempTable("empstore3")


dfEmpStoreAssociationInd = spark.sql("select CASE when " +
                                     "primarylocation is null then ' -2' " +
                                     "else storenumber end as storenumber, " +
                                     "primarylocation, companycd, " +
                                     "sourceemployeeid, sourcesystenname, " +
                                     "CDC_IND_CD, year, month " +
                                     "from empstore3 ")

# dfEmpStoreAssociationInd2.show()

split_col1 = split(dfEmpStoreAssociationInd['storenumber'], ' ')

dfEmpStoreAssociationInd2 = dfEmpStoreAssociationInd.\
    withColumn('storenumber', split_col1.getItem(1))

split_col2 = split(dfEmpStoreAssociationInd2['primarylocation'], ' ')

dfEmpStoreAssociationInd2 = dfEmpStoreAssociationInd2.\
    withColumn('primarylocation1', split_col2.getItem(0))

dfEmpStoreAssociationInd2 = dfEmpStoreAssociationInd2.withColumn(
    'storenumber', regexp_extract('storenumber', '(.*?)(-?\d+)', 2))

dfEmpStoreAssociationInd2 = dfEmpStoreAssociationInd2.withColumn(
    'primarylocation1', regexp_extract('primarylocation1', '(.*?)(-?\d+)', 2))

dfEmpStoreAssociationInd2.registerTempTable("empstore4")

dfEmpStoreAssociationInd2 = spark.sql("select *, case when " +
                                      "storenumber==primarylocation1 " +
                                      "then 1 else 0 end as " +
                                      "primarylocationindicator " +
                                      "from empstore4")

dfEmpStoreAssociationInd2.show()

dfEmpStoreAssociationInd2.registerTempTable("empstore5")

dfEmpStoreAssociationInd3 = spark.sql("select  storenumber, " +
                                      "primarylocation, " +
                                      "companycd, sourceemployeeid, " +
                                      "sourcesystenname, CDC_IND_CD, " +
                                      "year, month,CASE WHEN storenumber " +
                                      "like '%-2%' THEN '1' else " +
                                      "primarylocationindicator " +
                                      " END as primarylocationindicator " +
                                      "from empstore5 ")

dfEmpStoreAssociationInd4 = dfEmpStoreAssociationInd3.\
    withColumn("storenumber", dfEmpStoreAssociationInd3["storenumber"].
               cast(IntegerType()))

dfEmpStoreAssociationInd5 = dfEmpStoreAssociationInd4.\
    where(col("storenumber").isNotNull())
dfEmpStoreAssociationInd6 = dfEmpStoreAssociationInd5.\
    where(col("sourceemployeeid").isNotNull())
dfEmpStoreAssociationInd7 = dfEmpStoreAssociationInd6.\
    where(col("companycd").isNotNull())
dfEmpStoreAssociationInd8 = dfEmpStoreAssociationInd7.\
    where(col("sourcesystenname").isNotNull())

dfEmpStoreAssociationInd9 = dfEmpStoreAssociationInd8.dropDuplicates(
    ['storenumber', 'sourceemployeeid', 'companycd', 'sourcesystenname'])

dfEmpStoreAssociationInd9.coalesce(1).select("*").\
    write.mode("overwrite").parquet(EmployeeMasterListOp + '/' + 'Working')

dfEmpStoreAssociationInd9.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(EmployeeMasterListOp)

spark.stop()
