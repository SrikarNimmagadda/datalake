from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
import sys
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as sf
from pyspark.sql.functions import col


EmployeeMasterListInp = sys.argv[1]  # employee source parquet
EmployeeMasterListOp = sys.argv[2]


spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()

dfEmployeeMasterList = spark.read.parquet(EmployeeMasterListInp)

dfEmployeeMasterList.registerTempTable("employee")

# Spark Transformation begins here

dfEmpStoreAssociation = spark.sql("select a.assignedlocations as storenumber," +
                                  " a.primarylocation, 4 as companycd," +
                                  " a.employeeid as sourceemployeeid, " +
                                  "'RQ4' as sourcesystenname," +
                                  "CASE WHEN a.rowevent = 'Inserted' THEN 'I' " +
                                  "WHEN a.rowevent = 'Added' THEN 'I' " +
                                  "WHEN a.rowevent = 'Updated' THEN 'C' END " +
                                  "as CDC_IND_CD, " +
                                  "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                                  "as YEAR," +
                                  "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) " +
                                  "as MONTH from employee a")

dfEmpStoreAssociationInd = dfEmpStoreAssociation.\
    withColumn('storenumber', explode(split('storenumber', ',')))
dfEmpStoreAssociationInd1 = dfEmpStoreAssociationInd.\
    withColumn('primarylocationindicator',
               sf.expr("IF(INSTR(storenumber, primarylocation) > 0, 1, 0)"))
# dfEmpStoreAssInd1.show()

split_col1 = split(dfEmpStoreAssociationInd1['storenumber'], ' ')

dfEmpStoreAssociationInd2 = dfEmpStoreAssociationInd1.\
    withColumn('storenumber', split_col1.getItem(1))

dfEmpStoreAssociationInd3 = dfEmpStoreAssociationInd2.withColumn(
    "storenumber", dfEmpStoreAssociationInd2["storenumber"].cast(IntegerType()))

dfEmpStoreAssociationInd4 = dfEmpStoreAssociationInd3.where(col("storenumber").isNotNull())

# dfEmpStoreAssInd4.show()

dfEmpStoreAssociationInd4.coalesce(1).select("*").\
    write.mode("overwrite").parquet(EmployeeMasterListOp + '/' + 'Working')

dfEmpStoreAssociationInd4.coalesce(1).write.mode('append').partitionBy(
    'YEAR', 'MONTH').format('parquet').save(EmployeeMasterListOp)


spark.stop()
