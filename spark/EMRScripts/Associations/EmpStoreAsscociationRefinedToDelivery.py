from pyspark.sql import SparkSession
import sys
from datetime import datetime

EmpStoreAssociationRefineInp = sys.argv[1]
EmpStoreAssociationdeliveryOP = sys.argv[2]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
    appName("employeeRefine").getOrCreate()

dfEmpStoreAssociation = spark.read.parquet(EmpStoreAssociationRefineInp)
dfEmpStoreAssociation.registerTempTable("employee")


#  Spark Transformation begins here

dfEmpStoreAssociation = spark.sql("select a.storenumber as STORE_NUM,"
                                  + "a.sourceemployeeid as SRC_EMP_ID,"
                                  + "a.companycd as CO_CD,"
                                  + "a.sourcesystenname as SRC_SYS_NM,"
                                  + " a.primarylocationindicator as PRI_LOC_IN,"
                                  + " a.CDC_IND_CD as CDC_IND_CD "
                                  + "from employee a")


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfEmpStoreAssociation.coalesce(1).\
    write.format("com.databricks.spark.csv").option("header", "true").mode(
        "overwrite").save(EmpStoreAssociationdeliveryOP + '/' + 'Current')

dfEmpStoreAssociation.coalesce(1).\
    write.format("com.databricks.spark.csv").option("header", "true").mode(
        "append").save(EmpStoreAssociationdeliveryOP + '/' + 'Previous')


spark.stop()
