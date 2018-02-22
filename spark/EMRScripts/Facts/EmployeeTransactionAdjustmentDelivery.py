from pyspark.sql import SparkSession
import sys

EmpTransAdjOut = sys.argv[1]
EmpTransAdjIn = sys.argv[2]


spark = SparkSession.builder.appName("EmpTransAdj").getOrCreate()

dfEmpTransAdj = spark.read.parquet(EmpTransAdjIn)

#############################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

dfEmpTransAdj.registerTempTable("EmpTransAdj")

dfEmpTransAdj = spark.sql("select reportdate as RPT_DT,sourceemployeeid as SRC_EMP_ID,"
                          "adjustmenttype as ADJMNT_TYP,companycd as CO_CD,"
                          "sourcesystemname as SRC_SYS_NM,adjustmentamount as ADJMNT_AMT from EmpTransAdj")

dfEmpTransAdj.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(EmpTransAdjOut + '/' + 'Current')

spark.stop()
