from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("DimCompliance").getOrCreate()

StoreTrafficInput = sys.argv[1]
StoreTrafficOutput = sys.argv[2]

StoreTrafficTable_DF = spark.read.parquet(StoreTrafficInput).registerTempTable("StoreTrafficTable")

Final_DF = spark.sql("select a.reportdate as RPT_DT,a.storenumber as STORE_NUM , a.trafficdate TRAFFIC_DT, a.traffictime TRAFFIC_TM, a.companycd CO_CD, a.sourcesystemlocationid  as SRC_SYS_LOC_ID, a.sourcesystemname SRC_SYS_NM, a.traffictype TRAFFIC_TYP, a.trafficcount TRAFFIC_CNT from StoreTrafficTable a")
Final_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("overwrite").save(StoreTrafficOutput + '/' + 'Current')
Final_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("quoteMode", "All").option("header", "true").mode("overwrite").save(StoreTrafficOutput + '/' + 'Previous')

spark.stop()
