from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("DimStoreTransAdjDelivery").getOrCreate()

DimCustExpRefinedOutput = sys.argv[1]
DimStoreTransAdjRefinedInput = sys.argv[2]

DimCustExpRefinedInput_DF = spark.read.parquet(DimStoreTransAdjRefinedInput).registerTempTable("DimStoreTransAdjRefinedTable")

Final_Joined_DF = spark.sql("select reportdate as RPT_DT,a.storenumber as STORE_NUM,"
                            "a.adjustmentcategory as ADJMNT_CAT,a.adjustmenttype as ADJMNT_TYP,"
                            "a.companycd as CO_CD,a.adjustmentamount as ADJMNT_AMT from DimStoreTransAdjRefinedTable a")

Final_Joined_DF.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DimCustExpRefinedOutput + '/' + 'Current')

spark.stop()
