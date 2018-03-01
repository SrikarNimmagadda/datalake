from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("DimStoreTrafficRefined").getOrCreate()

StoreTrafficInput = sys.argv[1]
StoreRefineInp = sys.argv[2]
StoreTrafficRefine = sys.argv[3]

SpringCustExpInput_DF = spark.read.parquet(StoreTrafficInput).registerTempTable("StoreTrafficInputTable")
dfStoreRefine = spark.read.parquet(StoreRefineInp).registerTempTable("storerefine")
Final_Joined_DF = spark.sql("select Distinct a.reportdate as reportdate, a.storenumber as storenumber,"
                            "a.trafficdate as trafficdate, a.traffictime as traffictime, "
                            "'4' as companycd,  a.storename as locationname, a.shoppertraklocationId as sourcesystemlocationid, "
                            "'TBSFTP' as sourcesystemname, a.traffictype as traffictype, a.traffic as trafficcount, b.SpringMarket as springmarket, "
                            "b.SpringRegion as springregion,b.SpringDistrict as springdistrict, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as "
                            "year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                            "from StoreTrafficInputTable a "
                            "inner join storerefine b "
                            "on a.storenumber = b.StoreNumber ")

Final_Joined_DF.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(StoreTrafficRefine)

Final_Joined_DF.coalesce(1).select("*").write.mode("overwrite").parquet(StoreTrafficRefine + '/' + 'Working')

spark.stop()
