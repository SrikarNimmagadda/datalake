from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
import csv

storeTrafficInput = sys.argv[1]
storeTrafficOutput = sys.argv[2]

spark = SparkSession.builder.appName("DimStoreTrafficCSVToParquet_CSVToParquet").getOrCreate()
schema = StructType([StructField('shoppertrak_locationId', StringType(), False),
                    StructField('storename', StringType(), True),
                    StructField('store_number', StringType(), True),
                    StructField('trafficdate', StringType(), True),
                    StructField('traffictime', StringType(), True),
                    StructField('traffic', StringType(), True),
                    StructField('traffictype', StringType(), False)])

# dfStoreTraffic = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


dfStoreTraffic = spark.sparkContext.textFile(storeTrafficInput).mapPartitions(lambda partition: csv.reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).toDF(schema)


dfStoreTraffic = dfStoreTraffic.withColumn("shoppertraklocationid", dfStoreTraffic["shoppertrak_locationId"].cast(IntegerType()))
dfStoreTraffic = dfStoreTraffic.withColumn("storenumber", dfStoreTraffic["store_number"].cast(IntegerType()))

dfStoreTraffic.registerTempTable("StoreTraffic")
dfStoreTrafficFinal = spark.sql("select a.trafficdate as reportdate,a.shoppertraklocationId,a.storename ,a.storenumber,a.trafficdate,a.traffictime,a.traffic,a.traffictype, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from StoreTraffic a")
dfStoreTrafficFinal.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(storeTrafficOutput)

dfStoreTrafficFinal.coalesce(1).select("*").write.mode("overwrite").parquet(storeTrafficOutput + '/' + 'Working')

spark.stop()
