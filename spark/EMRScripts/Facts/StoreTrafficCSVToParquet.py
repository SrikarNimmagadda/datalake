# ********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimStoreCnCConversionCSVtoParquet                                                                                               *
#   Written By            : Aakash Basu                                                                                                      *
#   Date Written          : October 2017                                                                                                     *
#   Project Name          : GameStop                                                                                                         *
#   Version               : 1.0                                                                                                              *
#   Description           : This code is for retrieving the customer experience of stores                                                    *
#   Change                : None																                                             *
#   History 	          : None                                                                                                             *
#   NOTES                 : Complete                                                                                                         *
#   Spark Submit Command  : spark-submit --class spark.FactDeposits --master yarn-client --num-executors 30 --executor-cores 4               *
#                          --executor-memory 24G --driver-memory 4G                                                                          *
#                          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0                               *
#                          target/scala-2.11/factdeposits_2.11-1.0.jar <FactSnapShot HDFS Path> <DimCustomer HDFS Path>                      *
#                          <DimCalendar HDFS Path> <DimProduct HDFS Path> <DimScenario  HDFS Path> <PrmMain HDFS Path>                       *
#                          <BplData_norm HDFS Path> <parallelism config value> <shuffle partitions config value>                             *
#                          <storage memoryfraction config value> <maxresultsize config value> <shuffle spillafterread config value>          *
#                          <executor memoryoverhead config value>                                                                            *
# ********************************************************************************************************************************************


from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.functions import lit
import csv

storeTrafficInput = sys.argv[1]
storeTrafficOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("DimStoreTrafficCSVToParquet_CSVToParquet").getOrCreate()

schema = StructType([StructField('shoppertrak_locationId', StringType(), False),
                    StructField('storename', StringType(), True),
                    StructField('store_number', StringType(), True),
                    StructField('trafficdate', StringType(), True),
                    StructField('traffictime', StringType(), True),
                    StructField('traffic', StringType(), True),
                    StructField('traffictype', StringType(), False)])

# dfStoreTraffic = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


dfStoreTraffic = spark.sparkContext.textFile(storeTrafficInput).mapPartitions(lambda partition: csv.reader([line.encode('utf-8') for line in partition], delimiter=',', quotechar='"')).toDF(schema)

today = datetime.now().strftime('%m/%d/%Y')
dfStoreTraffic = dfStoreTraffic.withColumn('reportdate', lit(today))

dfStoreTraffic = dfStoreTraffic.withColumn("shoppertraklocationid", dfStoreTraffic["shoppertrak_locationId"].cast(IntegerType()))
dfStoreTraffic = dfStoreTraffic.withColumn("storenumber", dfStoreTraffic["store_number"].cast(IntegerType()))

dfStoreTraffic.registerTempTable("StoreTraffic")
dfStoreTrafficFinal = spark.sql("select a.reportdate,a.shoppertraklocationId,a.storename ,a.storenumber,a.trafficdate,a.traffictime,a.traffic,a.traffictype, "
                                + "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from StoreTraffic a")

dfStoreTrafficFinal.show()

dfStoreTrafficFinal.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(storeTrafficOutput)

dfStoreTrafficFinal.coalesce(1).select("*").write.mode("overwrite").parquet(storeTrafficOutput + '/' + 'Working')

spark.stop()
