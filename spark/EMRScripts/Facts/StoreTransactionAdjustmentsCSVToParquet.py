from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import split
import csv
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("StoreTransactionAdjustments").getOrCreate()

StoreTransAdjustmentsOut = sys.argv[1]
StoreTransAdjustmentsIn = sys.argv[2]
StoreTransAdjustmentsMisc = sys.argv[3]

schema = StructType([StructField('Market', StringType(), True), StructField('Region', StringType(), True), StructField('District', StringType(), False), StructField('Location', StringType(), True), StructField('Loc', StringType(), True), StructField('ATT|FeaturesCommission', StringType(), True), StructField('RetailIQ|FeaturesCommission', StringType(), True), StructField('Uncollected|FeaturesCommission|1', StringType(), True), StructField('Uncollected|FeaturesCommission%|2', StringType(), True), StructField('Dispute|FeaturesCommission', StringType(), True), StructField('Uncollected|Features', StringType(), True), StructField('Uncollected|%Features', StringType(), True), StructField('Wired|Disputes', StringType(), True), StructField('TV|Disputes', StringType(), True), StructField('TV|Extras', StringType(), True), StructField('Wired|Extras', StringType(), True), StructField('AT&T|DTVNOW', StringType(), True), StructField('RetailIQ|DTVNow', StringType(), True), StructField('DirecTVNow|Disputes', StringType(), True), StructField('Uncollected|DirecTVNow|2', StringType(), True), StructField('Incorrect|ActivationorUpgrade', StringType(), True), StructField('Incorrect|DF', StringType(), True), StructField('Incorrect|Prepaid', StringType(), True), StructField('Incorrect|BYOD', StringType(), True), StructField('Incorrect|Total', StringType(), True), StructField('DF|Accessories', StringType(), True), StructField('Total|Adjustment', StringType(), True), StructField('Uncollected|Deposits', StringType(), True), StructField('Total|AdjustmentwithDeposits', StringType(), True)])

dfStoreTransAdj = spark.sparkContext.textFile(StoreTransAdjustmentsIn).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: line[0] != 'Market' or len(line[1]) != 0).toDF(schema)

dfStoreTransAdj.printSchema()

dfStoreTransAdjMisc = spark.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").load(StoreTransAdjustmentsMisc)

split_col = split(dfStoreTransAdjMisc['Location'], ' ')

dfStoreTransAdjMisc = dfStoreTransAdjMisc.withColumn('storenumber', split_col.getItem(0))


dfStoreTransAdjMisc = dfStoreTransAdjMisc.withColumnRenamed("Location", "location_Miscellaneous").\
    withColumnRenamed("GP Adjustment", "gpadjustments_Miscellaneous").\
    withColumnRenamed("CRU Adjustment", "cruadjustments_Miscellaneous").\
    withColumnRenamed("Acc Elig Opps Adjustment", "acceligoppsadjustment_Miscellaneous").\
    withColumnRenamed("Total Opps Adjustment", "totaloppsadjustment_Miscellaneous")

dfStoreTransAdjMisc = dfStoreTransAdjMisc.drop("location_zyxwv")

dfStoreTransAdj1 = dfStoreTransAdj.where(dfStoreTransAdj['Market'] != 'Oct-17')
dfStoreTransAdj2 = dfStoreTransAdj1.where(dfStoreTransAdj['Market'] != 'Market')

today = datetime.now().strftime('%m/%d/%Y')
dfStoreTransAdj2 = dfStoreTransAdj2.withColumn('reportdate', lit(today))

dfStoreTransAdj2.coalesce(1).select("*").write.mode("overwrite").parquet(StoreTransAdjustmentsOut + '/' + 'Working1')
dfStoreTransAdjMisc.coalesce(1).select("*").write.mode("overwrite").parquet(StoreTransAdjustmentsOut + '/' + 'Working2')

spark.stop()
