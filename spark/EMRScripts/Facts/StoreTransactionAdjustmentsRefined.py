from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("StoreTransactionAdjustments").getOrCreate()

StoreTransAdjustmentsOut = sys.argv[1]
StoreTransAdjustmentsIn1 = sys.argv[2]
StoreTransAdjustmentsIn2 = sys.argv[3]

dfStoreTransAdjustments1 = spark.read.parquet(StoreTransAdjustmentsIn1)
dfStoreTransAdjustments2 = spark.read.parquet(StoreTransAdjustmentsIn2)
dfStoreTransAdjustments1.registerTempTable("abc")
dfStoreTransAdjustments2.registerTempTable("xyz")


def rowExpander(row):
    rowDict = row.asDict()
    MarketVal = rowDict.pop('Market')
    RegionVal = rowDict.pop('Region')
    DistrictVal = rowDict.pop('District')
    LocationVal = rowDict.pop('Location')
    reportdateVal = rowDict.pop('reportdate')
    LocVal = rowDict.pop('Loc')
    for k in rowDict:
        a = k.split("|")
        yield Row(**{'Market': MarketVal, 'reportdate': reportdateVal, 'Region': RegionVal, 'District': DistrictVal, 'Location': LocationVal, 'Loc': LocVal, 'AdjustmentType': a[0], 'AdjustmentCategory': a[1], 'AdjustmentAmount': row[k]})


FinalDF1 = spark.createDataFrame(dfStoreTransAdjustments1.rdd.flatMap(rowExpander))
FinalDF1.registerTempTable("Store_Trans_Adj1")

FinalDF_HIST = spark.sql("select Market as springmarket, reportdate, Region as springregion,District as springdistrict, "
                         "Location as locationname, Loc as storenumber, AdjustmentType as adjustmenttype, "
                         "AdjustmentAmount as adjustmentamount, AdjustmentCategory as adjustmentcategory from Store_Trans_Adj1")

FinalDF_RPT2 = spark.sql("select a.Market, a.Region, a.District,a.Location, a.reportdate, b.storenumber, b.gpadjustments_Miscellaneous, "
                         "b.cruadjustments_Miscellaneous, "
                         "b.acceligoppsadjustment_Miscellaneous, b.totaloppsadjustment_Miscellaneous from abc a inner join xyz b  on b.storenumber = a.Loc")


def rowExpander(row):
    rowDict1 = row.asDict()
    MarketVal = rowDict1.pop('Market')
    RegionVal = rowDict1.pop('Region')
    DistrictVal = rowDict1.pop('District')
    LocationVal = rowDict1.pop('Location')
    reportdateVal = rowDict1.pop('reportdate')
    storenumberVal = rowDict1.pop('storenumber')
    for k in rowDict1:
        a = k.split("_")
        yield Row(**{'Market': MarketVal, 'reportdate': reportdateVal, 'Region': RegionVal, 'District': DistrictVal, 'Location': LocationVal, 'storenumber': storenumberVal, 'AdjustmentCategory': a[0], 'AdjustmentType': a[1], 'AdjustmentAmount': row[k]})


FinalDF2 = spark.createDataFrame(FinalDF_RPT2.rdd.flatMap(rowExpander))
FinalDF2.registerTempTable("Store_Trans_Adj2")

FinalDF_RPT2 = spark.sql("select Market as springmarket, reportdate, Region as springregion, District as springdistrict, Location as locationname, storenumber, AdjustmentType as adjustmenttype, AdjustmentAmount as adjustmentamount, AdjustmentCategory as adjustmentcategory from Store_Trans_Adj2")

FinalDF_RPT2.registerTempTable("RPT")

FinalDF_RPT = spark.sql("select springmarket, reportdate, springregion, springdistrict, locationname, storenumber, adjustmenttype, adjustmentamount, adjustmentcategory from RPT")

FinalDF = FinalDF_HIST.union(FinalDF_RPT)
FinalDF.registerTempTable("finaltable")

FINALDF1 = spark.sql("select springmarket, reportdate, springregion, springdistrict, locationname, "
                     "storenumber, adjustmenttype, adjustmentcategory, adjustmentamount, '4' companycd, "
                     "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()), 6, 2) as month from finaltable")

FINALDF1 = FINALDF1.where(col("storenumber").isNotNull())

FINALDF1.coalesce(1).select("*").write.mode("overwrite").parquet(StoreTransAdjustmentsOut + '/' + 'Working')

FINALDF1.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(StoreTransAdjustmentsOut)
spark.stop()
