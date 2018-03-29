from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
from pyspark.sql.types import DecimalType


class StoreTransactionAdjustments(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.storeTransAdjustmentsOut = sys.argv[1]
        self.storeTransAdjustmentsIn1 = sys.argv[2]
        self.storeTransAdjustmentsIn2 = sys.argv[3]

    def loadParquet(self):

        dfStoreTransAdjustments1 = self.sparkSession.read.parquet(self.storeTransAdjustmentsIn1)
        dfStoreTransAdjustments2 = self.sparkSession.read.parquet(self.storeTransAdjustmentsIn2)
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

        finalDf1 = self.sparkSession.createDataFrame(dfStoreTransAdjustments1.rdd.flatMap(rowExpander))
        finalDf1.registerTempTable("Store_Trans_Adj1")

        finalDf_Hist = self.sparkSession.sql("select Market as springmarket,reportdate,Region as springregion,District as springdistrict,"
                                             "Location as locationname,Loc as storenumber,AdjustmentType as adjustmenttype,"
                                             "AdjustmentAmount as adjustmentamount,AdjustmentCategory as adjustmentcategory from Store_Trans_Adj1")

        finalDf_Rpt2 = self.sparkSession.sql("select a.Market , a.Region, a.District, a.Location, a.reportdate, b.storenumber, b.gpadjustments_Miscellaneous, b.cruadjustments_Miscellaneous, b.acceligoppsadjustment_Miscellaneous, b.totaloppsadjustment_Miscellaneous from abc a inner join xyz b on b.storenumber=a.Loc")

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

        finalDf2 = self.sparkSession.createDataFrame(finalDf_Rpt2.rdd.flatMap(rowExpander))
        finalDf2.registerTempTable("Store_Trans_Adj2")

        finalDf_Rpt2 = self.sparkSession.sql("select Market as springmarket, reportdate, Region as springregion, District as springdistrict, Location as locationname, storenumber,AdjustmentType as adjustmenttype, AdjustmentAmount as adjustmentamount, AdjustmentCategory as adjustmentcategory from Store_Trans_Adj2")

        finalDf_Rpt2.registerTempTable("RPT")

        finalDf_Rpt = self.sparkSession.sql("select springmarket, reportdate, springregion, springdistrict, locationname, storenumber, adjustmenttype, adjustmentamount, adjustmentcategory from RPT")

        FinalDF = finalDf_Hist.union(finalDf_Rpt)
        FinalDF.registerTempTable("finaltable")

        finalDf1 = self.sparkSession.sql("select springmarket, reportdate, springregion, springdistrict, locationname, storenumber, adjustmenttype, adjustmentcategory, adjustmentamount as adjustmentamount1,'4' companycd, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from finaltable")

        finalDf1 = finalDf1.where(finalDf1.storenumber != '')
        finalDfChangedType = finalDf1.withColumn("adjustmentamount", finalDf1["adjustmentamount1"].cast(DecimalType()))
        finalDfChangedType.drop('adjustmentamount1')
        finalDfChangedType.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTransAdjustmentsOut + '/' + 'Working')
        finalDfChangedType.coalesce(1).select("*").write.mode("append").parquet(self.storeTransAdjustmentsOut + '/' + 'Previous')

        finalDfChangedType.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.storeTransAdjustmentsOut)

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreTransactionAdjustments().loadParquet()
