from pyspark.sql import SparkSession
import sys


class ATTSalesActualsRefined(object):

    def __init__(self):
        self.ATTSalesActualOP = sys.argv[1]
        self.AttSalesActualsInp1 = sys.argv[2]
        self.StoreRefineInp = sys.argv[3]
        self.StoreDealerAssInp = sys.argv[4]
        self.ATTDealerCodeInp = sys.argv[5]
        self.AttSalesActualsInp2 = sys.argv[6]

    def loadRefined(self):
        spark = SparkSession.builder.appName("ATTSalesActuals").getOrCreate()

        #########################################################################################################
        #                                 Read the 2 source files                                              #
        #########################################################################################################

        dfAttSalesActualsInput = spark.read.parquet(self.AttSalesActualsInp1)
        dfAttSalesActualsInputRPT = spark.read.parquet(self.AttSalesActualsInp2)
        dfStoreRefine = spark.read.parquet(self.StoreRefineInp)
        dfStoreDealerAss = spark.read.parquet(self.StoreDealerAssInp)
        dfATTDealer = spark.read.parquet(self.ATTDealerCodeInp)

        dfAttSalesActualsInput.registerTempTable("HIST")
        dfAttSalesActualsInput.printSchema()

        dfAttSalesActualsInputRPT.registerTempTable("RPT")
        dfAttSalesActualsInputRPT.printSchema()

        # FinalATTDF = spark.sql("select a.attlocationname,a.locid,a.dealercode,a.kpiname,a.reportdate,"
        #                       "a.kpiid,a.actualvalue,case WHEN (a.dealercode=b.dealercode2 and a.kpiname = b.kpiname2 and a.reportdate=b.reportdate2) THEN 1 ELSE 0 END as currentkpiindicator ,"
        #                       "a.projectedvalue from HIST a inner join RPT b on  a.attlocationname = b.attlocationname2")

        FinalATTDF = spark.sql("select a.attlocationname,a.locid,a.dealercode,a.kpiname,a.reportdate,"
                               "a.kpiid,a.actualvalue,case WHEN (a.dealercode=b.dealercode2 and a.kpiname = b.kpiname2 and a.reportdate=b.reportdate2) THEN 1 ELSE 0 END as currentkpiindicator ,"
                               "a.projectedvalue from HIST a left join RPT b on  a.dealercode=b.dealercode2 and a.kpiname = b.kpiname2 and a.reportdate=b.reportdate2 ")

        FinalATTDF.show(5000, False)

        FinalATTDF.registerTempTable("ATTSalesActuals")
        dfStoreRefine.registerTempTable("store")
        dfATTDealer.registerTempTable("ATTDealer")
        dfStoreDealerAss.registerTempTable("storedealerass")

        #########################################################################################################
        #                                 Spark Transformation begins here                                      #
        #########################################################################################################

        dfOutput = spark.sql("select distinct a.attlocationname,a.dealercode,a.kpiid,a.kpiname,"
                             "a.actualvalue,a.projectedvalue,b.StoreNumber as storenumber,"
                             "'4' as companycode,c.SpringMarket as springmarket,c.SpringRegion as springregion,c.SpringDistrict as springdistrict,a.reportdate,"
                             "d.attlocationid,d.attmarket,d.attregion,c.LocationName as locationname,a.currentkpiindicator "
                             "from ATTSalesActuals a inner join storedealerass b on a.dealercode = b.DealerCode inner join"
                             " store c on b.StoreNumber = c.StoreNumber inner join ATTDealer d on a.dealercode=d.dealercode where b.AssociationType='Retail' and b.AssociationStatus='Active' ")

        dfOutput.show(5000, False)

        FinaldfOutput1 = dfOutput.dropDuplicates(['dealercode', 'kpiname', 'reportdate'])

        FinaldfOutput1.registerTempTable("Final")

        FinaldfOutput = spark.sql("select attlocationname,dealercode,kpiname,actualvalue,projectedvalue,"
                                  "storenumber,companycode,springmarket,springregion,springdistrict,"
                                  "reportdate,attlocationid,attmarket,attregion,locationname,"
                                  "currentkpiindicator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                  "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from Final")
        FinaldfOutput.coalesce(1).select("*").write.mode("overwrite").parquet(self.ATTSalesActualOP + '/' + 'Working')

        FinaldfOutput.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.ATTSalesActualOP)

        spark.stop()


if __name__ == "__main__":
    ATTSalesActualsRefined().loadRefined()
