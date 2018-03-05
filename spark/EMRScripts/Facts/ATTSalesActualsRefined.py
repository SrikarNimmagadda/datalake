from pyspark.sql import SparkSession
import sys


class ATTSalesActualsRefined(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.attSalesActualOP = sys.argv[1]
        self.attSalesActualsInp1 = sys.argv[2]
        self.storeRefineInp = sys.argv[3]
        self.storeDealerAssInp = sys.argv[4]
        self.attDealerCodeInp = sys.argv[5]
        self.attSalesActualsInp2 = sys.argv[6]

        #########################################################################################################
        #                                 Read the 2 source files                                              #
        #########################################################################################################

    def loadParquet(self):

        dfAttSalesActualsInput = self.sparkSession.read.parquet(self.attSalesActualsInp1)
        dfAttSalesActualsInputRpt = self.sparkSession.read.parquet(self.attSalesActualsInp2)
        dfStoreRefine = self.sparkSession.read.parquet(self.storeRefineInp)
        dfStoreDealerAss = self.sparkSession.read.parquet(self.storeDealerAssInp)
        dfATTDealer = self.sparkSession.read.parquet(self.attDealerCodeInp)

        dfAttSalesActualsInput.registerTempTable("HIST")

        dfAttSalesActualsInputRpt.registerTempTable("RPT")

        finalAttDf = self.sparkSession.sql("select a.attlocationname,a.locid,a.dealercode,a.kpiname,a.reportdate,"
                                           "a.kpiid,a.actualvalue,case WHEN (a.dealercode=b.dealercode2 and a.kpiname = b.kpiname2 and a.reportdate=b.reportdate2) THEN 1 ELSE 0 END as currentkpiindicator ,"
                                           "a.projectedvalue from HIST a inner join RPT b on  a.attlocationname = b.attlocationname2")

        finalAttDf.registerTempTable("ATTSalesActuals")
        dfStoreRefine.registerTempTable("store")
        dfATTDealer.registerTempTable("ATTDealer")
        dfStoreDealerAss.registerTempTable("storedealerass")

        #########################################################################################################
        #                                 Spark Transformation begins here                                      #
        #########################################################################################################

        dfOutput = self.sparkSession.sql("select distinct a.attlocationname,a.dealercode,a.kpiid,a.kpiname,"
                                         "a.actualvalue,a.projectedvalue,b.StoreNumber as storenumber,"
                                         "'4' as companycode,c.SpringMarket as springmarket,c.SpringRegion as springregion,c.SpringDistrict as springdistrict,a.reportdate,"
                                         "d.attlocationid,d.attmarket,d.attregion,c.LocationName as locationname,a.currentkpiindicator "
                                         "from ATTSalesActuals a inner join storedealerass b on a.dealercode = b.DealerCode inner join"
                                         " store c on b.StoreNumber = c.StoreNumber inner join ATTDealer d on a.dealercode=d.dealercode where b.AssociationType='Retail' and b.AssociationStatus='Active' and a.currentkpiindicator=1")

        finalDfOutput1 = dfOutput.dropDuplicates(['dealercode', 'kpiname', 'reportdate'])

        finalDfOutput1.registerTempTable("Final")
        finalDfOutput = self.sparkSession.sql("select attlocationname,dealercode,kpiname,actualvalue,projectedvalue,"
                                              "storenumber,companycode,springmarket,springregion,springdistrict,"
                                              "reportdate,attlocationid,attmarket,attregion,locationname,"
                                              "currentkpiindicator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                              "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from Final")

        finalDfOutput.coalesce(1).select("*").write.mode("overwrite").parquet(self.attSalesActualOP + '/' + 'Working')

        finalDfOutput.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.attSalesActualOP)

        self.sparkSession.stop()


if __name__ == "__main__":
    ATTSalesActualsRefined().loadParquet()
