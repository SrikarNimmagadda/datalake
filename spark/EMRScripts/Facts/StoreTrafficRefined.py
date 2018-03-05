from pyspark.sql import SparkSession
import sys


class StoreTrafficRefined(object):

        def __init__(self):

                self.appName = self.__class__.__name__
                self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
                self.storeTrafficInput = sys.argv[1]
                self.storeRefineInp = sys.argv[2]
                self.storeTrafficRefine = sys.argv[3]

                #########################################################################################################
                #                                 Reading the source data files                                         #
                #########################################################################################################

        def loadParquet(self):

                springCustExpInput_Df = self.sparkSession.read.parquet(self.storeTrafficInput)
                springCustExpInput_Df.registerTempTable("StoreTrafficInputTable")
                dfStoreRefine = self.sparkSession.read.parquet(self.storeRefineInp)
                dfStoreRefine.registerTempTable("storerefine")
                final_Joined_Df = self.sparkSession.sql("select Distinct a.reportdate as reportdate, a.storenumber as storenumber,"
                                                        "a.trafficdate as trafficdate, a.traffictime as traffictime, "
                                                        "'4' as companycd,  a.storename as locationname, a.shoppertraklocationId as sourcesystemlocationid, "
                                                        "'TBSFTP' as sourcesystemname, a.traffictype as traffictype, a.traffic as trafficcount, b.SpringMarket as springmarket, "
                                                        "b.SpringRegion as springregion,b.SpringDistrict as springdistrict, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as "
                                                        "year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month "
                                                        "from StoreTrafficInputTable a "
                                                        "inner join storerefine b "
                                                        "on a.storenumber = b.StoreNumber ")

                final_Joined_Df.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(self.storeTrafficRefine)

                final_Joined_Df.coalesce(1).select("*").write.mode("overwrite").parquet(self.storeTrafficRefine + '/' + 'Working')

                self.sparkSession.stop()


if __name__ == "__main__":
        StoreTrafficRefined().loadParquet()
