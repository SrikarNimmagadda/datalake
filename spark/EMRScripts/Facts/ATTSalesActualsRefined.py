from pyspark.sql import SparkSession
import sys

attSalesActualOP = sys.argv[1]
attSalesActualsInp1 = sys.argv[2]
storeRefineInp = sys.argv[3]
storeDealerAssInp = sys.argv[4]
attDealerCodeInp = sys.argv[5]
attSalesActualsInp2 = sys.argv[6]

spark = SparkSession.builder.appName("ATTSalesActualsRefined").getOrCreate()

#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################

dfAttSalesActualsInput = spark.read.parquet(attSalesActualsInp1)
dfAttSalesActualsInputRPT = spark.read.parquet(attSalesActualsInp2)
dfStoreRefine = spark.read.parquet(storeRefineInp)
dfStoreDealerAss = spark.read.parquet(storeDealerAssInp)
dfATTDealer = spark.read.parquet(attDealerCodeInp)

dfAttSalesActualsInput.registerTempTable("HIST")
dfAttSalesActualsInput.printSchema()

dfAttSalesActualsInputRPT.registerTempTable("RPT")
dfAttSalesActualsInputRPT.printSchema()

FinalATTDF = spark.sql("select a.attlocationname,a.locid,a.dealercode,a.kpiname,a.reportdate,"
                       "a.kpiid,a.actualvalue,case WHEN (a.dealercode=b.dealercode2 and a.kpiname = b.kpiname2 and a.reportdate=b.reportdate2) THEN 1 ELSE 0 END as currentkpiindicator ,"
                       "a.projectedvalue from HIST a inner join RPT b on  a.attlocationname = b.attlocationname2")

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
                     " store c on b.StoreNumber = c.StoreNumber inner join ATTDealer d on a.dealercode=d.dealercode where b.AssociationType='Retail' and b.AssociationStatus='Active' and a.currentkpiindicator=1")

FinaldfOutput1 = dfOutput.dropDuplicates(['dealercode', 'kpiname', 'reportdate'])

FinaldfOutput1.registerTempTable("Final")
FinaldfOutput = spark.sql("select attlocationname,dealercode,kpiname,actualvalue,projectedvalue,"
                          "storenumber,companycode,springmarket,springregion,springdistrict,"
                          "reportdate,attlocationid,attmarket,attregion,locationname,"
                          "currentkpiindicator,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                          "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from Final")

FinaldfOutput.coalesce(1).select("*").write.mode("overwrite").parquet(attSalesActualOP + '/' + 'Working')

FinaldfOutput.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(attSalesActualOP)

spark.stop()
