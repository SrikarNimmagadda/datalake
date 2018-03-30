from pyspark.sql import SparkSession
import sys

EmpployeeRefineInp = sys.argv[1]
StoreRefineInp = sys.argv[2]
StoreDealerAssInp = sys.argv[3]
ATTDealerCodeInp = sys.argv[4]
SalesLeadInp = sys.argv[5]
SalesLeadOP = sys.argv[6]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SalesLeadRefine").getOrCreate()
dfEmpployeeRefine = spark.read.parquet(EmpployeeRefineInp)
dfStoreRefine = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss = spark.read.parquet(StoreDealerAssInp)
dfATTDealer = spark.read.parquet(ATTDealerCodeInp)
dfSalesLead = spark.read.parquet(SalesLeadInp)
dfEmpployeeRefine.registerTempTable("emprefine")
dfStoreRefine.registerTempTable("storerefine")
dfStoreDealerAss.registerTempTable("storedealerass")
dfATTDealer.registerTempTable("attdealer")
dfSalesLead.registerTempTable("salesleadtable")
####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################
Temp_DF = spark.sql("select a.reportdate as reportdate, b.storenumber as storenumber, a.dealercode as dealercode, '4' as companycd, a.SpringMarket as springmarket, a.SpringRegion as springregion, a.SpringDistrict as springdistrict, a.dos as attdirectorofsales, a.arsm as attregionsalesmanager, a.account as customeraccountname, a.existingcustomer as currencustomerindicator, a.repname as salesrepresentativename, a.status as leadstatus, a.grossadds as grossactivations, a.sbassistancerequested as sbassistancerequestindicator, a.enterdate as entereddate, a.closedate as closeddate, a.lastupdate as lastupdatedate, a.bae as baeemployeename, a.description as description, a.contactinfo as contactinfo, a.fan as fan, d.attlocationid as attlocationid, d.attlocationname as attlocationname, a.market as attmarket, a.region as attregion, c.LocationName as locationname, 'GoogleFile' as sourcesystemname, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from salesleadtable a left join storedealerass b on a.dealercode = b.dealercode left join storerefine c on b.storenumber = c.StoreNumber left join attdealer d on b.dealercode = d.dealercode where b.associationtype = 'Retail' and  b.associationstatus='Active'")

Temp_DF = Temp_DF.dropDuplicates(['storenumber', 'dealercode', 'companycd', 'reportdate', 'springmarket', 'springregion', 'springdistrict', 'attdirectorofsales', 'attregionsalesmanager', 'customeraccountname', 'currencustomerindicator', 'salesrepresentativename', 'leadstatus', 'grossactivations', 'sbassistancerequestindicator', 'entereddate', 'closeddate', 'lastupdatedate', 'baeemployeename', 'description', 'contactinfo', 'fan', 'attlocationid', 'attlocationname', 'attmarket', 'attregion', 'locationname', 'sourcesystemname'])
Temp_DF.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(SalesLeadOP)
Temp_DF.coalesce(1).select("*").write.mode("overwrite").parquet(SalesLeadOP + '/' + 'Working')
spark.stop()
