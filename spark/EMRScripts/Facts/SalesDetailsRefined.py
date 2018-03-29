from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import sys
import pyspark.sql.functions as sf
EmpployeeRefineInp = sys.argv[1]
StoreRefineInp = sys.argv[2]
SalesDeailsInp = sys.argv[3]
SalesDetailsOP = sys.argv[4]
# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SalesLeadRefine").getOrCreate()
dfEmpployeeRefine = spark.read.parquet(EmpployeeRefineInp)
dfStoreRefine = spark.read.parquet(StoreRefineInp)
dfSalesDetails = spark.read.parquet(SalesDeailsInp)
dfSalesDetails = dfSalesDetails.withColumn('rawGrossProfit', sf.col("quantity") * sf.col("price") - sf.col("quantity") * sf.col("cost"))
dfSalesDetails = dfSalesDetails.withColumn('saleamount', sf.col("quantity") * sf.col("price"))
dfEmpployeeRefine.registerTempTable("emprefine")
dfStoreRefine.registerTempTable("storerefine")

split_col = split(dfSalesDetails['storename'], ' ')

dfSalesDetails = dfSalesDetails.withColumn('storenumber', split_col.getItem(0))
dfSalesDetails = dfSalesDetails.withColumn('primarylocationname', split_col.getItem(1))
dfSalesDetails.registerTempTable("salesdetails")

####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

FinalJoin_DF = spark.sql("select a.datecreated as reportdate, a.storenumber as storenumber, a.productsku as productsku, a.invoicenumber as invoicenumber, a.lineid as lineid, a.employeeid as sourceemployeeid, '4' as companycd, 'RQ4' as sourcesystemname, a.datecreated as invoicedate, a.customerid as sourcecustomerid, a.serialnumber as productserialnumber, a.customername as customername, a.employeename as employeename, a.price as price, a.cost as cost, a.rqpriority as rqpriority, a.quantity as quantity, a.specialproductid as specialproductid, a.rawGrossProfit as rawgrossprofit, a.saleamount as saleamount, a.channelname as springmarket, b.SpringRegion as springregion, b.SpringDistrict as springdistrict, b.LocationName as locationname from salesdetails a inner join storerefine b on a.storenumber = b.StoreNumber ")
FinalJoin_DF.registerTempTable("SalesDetails")
FinalJoin_DF.printSchema()
FinalJoin_DF.show(1000)
FinalJoin_DFFinal = spark.sql("select a.reportdate, a.storenumber, a.productsku, a.invoicenumber, a.lineid, a.sourceemployeeid, a.companycd, a.sourcesystemname, a.invoicedate, a.sourcecustomerid, a.productserialnumber, a.customername, a.employeename, a.price, a.cost, a.rqpriority, a.quantity, a.specialproductid, a.rawgrossprofit, a.saleamount, a.springmarket, a.springregion, a.springdistrict, a.locationname, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from SalesDetails a")
FinalJoin_DFFinal = FinalJoin_DFFinal.na.fill({'reportdate': 'NA', 'storenumber': -1, 'productsku': 'NA', 'invoicenumber': 'NA', 'lineid': -1, 'sourceemployeeid': 'NA', 'companycd': -1, 'sourcesystemname': 'NA', 'employeename': 'NA', 'springmarket': 'NA', 'springregion': 'NA', 'springdistrict': 'NA', 'locationname': 'NA'})
FinalJoin_DFFinal.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(SalesDetailsOP)
FinalJoin_DFFinal.coalesce(1).select("*").write.mode("overwrite").parquet(SalesDetailsOP + '/' + 'Working')
spark.stop()
