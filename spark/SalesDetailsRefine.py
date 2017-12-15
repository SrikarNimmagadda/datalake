from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import date_sub

EmpployeeRefineInp = sys.argv[1]
StoreRefineInp = sys.argv[2]
SalesDeailsInp = sys.argv[3]
CompanyInp = sys.argv[4]
SalesDetailsOP = sys.argv[5]
FileTime = sys.argv[6]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("SalesLeadRefine").getOrCreate()
				   
dfEmpployeeRefine  = spark.read.parquet(EmpployeeRefineInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfSalesDetails  = spark.read.parquet(SalesDeailsInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
				   
dfSalesDetails = dfSalesDetails.withColumn('rawGrossProfit',sf.col("quantity") * sf.col("price") - sf.col("quantity") * sf.col("cost"))
dfSalesDetails = dfSalesDetails.withColumn('saleamount',sf.col("quantity") * sf.col("price"))				   
				   
dfEmpployeeRefine.registerTempTable("emprefine")
dfStoreRefine.registerTempTable("storerefine")

split_col = split(dfSalesDetails['storename'], ' ')

dfSalesDetails = dfSalesDetails.withColumn('storenumber', split_col.getItem(0))
dfSalesDetails = dfSalesDetails.withColumn('primarylocationname', split_col.getItem(1))
dfSalesDetails.registerTempTable("salesdetails")
dfCustomer.registerTempTable("company")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    

dfCustomer = spark.sql("select a.companycode from company a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer")

FinalJoin_DF = spark.sql("select date_sub(current_timestamp(), 1) as report_date,b.storenumber as storenumber,"
+ "a.productsku as productsku, a.invoicenumber as invoicenumber, a.datecreated as invoicedate, a.lineid as lineid,"
+ "a.employeeid sourceemployeeid, d.companycode as companycd, '' as sourcesystemname,"
+ "a.customerid as customerid, a.customername as customername, c.name as employeename, a.price as price,"
+ "a.cost as cost, a.rqpriority as rqpriority, a.quantity as quantity, a.serialnumber as productserialnumber,"
+ "'' as serialproductid, a.rawGrossProfit as rawgrossprofit,a.saleamount as saleamount,"
+ "b.Market as springmarket, b.Region as springregion, b.District as springdistrict, b.LocationCode as locationname "
+ "from salesdetails a inner join storerefine b "
+ "on a.storenumber = b.storenumber "
+ "inner join emprefine c "
+ "on a.employeeid = c.sourceemployeeid "
+ "cross join customer d")


FinalJoin_DF.coalesce(1).select("*"). \
       write.format("com.databricks.spark.csv").\
       option("header", "true").mode("overwrite").save(SalesDetailsOP);
				
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')				


FinalJoin_DF.coalesce(1).select("*"). \
write.parquet(SalesDetailsOP + '/' + todayyear + '/' + todaymonth + '/' + 'SalesDetailsRefine' + FileTime);

spark.stop()
