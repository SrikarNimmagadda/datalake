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


SalesDeailsInp = sys.argv[1]
SalesDetailsOP = sys.argv[2]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("SalesLeadDelivery").getOrCreate()
		   

#Reading Parquert and creating dataframe
dfSalesDetails = spark.read.parquet("s3n://tb-us-east-1-dev-refined-regular/Sales/2017/11/SalesDetailsRefine20171111")
dfSalesDetails  = spark.read.parquet(SalesDeailsInp)
			   
#Register spark temp schema
dfSalesDetails.registerTempTable("salesdetails")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    


Temp_DF = spark.sql("select a.storenumber as STORE_NUM, a.companycd as CO_CD, a.invoicenumber as INVOICE_NBR,"
+ "a.lineid as LINE_ID, a.productsku as PROD_SKU,a.sourceemployeeid as SRC_EMP_ID,"
+ "a.report_date as RPT_DT, a.invoicedate as INVOICE_DT, '' as SRC_ID, a.sourcesystemname as SRC_SYS_NM,"
+ "a.productserialnumber as PROD_SN, a.quantity as QTY, a.rqpriority as RQ_PRITY, a.cost as CST, a.price as PRC,"
+ "a.rawgrossprofit as RAW_GRS_PRFT, a.serialproductid as SPCL_PROD_ID, a.saleamount as SALE_AMT "
+ "from salesdetails a")

FinalJoin_DF = Temp_DF.filter(Temp_DF.PROD_SN != 'null')

FinalJoin_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(SalesDetailsOP);
                
spark.stop()