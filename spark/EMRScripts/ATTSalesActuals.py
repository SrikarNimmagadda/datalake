#This module for PurchaseOrderDetailsReport Refine#############

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf


CompanyInp = sys.argv[1]
ATTMyResultsInp = sys.argv[2]
StoreRefineInp = sys.argv[3] 
StoreDealerAssInp = sys.argv[4]
#ATTDealerCodeInp = sys.argv[5]
ATTSalesActualOP = sys.argv[5] 
FileTime = sys.argv[6]

spark = SparkSession.builder.\
        appName("ATTSalesActuals").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfATTMyResults  = spark.read.parquet(ATTMyResultsInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
#dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   

#dfPurchaseOrderDetail.registerTempTable("purchasingorder")
dfStoreRefine.registerTempTable("store")
#dfATTDealer.registerTempTable("dealer")
dfStoreDealerAss.registerTempTable("storedealerass")
dfCustomer.registerTempTable("customer") 

todaymonth = datetime.now().strftime('%m')
todayday =  datetime.now().strftime('%d')

#thirtyonemonthlist = [1,3,5,7,8,10,12]
#thirtymonthlist = [4,6,9,11]

thirtyonemonthlist = ['1','3','5','7','8','10','12']
thirtymonthlist = ['4','6','9','11']
if todaymonth in thirtyonemonthlist:
    todaymonth = 31
elif todaymonth in thirtymonthlist:
    todaymonth = 30
else:
    todaymonth = 28
    
dfATTMyResults = dfATTMyResults.withColumn('projectedvalue',sf.col("TotalActual") * todaymonth / todayday)
#dfATTMyResults = dfATTMyResults.withColumn('saleamount',sf.col("quantity") * sf.col("price"))				   

dfATTMyResults.registerTempTable("myresult") 


dfCustomer.registerTempTable("customer")  
            
#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 



dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfOutput = spark.sql("select distinct b.StoreNumber as storenumber,a.Dlr1Code as dealercode,d.companycode as companycd,"
            + "'' as report_date,a.KPI as kpi,a.TotalActual as actualvalue,a.projectedvalue,"
            + "c.Market as springmarket,c.Region as springregion,"
            + "c.District as springdistrict,c.locationname as storename, a.LocId as attlocationid,a.Location attlocationname,"
            + "a.Market as attmarket,a.Region as attregion " #'I' as cdcindicator "
            + "from myresult a inner join storedealerass b"
            + " on a.Dlr1Code = b.DealerCode "
            + "inner join store c "
            + "on b.StoreNumber = c.storenumber "
            + "cross join customer1 d")


# a.TotalActual*(DAY(EOMONTH(GETDATE()))/DAY(GETDATE())) as projectedvalue                  
todayyear = datetime.now().strftime('%Y')
             

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(ATTSalesActualOP)

dfOutput.coalesce(1).select("*"). \
write.parquet(ATTSalesActualOP + '/' + todayyear + '/' + datetime.now().strftime('%m') + '/' + 'ATTSalesActual' + FileTime);
                
spark.stop()

                   