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
PurchaseOrderDetailInp = sys.argv[2]
StoreRefineInp = sys.argv[3] 
StoreDealerAssInp = sys.argv[4]
ATTDealerCodeInp = sys.argv[5]
EmployeeRefineInp = sys.argv[6]
ProductRefineInp = sys.argv[7]
PurchaseOrderOP = sys.argv[8] 
FileTime = sys.argv[9]

spark = SparkSession.builder.\
        appName("PurchaseOrderDetailsReport").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfPurchaseOrderDetail  = spark.read.parquet(PurchaseOrderDetailInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)
dfEmployeeRefine  = spark.read.parquet(EmployeeRefineInp)
dfProductRefine = spark.read.parquet(ProductRefineInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   

#dfPurchaseOrderDetail.registerTempTable("purchasingorder")
dfStoreRefine.registerTempTable("store")
dfATTDealer.registerTempTable("dealer")
dfStoreDealerAss.registerTempTable("storedealerass")
dfCustomer.registerTempTable("customer")  
dfEmployeeRefine.registerTempTable("employee") 
dfProductRefine.registerTempTable("product")  

dfPurchaseOrderDetail = dfPurchaseOrderDetail.withColumnRenamed("PurchaseOrderID", "sourcepurchaseorderid").\
            withColumnRenamed("PurchaseOrderIDentifier", "purchaseorderidentifier").\
            withColumnRenamed("ProductName", "productname").\
            withColumnRenamed("GlobalProductID", "productcategoryid").\
            withColumnRenamed("Category", "productcategoryname").\
            withColumnRenamed("QuantityOrdered", "quantityordered").\
            withColumnRenamed("QuantityReceived", "quantityreceived").\
            withColumnRenamed("TotalQuantityReceived", "totalquantityreceived").\
            withColumnRenamed("TotalCostReceived", "totalcostreceived").\
            withColumnRenamed("VendorIDentifier", "vendorsku").\
            withColumnRenamed("UnitCost", "unitcost").\
            withColumnRenamed("DateCommitted", "ordersubmitteddate").\
            withColumnRenamed("DateETA", "orderetadate").\
            withColumnRenamed("EmployeeName", "employeename").\
            withColumnRenamed("PartiallyReceived", "partiallyreceivedindicator").\
            withColumnRenamed("FullyReceived", "fullyreceivedindicator").\
            withColumnRenamed("OrderingComments", "ordercomments").\
            withColumnRenamed("VendorName", "vendorname").\
            withColumnRenamed("OrderEntryID","orderentryid").\
            withColumnRenamed("OrderEntryIDByStore","orderentryidbystore").\
            withColumnRenamed("VendorTerms","vendorterms").\
            withColumnRenamed("COGSAccountNumber","cogsaccountnumber")
            #withColumnRenamed("VendorIDentifier","vendoridentifier")
            
split_col = split(dfPurchaseOrderDetail['ReceiverStoreName'], ' ')
dfPurchaseOrderDetail = dfPurchaseOrderDetail.withColumn('storenumberorderedfor', split_col.getItem(0))
dfPurchaseOrderDetail = dfPurchaseOrderDetail.withColumn('locationnameorderedfor', split_col.getItem(1))

split_col = split(dfPurchaseOrderDetail['OrdererStoreName'], ' ')
dfPurchaseOrderDetail = dfPurchaseOrderDetail.withColumn('storenumberorderedby', split_col.getItem(0))
dfPurchaseOrderDetail = dfPurchaseOrderDetail.withColumn('locationnameorderedby', split_col.getItem(1))

dfPurchaseOrderDetail.registerTempTable("purchasingorder")
             

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")
dfOutput = spark.sql("select b.companycode as companycd,a.sourcepurchaseorderid,'RQ4' as sourcesystemname,"
                    + "c.productsku,'' as businessdate,a.purchaseorderidentifier,"
                    + "case when a.POCompleted = FALSE then '0' else '1' end as pocompleteindicator, "
                    + "a.storenumberorderedfor,a.locationnameorderedfor,a.productname,a.productcategoryid,"
                    + "a.productcategoryname,a.quantityordered,a.quantityreceived,a.totalquantityreceived,"
                    + "a.totalcostreceived,"
                    + "a.vendorsku,a.unitcost,a.ordersubmitteddate,a.storenumberorderedby,a.locationnameorderedby,"
                    + "a.orderetadate,d.sourceemployeeid as employeeid,a.employeename,a.partiallyreceivedindicator,"
                    + "a.fullyreceivedindicator,a.ordercomments,a.vendorname,a.orderentryid,a.orderentryidbystore,"
                    + "a.vendorterms,a.cogsaccountnumber,e.Market as springmarket,e.Region as springregion,"
                    + "e.District as springdistrict,g.Market as attmarket,g.Region as attregion,a.vendorsku as vendoridentifier,"
                    + "'I' as cdcindicator "
                    + " from purchasingorder a cross join customer1 b "
                    + "inner join product c "
                    + "on a.productname = c.productname "
                    + "inner join employee d "
                    + "on a.employeename = d.name "
                    + "inner join store e "
                    + "on a.storenumberorderedfor = e.StoreNumber "
                    + "inner join storedealerass f "
                    + "on a.storenumberorderedfor = f.StoreNumber "
                    + "inner join dealer g "
                    + "on a.storenumberorderedfor = g.TBLoc OR a.storenumberorderedfor = g.SMFMapping ")
                    
                  
               
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfOutput.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(PurchaseOrderOP)

dfOutput.coalesce(1).select("*"). \
write.parquet(PurchaseOrderOP + '/' + todayyear + '/' + todaymonth + '/' + 'PurchaseOrderDetails' + FileTime);
                
spark.stop()

                   