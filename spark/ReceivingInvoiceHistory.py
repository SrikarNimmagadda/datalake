#This module for Company Refine#############

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
from pyspark.sql.functions import regexp_replace

CompanyInp = sys.argv[1]
ReceivingInvoiceInp = sys.argv[2]
StoreRefineInp = sys.argv[3] 
ATTRefineInp = sys.argv[4] 
EmployeeRefineInp = sys.argv[5]
StoreDealerAssInp = sys.argv[6]
ReceivingInvoiceOP = sys.argv[7] 
FileTime = sys.argv[8]

spark = SparkSession.builder.\
        appName("ReceivingInvoiceHistory").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfReceivingInvoice  = spark.read.parquet(ReceivingInvoiceInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfATTRefine  = spark.read.parquet(ATTRefineInp)
dfEmployeeRefine  = spark.read.parquet(EmployeeRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   


dfStoreRefine.registerTempTable("store")
dfATTRefine.registerTempTable("dealer")
dfCustomer.registerTempTable("customer")  
dfEmployeeRefine.registerTempTable("employee")  
dfStoreDealerAss.registerTempTable("storedealerass")

dfReceivingInvoice = dfReceivingInvoice.withColumnRenamed("ReceivingID", "receivingid").\
            withColumnRenamed("ProductIdentifier", "productsku").\
            withColumnRenamed("SerialNumber", "productserialnumber").\
            withColumnRenamed("ReceivingIDByStore", "receivingidbystore").\
            withColumnRenamed("DateReceived", "receiveddate").\
            withColumnRenamed("PurchaseOrderID", "sourcepurchaseorderid").\
            withColumnRenamed("ReferenceNumber", "referencenumber").\
            withColumnRenamed("VendorSKU", "vendorsku").\
            withColumnRenamed("ProductName", "productname").\
            withColumnRenamed("Quantity", "quantityreceived").\
            withColumnRenamed("UnitCost", "unitcost").\
            withColumnRenamed("TotalCost", "totalcost").\
            withColumnRenamed("ReconciledDate", "reconcileddate").\
            withColumnRenamed("ReasonCode", "correctionreasoncode").\
            withColumnRenamed("VendorName", "vendorname").\
            withColumnRenamed("EmployeeUsername", "employeename1").\
            withColumnRenamed("AccountNumber", "accountnumber").\
            withColumnRenamed("ReceivingComments","receivingcomments")

dfReceivingInvoice_df = dfReceivingInvoice.withColumn("employeename", regexp_replace("employeename1", "[\.\s]+", " ")).drop('employeename1')             
dfReceivingInvoice_df.registerTempTable("receivinginvoice")            
#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")
dfOutput = spark.sql("select b.companycode as companycd,a.receivingid,a.productsku,a.productserialnumber,"
                    + "a.receivingidbystore,'' as report_date,a.receiveddate,a.sourcepurchaseorderid,"
                    + "'RQ4' as sourcesystemname,a.referencenumber,"
                    + "a.vendorsku,a.productname,a.quantityreceived,a.unitcost,"
                    + "a.totalcost,"
                    + "case when a.Reconciled = FALSE then '0' else '1' end as reconciledindicator, "
                    + "a.reconcileddate,"
                    + "case when a.Correction = FALSE then '0' else '1' end as correctionindicator, "
                    + "a.correctionreasoncode,a.vendorname,a.employeename,c.sourceemployeeid as employeeid,"
                    + "a.accountnumber,a.receivingcomments,a.VendorTerms as vendorterms,"
                    + "'I' as cdcindicator "
                    + " from receivinginvoice a cross join customer1 b "
                    + "inner join employee c "
                    + "on a.employeename = c.name ")
                   
               
             
dfOutput1 = dfOutput.select(split(dfOutput.receivingidbystore, '[A-Z]+').alias('storenumber'),col('companycd'),\
col('receivingid'),col('productsku'),col('productserialnumber'),col('receivingidbystore'),col('report_date'),\
col('receiveddate'),col('sourcepurchaseorderid'),col('sourcesystemname'),col('referencenumber'),col('vendorsku'),\
col('productname'),col('quantityreceived'),col('unitcost'),col('totalcost'),col('reconciledindicator'),\
col('reconcileddate'),col('correctionindicator'),col('correctionreasoncode'),\
col('vendorname'),col('employeename'),col('employeeid'),col('accountnumber'),col('receivingcomments'),col('vendorterms'),col('cdcindicator'))

dfOutput1 = dfOutput1.withColumn("storenumber1", dfOutput1["storenumber"].getItem(0)).withColumn("storenumber2", dfOutput1["storenumber"].getItem(1)).\
withColumn("storenumber3", dfOutput1["storenumber"].getItem(2)).\
select(col("storenumber1"),col("storenumber2"),col('companycd'),\
col('receivingid'),col('productsku'),col('productserialnumber'),col('receivingidbystore'),col('report_date'),\
col('receiveddate'),col('sourcepurchaseorderid'),col('sourcesystemname'),col('referencenumber'),col('vendorsku'),\
col('productname'),col('quantityreceived'),col('unitcost'),col('totalcost'),col('reconciledindicator'),\
col('reconcileddate'),col('correctionindicator'),col('correctionreasoncode'),\
col('vendorname'),col('employeename'),col('employeeid'),col('accountnumber'),col('receivingcomments'),col('vendorterms'),col('cdcindicator'))        
#dfOutput2.show()                  
#dfOutput2.show()                  

dfOutput2 = dfOutput1.withColumn("storenumber",sf.when(dfOutput1["storenumber1"] == '',dfOutput1["storenumber2"]).otherwise(dfOutput1["storenumber1"])).\
select(col("storenumber"),col('companycd'),\
col('receivingid'),col('productsku'),col('productserialnumber'),col('receivingidbystore'),col('report_date'),\
col('receiveddate'),col('sourcepurchaseorderid'),col('sourcesystemname'),col('referencenumber'),col('vendorsku'),\
col('productname'),col('quantityreceived'),col('unitcost'),col('totalcost'),col('reconciledindicator'),\
col('reconcileddate'),col('correctionindicator'),col('correctionreasoncode'),\
col('vendorname'),col('employeename'),col('employeeid'),col('accountnumber'),col('receivingcomments'),col('vendorterms'),col('cdcindicator'))

dfOutput2.registerTempTable("receivinginvoice") 

dfreceiving = spark.sql("select distinct a.companycd,a.receivingid,a.productsku,a.productserialnumber,"
            + "a.receivingidbystore,a.report_date,a.receiveddate,a.sourcepurchaseorderid,"
            + "a.sourcesystemname,a.storenumber,b.locationname as locationname,a.referencenumber,"
            + "a.vendorsku,a.productname,a.quantityreceived,a.unitcost,"
            + "a.totalcost,a.reconciledindicator,a.reconcileddate,a.correctionindicator,"
            + "a.correctionreasoncode,a.vendorname,a.employeename,a.employeeid,a.accountnumber,a.receivingcomments,a.vendorterms,"
            + "b.market as springmarket,b.region as springregion,b.district as springdistrict,"
            + "c.Market as attmarket,c.Region as attregion,"
            + "a.cdcindicator "
            + "from receivinginvoice a inner join store b "
            + " on a.storenumber = b.storenumber "
            + "inner join storedealerass d "
            + "on a.storenumber = d.StoreNumber "
            + "inner join dealer c "
            + "on a.storenumber = c.TBLoc OR a.storenumber = c.SMFMapping ")
                                   
                
              
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfreceiving.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(ReceivingInvoiceOP)

dfreceiving.coalesce(1).select("*"). \
write.parquet(ReceivingInvoiceOP + '/' + todayyear + '/' + todaymonth + '/' + 'ReceivingInvoiceHistory' + FileTime);
                
spark.stop()

                   