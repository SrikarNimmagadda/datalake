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
import re

CompanyInp = sys.argv[1]
ATTShippingDetailInp = sys.argv[2]
ProductRefineInp = sys.argv[3] 
StoreRefineInp = sys.argv[4] 
ATTShippingDetailsOP = sys.argv[5] 
FileTime = sys.argv[6]

spark = SparkSession.builder.\
        appName("ATTShippingDetails").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfATTShippingDetail  = spark.read.parquet(ATTShippingDetailInp)
dfProductRefine  = spark.read.parquet(ProductRefineInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   

#dfPurchaseOrderDetail.registerTempTable("purchasingorder")
dfATTShippingDetail.registerTempTable("attshipping")
dfProductRefine.registerTempTable("product")

dfCustomer.registerTempTable("customer")  
            
#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 
#split_col = re.split('(\d+)',dfATTShippingDetail['PONUMBER'])
#split_col = split(dfATTShippingDetail['PONUMBER'], '[A-Z]+')
#dfATTShippingDetail = dfATTShippingDetail.withColumn('storenumber', split_col.getItem(0))
#dfATTShippingDetail = dfATTShippingDetail.withColumn('locationname', split_col.getItem(1))


dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")
dfOutput = spark.sql("select '' as report_date,b.companycode as companycd,a.PONUMBER as ponumber,c.productsku,"
                    + "a.INVOICENUMBER as attinvoicenumber,a.ORDERNUMBER as ordernumber,a.ACTUALSHIPDATE as actualshipdate,"
                    + "a.ITEMNUMBER as manufacturepartnumber,a.ITEMDESCRIPTION as productdescription,c.categoryid as productcategoryid,"
                    + "a.ITEMCATEGORY as productcategoryname,a.UNITPRICE as unitprice,a.EXTDPRICE as extendedprice,"
                    + "a.QUANTITYORDERED as quantityordered,a.QUANTITYSHIPPED as quantityshipped,"
                    + "a.IMEI as imei,a.TRACKINGNUMBER as trackingnumber,"
                    + "'' as springmarket,'' as springregion,'' as springdistrict,"
                    + "a.MARKET as attmarket,a.REGION as attregion,'I' as cdcindicator "
                    + " from attshipping a cross join customer1 b "
                    + "inner join product c "
                    + "on a.ITEMNUMBER = c.manufacturerpartnumber ")
                    
#dfOutput1 = dfOutput.select(split(dfOutput.storenumber, '[A-Z]+').alias('s'),col('businessdate'),col('ponumber'))
#dfOutput1.show()
#df.withColumn("col5", dfOutput["storenumber"].getItem(0)).show()


#df1 = df.select(split(df.s, ',').alias('s1'))
#df1.show()

dfOutput1 = dfOutput.select(col('report_date'),split(dfOutput.ponumber, '[A-Z]+').alias('storenumber'),col('companycd'),\
col('ponumber'),col('productsku'),col('attinvoicenumber'),col('ordernumber'),col('actualshipdate'),\
col('manufacturepartnumber'),col('productdescription'),col('productcategoryid'),col('productcategoryname'),col('unitprice'),\
col('extendedprice'),col('quantityordered'),col('quantityshipped'),col('imei'),col('trackingnumber'),\
col('attmarket'),col('attregion'),col('cdcindicator'))



dfOutput1 = dfOutput1.withColumn("storenumber1", dfOutput1["storenumber"].getItem(0)).withColumn("storenumber2", dfOutput1["storenumber"].getItem(1)).\
withColumn("storenumber3", dfOutput1["storenumber"].getItem(2)).\
select(col("storenumber1"),col("storenumber2"),col('report_date'),col('companycd'),\
col('ponumber'),col('productsku'),col('attinvoicenumber'),col('ordernumber'),col('actualshipdate'),\
col('manufacturepartnumber'),col('productdescription'),col('productcategoryid'),col('productcategoryname'),col('unitprice'),\
col('extendedprice'),col('quantityordered'),col('quantityshipped'),col('imei'),col('trackingnumber'),\
col('attmarket'),col('attregion'),col('cdcindicator'))         
#dfOutput2.show()                  

dfOutput2 = dfOutput1.withColumn("storenumber",sf.when(dfOutput1["storenumber1"] == '',dfOutput1["storenumber2"]).otherwise(dfOutput1["storenumber1"])).\
select(col('report_date'),col("storenumber"),col('companycd'),\
col('ponumber'),col('productsku'),col('attinvoicenumber'),col('ordernumber'),col('actualshipdate'),\
col('manufacturepartnumber'),col('productdescription'),col('productcategoryid'),col('productcategoryname'),col('unitprice'),\
col('extendedprice'),col('quantityordered'),col('quantityshipped'),col('imei'),col('trackingnumber'),\
col('attmarket'),col('attregion'),col('cdcindicator'))  

dfOutput2.registerTempTable("atttemp") 
dfStoreRefine.registerTempTable("store") 

dfatt = spark.sql("select a.report_date,a.storenumber,b.locationname as locationname,a.companycd,a.ponumber,a.productsku,"
        + "a.attinvoicenumber,a.ordernumber,a.actualshipdate,a.manufacturepartnumber,a.productdescription,a.productcategoryid,"
        + "a.productcategoryname,a.unitprice,a.extendedprice,a.quantityordered,a.quantityshipped,a.imei,a.trackingnumber,"
        + "b.market as springmarket,b.region as springregion,b.district as springdistrict,"
        + "'RQ4' as sourcesystemname,"
        + "a.attmarket,a.attregion,a.cdcindicator "
        + "from atttemp a inner join store b "
        + " on a.storenumber = b.storenumber")                          

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfatt.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(ATTShippingDetailsOP)

dfatt.coalesce(1).select("*"). \
write.parquet(ATTShippingDetailsOP + '/' + todayyear + '/' + todaymonth + '/' + 'ATTShippingDetails' + FileTime);
                
spark.stop()

                   