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

SalesDetailsInp = sys.argv[1]	
SalesDetailsOP = sys.argv[2]
FileTime = sys.argv[3]	

spark = SparkSession.builder.\
        appName("SalesDetails_CSVToParquet").getOrCreate()
        
dfSalesDetails = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(SalesDetailsInp)
					
						
dfSalesDetails = dfSalesDetails.withColumnRenamed("ChannelName", "channelname").\
								withColumnRenamed("StoreID", "storeid").\
								withColumnRenamed("StoreName", "storename").\
								withColumnRenamed("CustomerID", "customerid").\
								withColumnRenamed("CustomerName", "customername").\
								withColumnRenamed("EmployeeID", "employeeid").\
								withColumnRenamed("DateCreated", "datecreated").\
								withColumnRenamed("InvoiceNumber", "invoicenumber").\
								withColumnRenamed("LineID", "lineid").\
								withColumnRenamed("ProductSKU", "productsku").\
								withColumnRenamed("Price", "price").\
								withColumnRenamed("Cost", "cost").\
								withColumnRenamed("RQPriority", "rqpriority").\
								withColumnRenamed("Quantity", "quantity").\
								withColumnRenamed("EmployeeName", "employeename").\
								withColumnRenamed("SerialNumber", "serialnumber").\
								withColumnRenamed("DivisionID", "divisionid").\
								withColumnRenamed("AccessedDate", "accesseddate")



#dfSalesDetails.coalesce(1).select("*"). \
#write.mode("overwrite").parquet(SalesDetailsOP);

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfSalesDetails.coalesce(1).select("*"). \
write.parquet(SalesDetailsOP + '/' + todayyear + '/' + todaymonth + '/' + 'SalesDetailsOP' + FileTime);

spark.stop()