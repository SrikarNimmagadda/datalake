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


SpringCommCust = sys.argv[1]
CustomerInp = sys.argv[2]
CompanyOutputArg = sys.argv[3] 
CompanyFileTime = sys.argv[4]

spark = SparkSession.builder.\
        appName("CompanyRefinary").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfSpringComm  = spark.read.parquet(SpringCommCust)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CustomerInp)
                   
dfSpringComm = dfSpringComm.withColumnRenamed("VIPCustomer", "vipcustomer")

dfSpringComm.registerTempTable("springcomm")
dfCustomer.registerTempTable("customer")
                   

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")
dfSpringComm = spark.sql("select a.customeridentifier as sourceidentifier,b.companycode as companycd,'RQ4' as sourcesystemname,"
                    + "a.firstname,a.lastname,a.companyname,"
                    + "a.customertype,"
                    + "a.city,a.stateprovince,a.zippostalcode,a.country,a.multilevelpriceidentifier,"
                    + "a.billingaccountnumber,a.totalactivations,a.datecreated as datecreatedatsource,"
                    + "a.vipcustomer as vipcustomerindicator,'I' as cdcindicator"
                    + " from springcomm a cross join customer1 b")
                    
               
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfSpringComm.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(CompanyOutputArg)

dfSpringComm.coalesce(1).select("*"). \
write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'CUSTOMER' + CompanyFileTime);
                
spark.stop()

                   