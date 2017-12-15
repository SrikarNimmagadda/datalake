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

SalesLeadInput = sys.argv[1]
SalesLeadOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("SalesLead_CSVToParquet").getOrCreate()
        
dfSalesLead = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(SalesLeadInput)
					
						
dfSalesLead = dfSalesLead.withColumnRenamed("Master_Dealer", "masterdealer").\
                              withColumnRenamed("Region", "region").\
                              withColumnRenamed("Market", "market").\
                              withColumnRenamed("DOS", "dos").\
                              withColumnRenamed("ARSM", "arsm").\
			  				  withColumnRenamed("Store_Name", "storename").\
			 				  withColumnRenamed("Store_ID", "storeid").\
							  withColumnRenamed("Account", "account").\
							  withColumnRenamed("Description", "description").\
							  withColumnRenamed("Contact_Info", "contactinfo").\
							  withColumnRenamed("Win_The_Neighborhood", "wintheneighborhood").\
							  withColumnRenamed("Customer_ZIP", "customerzip").\
							  withColumnRenamed("Existing_Customer", "existingcustomer").\
							  withColumnRenamed("FAN", "fan").\
							  withColumnRenamed("Rep_Name", "repname").\
							  withColumnRenamed("Rep_Dealer_Code", "repdealercode").\
							  withColumnRenamed("Status", "status").\
							  withColumnRenamed("Gross_Adds", "grossadds").\
							  withColumnRenamed("SB_Assistance_Requested", "sbassistancerequested").\
							  withColumnRenamed("Enter_Date", "enterdate").\
							  withColumnRenamed("Close_Date", "closedate").\
							  withColumnRenamed("Follow_Up_Notes", "followupnotes").\
							  withColumnRenamed("Last_Update", "lastupdate").\
							  withColumnRenamed("Dealer Code", "dealercode").\
							  withColumnRenamed("Spring Market", "springmarket").\
							  withColumnRenamed("Spring Region", "springregion").\
							  withColumnRenamed("Spring District", "springdistrict").\
							  withColumnRenamed("BAE", "bae")



dfSalesLead.coalesce(1).select("*"). \
write.mode("overwrite").parquet(SalesLeadOutput);

spark.stop()