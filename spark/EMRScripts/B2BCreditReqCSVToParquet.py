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

B2BCreditReqDiscoveryInp = sys.argv[1]	
B2BCreditReqDiscoveryOP = sys.argv[2]

spark = SparkSession.builder.\
        appName("B2BCreditReq_CSVToParquet").getOrCreate()
        
dfB2BCreditReq = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(B2BCreditReqDiscoveryInp)
					
						
dfB2BCreditReq = dfB2BCreditReq.withColumnRenamed("Timestamp", "timestamp").\
						withColumnRenamed("Email Address", "emailaddress").\
						withColumnRenamed("Business Name", "businessname").\
						withColumnRenamed("BAN", "ban").\
						withColumnRenamed("Invoice Number", "invoicenumber").\
						withColumnRenamed("Please upload a copy of the RQ4 invoice", "rqinvoicecopy").\
						withColumnRenamed("RQ 4 Invoice", "rqinvoice").\
						withColumnRenamed("Date of Transaction", "dateoftransaction").\
						withColumnRenamed("Referring Rep", "refferingrep").\
						withColumnRenamed("Rep's Store Number", "repstorenumber").\
						withColumnRenamed("This credit should be applied to this employee...", "crdapptoemp").\
						withColumnRenamed("This credit should be applied to location...", "crdapptoloc").\
						withColumnRenamed("Full GP of the Invoice", "fullgpofinvoice").\
						withColumnRenamed("GP Credit for Store", "gpcrdofstore").\
						withColumnRenamed("Automated Actual 30% Reduction", "automatedactualreduction").\
						withColumnRenamed("GP Reduction to Business Account Executive (-30%)", "gpredtobae").\
						withColumnRenamed("# of CRU GA's to credit", "crugatocrd").\
						withColumnRenamed("# of Tablets to credit", "tabletstocrd").\
						withColumnRenamed("# of HSI to credit", "hsttocrd").\
						withColumnRenamed("# of TV to credit", "tvtocrd").\
						withColumnRenamed("# of Opps to credit", "oppstocrd").\
						withColumnRenamed("Protected Revenue (total)", "protectedreven").\
						withColumnRenamed("Protection Eligible Opps", "protectioneligopps").\
						withColumnRenamed("AMA Units", "amaunits").\
						withColumnRenamed("Accessory Units", "accessoryunits").\
						withColumnRenamed("Accessory Eligible Opps", "acceligopps").\
						withColumnRenamed("Credit goes to this person...", "crdgoesperson").\
						withColumnRenamed("Credit goes to this store...", "crdgoesstore").\
						withColumnRenamed("B2B Manager's Name", "b2bmanagername").\
						withColumnRenamed("Rep Name Match", "repnamematch").\
						withColumnRenamed("Approval", "approval").\
						withColumnRenamed("Reporting Manager", "repomanager").\
						withColumnRenamed("Completed", "completed").\
						withColumnRenamed("Notes", "notes").\
						withColumnRenamed("Reporting Manager Email", "repmanageremail")



dfB2BCreditReq.coalesce(1).select("*"). \
write.mode("overwrite").parquet(B2BCreditReqDiscoveryOP);

spark.stop()