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

OperationalEfficiencyScoreCard = sys.argv[1]
OperationalEfficiencyScoreCardOutputArg = sys.argv[2]

spark = SparkSession.builder.\
        appName("OperationalEfficiencyScoreCard_CSVToParquet").getOrCreate()
        
dfProductCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(OperationalEfficiencyScoreCard)
						
dfProductCust = dfProductCust.withColumnRenamed("Market", "market").\
                              withColumnRenamed("Region", "region").\
                              withColumnRenamed("District", "district").\
                              withColumnRenamed("Location", "location").\
                              withColumnRenamed("Sales Person", "sales_person").\
                              withColumnRenamed("Total Loss", "total_loss").\
			  				  withColumnRenamed("Total Issues", "total_issues").\
			 				  withColumnRenamed("Action Taken", "action_taken").\
			 				  withColumnRenamed("HR Consulted Before Termination", "hr_consulted_before_termination").\
			 				  withColumnRenamed("Transaction Errors", "transaction_errors").\
			 				  withColumnRenamed("Total Errors", "total_errors").\
							  withColumnRenamed("NEXT Trades", "next_trades").\
							  withColumnRenamed("Total Devices12", "total_devices1").\
							  withColumnRenamed("Hyla Loss", "hyla_loss").\
							  withColumnRenamed("Total Devices14", "total_devices2").\
							  withColumnRenamed("Denied RMA Devices", "denied_rma_devices").\
							  withColumnRenamed("Total Devices16", "total_device3").\
							  withColumnRenamed("Cash Deposits", "cash_deposits").\
							  withColumnRenamed("Total Missing Deposits", "total_missing_deposits").\
							  withColumnRenamed("Total Short Deposits", "total_short_deposits").\
							  withColumnRenamed("Shrinkage", "shrinkage").\
							  withColumnRenamed("Comments", "comments")


dfProductCust.coalesce(1).select("*"). \
    write.mode("overwrite").parquet(OperationalEfficiencyScoreCardOutputArg + '/');
    
spark.stop()