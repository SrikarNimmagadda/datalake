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
import csv

ATTScoredGoalInp = sys.argv[1]
ATTOutputArg = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        
'''
        
Schema = StructType() .\
                        add("Store", StringType(), True). \
                        add("District", StringType(), True). \
                        add("Gross Profit", StringType(), True). \
                        add("AdvTV", StringType(), True). \
                        add("Digital Life", StringType(), True). \
                        add("Acc GP/Opp", StringType(), True). \
                        add("CRU GA", StringType(), True). \
                        add("Tablets", StringType(), True). \
                        add("Integrated Products", StringType(), True). \
                        add("WTR", StringType(), True). \
                        add("Go Phone", StringType(), True). \
                        add("OT Hours", StringType(), True). \
                        add("Close Rate", StringType(), True). \
                        add("Accessory GP", StringType(), True). \
                        add("Accessory Attach Rate", StringType(), True). \
                        add("Traffic", StringType(), True). \
                        add("PPGA VAB", StringType(), True). \
                        add("AdvTV VAB", StringType(), True). \
                        add("DLife VAB", StringType(), True). \
                        add("CRU VAB", StringType(), True). \
                        add("HSI", StringType(), True). \
                        add("Approved FTE", StringType(), True).\
                        add("Approved HC (floaters)", StringType(), True). \
                        add("Approved HC", StringType(), True)
                        
df = spark.sparkContext.textFile(ATTScoredGoalInp)\
        .mapPartitions(lambda partition: csv.reader([line.replace('\0','') for line in partition],delimiter=',', quotechar='"')).filter(lambda line: len(line) > 1 and line[0] != 'Col1')\
        .toDF(Schema)
'''        
df = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(ATTScoredGoalInp)
                       
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#df.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(ATTOutputArg)

df.coalesce(1).select("*"). \
    write.parquet(ATTOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'ATTScoreGoal' + FileTime);
    
spark.stop()      