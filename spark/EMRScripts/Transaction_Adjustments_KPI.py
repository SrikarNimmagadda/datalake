#This module for Sales Transaction Adjustments KPI #############

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


TransactionAdjustmentsInp = sys.argv[1]
TransactionAdjustmentsKPIOp = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("SalesTransactionAdjustmentsKPI").getOrCreate()
        
#########################################################################################################
#                                 Read the source files                                              #
#########################################################################################################
        
dfTransactionAdjustmentsInp  = spark.read.parquet(TransactionAdjustmentsInp).registerTempTable("transactionadjustments")

'''
Report Date: DATE NOT NULL
Sto re Numb er : INTEGER NOT NULL (FK)
Source Emp loy ee Id: INTEGE R NOT NULL (FK)
KPI Name: V ARCHAR(50 ) NOT NULL (FK)
Company Cd: INTEGER NOT NULL (FK)
KPI V al ue: INTEGE R NOT NULL
'''
dfFinal = spark.sql("select a.storenumber,'TransactionAdjustments' as kpiname,"
                + "sum(a.adjustmentamount) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from transactionadjustments a "
                + "group by a.storenumber")

#"where a.kpi = 'Tablet Pstpd GA Cnt' "#
#a.businessdate as reportdate,

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfFinal.coalesce(1).\
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(TransactionAdjustmentsKPIOp)

dfFinal.coalesce(1).select("*"). \
write.parquet(TransactionAdjustmentsKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesTransactionAdjustmentsKPI' + FileTime);
                
spark.stop()


                   