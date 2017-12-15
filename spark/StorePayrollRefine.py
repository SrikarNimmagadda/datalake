#This module for Product Refine#############

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

StoreRefineInp = sys.argv[1]
StoreDealerAssInp = sys.argv[2]
ATTDealerCodeInp = sys.argv[3]
CompanyInp = sys.argv[4]
SalePayrolInp = sys.argv[5]
SalePayrolOp = sys.argv[6]
FileTime = sys.argv[7]

spark = SparkSession.builder.\
        appName("StorePayrolRefine").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)
dfSalePayrol  = spark.read.parquet(SalePayrolInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   

dfStoreRefine.registerTempTable("store")
dfStoreDealerAss.registerTempTable("storedealerass")
dfATTDealer.registerTempTable("dealer")

dfCustomer.registerTempTable("customer")

split_col = split(dfSalePayrol['RowLabels'], ' ')
dfSalePayrol = dfSalePayrol.withColumn('storenumber', split_col.getItem(0))
#dfSalePayrol = dfSalePayrol.withColumn('locationname', split_col.getItem(1))
dfSalePayrol = dfSalePayrol.withColumnRenamed("Payroll%ofGP", "PayrollofGP")
dfSalePayrol.registerTempTable("salepayrol")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfOP = spark.sql("select distinct ' ' as report_date,a.storenumber,b.companycode as companycd,"
                + "case when e.AssociationType = 'Retail' then f.DealerCode when e.AssociationType = 'SMF' then f.DealerCode  else ' ' end as dealercode, "
                + "d.locationname as locationname,"
                + "d.market as springmarket,d.region as springregion,d.district as springdistrict,"
                + "a.SumofREG as regularhours,a.SumofOT as overtimehours,a.SumofDOT as doubleovertimehours,"
                + "a.SumofTotal as totalhours,a.SumofGreeter as storegreeterscount,a.SumofOperationalASM as operationalasmcount,"
                + "a.SumofSMD as smdcount,a.SumofPayroll as totalpayrollamount,a.LocationGP as totalgrossprofit,"
                + "a.GPPerHour as grossprofitperhour,a.PayrollofGP as payrollpercentofgrossproft,"
                + "a.SumofFTE as totalftecount,a.SumofFTEC as fulltimeequivalent, a.SumofPTEC as parttimeequivalent,"
                + "a.SumofDLSC as districtleadsalesconsultantcount,"
                + "a.SumofBAM1 as bam1count,a.SumofBam2 as bam2count,a.SumofStoreManager as storemanagercount,"
                + "a.SumofMIT as mitcount,"                                              
                + "'I' as cdcindicator from salepayrol a cross join customer1 b "
                + "inner join store d "
                + "on a.storenumber = d.storenumber "
                + " inner join storedealerass e "
                + "on a.storenumber = e.StoreNumber "
                + "inner join dealer f "
                + "on a.storenumber = f.TBLoc OR a.storenumber = f.SMFMapping ")             
          
                  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOP.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(SalePayrolOp)

dfOP.coalesce(1).select("*"). \
write.parquet(SalePayrolOp + '/' + todayyear + '/' + todaymonth + '/' + 'StorePayrollRefine' + FileTime);
                
spark.stop()


