#This module for EmployeeCNCTraining Refine#############

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


CompanyInp = sys.argv[1]
CNCTrainingInp = sys.argv[2]
StoreRefineInp = sys.argv[3] 
StoreDealerAssInp = sys.argv[4]
ATTDealerCodeInp = sys.argv[5]
EmployeeRefineInp = sys.argv[6]
CNCTrainingOP = sys.argv[7] 
FileTime = sys.argv[8]

spark = SparkSession.builder.\
        appName("EmployeeCNCTrainingReport").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfCNCTrainingInp  = spark.read.parquet(CNCTrainingInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)
dfEmployeeRefine  = spark.read.parquet(EmployeeRefineInp)

dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   

#dfCNCTrainingInp.registerTempTable("cnctraining")
dfStoreRefine.registerTempTable("store")
dfATTDealer.registerTempTable("dealer")
dfStoreDealerAss.registerTempTable("storedealerass")
dfCustomer.registerTempTable("customer")  
dfEmployeeRefine.registerTempTable("employee") 

split_col = split(dfCNCTrainingInp['RQ4Location'], ' ')
dfCNCTrainingInp = dfCNCTrainingInp.withColumn('storenumber', split_col.getItem(0))
dfCNCTrainingInp = dfCNCTrainingInp.withColumn('locationname', split_col.getItem(1))
dfCNCTrainingInp = dfCNCTrainingInp.withColumnRenamed("RQ4/SpringUName", "RQ4SpringUName").\
                                    withColumnRenamed("Pass/Fail", "PassFail")

dfCNCTrainingInp.registerTempTable("cnctraining")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfOutput = spark.sql("select b.companycode as companycd,'' as reportdate,a.storenumber,a.locationname,c.sourceemployeeid, "
            + "a.RQ4SpringUName as employeename,"
            + "e.DealerCode as dealercode,"          
            + "a.Position as jobtitle,a.PassFail as passfailindicator,a.Complance as compliancecompletion,"
            + "a.MIT as mitcompletion,a.Ongoing as ongoingtrainingcompletion,a.Incomplete as incompletetrainingcount,"
            + "a.XMID as xmid," 
            + "d.Market as springmarket,d.Region as springregion,"
            + "d.District as springdistrict,"
            + "case when e.AssociationType = 'Retail' then e.StoreNumber end as attlocationid, " 
            + "case when e.AssociationType = 'Retail' then d.locationname end as attlocationname, "         
            + "'I' as cdcindicator, 'RQ4' as sourcesystemname "
            + " from cnctraining a cross join customer1 b "
            + "inner join employee c "
            + "on a.RQ4SpringUName = c.name "
            + "inner join store d "
            + "on a.storenumber = d.storenumber "
            + " inner join storedealerass e "
            + "on a.storenumber = e.StoreNumber "
            + "inner join dealer f "
            + "on a.storenumber = f.TBLoc OR a.storenumber = f.SMFMapping ")
                       
 #+ "case when b.AssociationType = 'Retail' then b.StoreNumber end as attlocationid, "
 #            + "case when b.AssociationType = 'Retail' then e.StoreName end as attlocationname, "               
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfOutput.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(CNCTrainingOP)

dfOutput.coalesce(1).select("*"). \
write.parquet(CNCTrainingOP + '/' + todayyear + '/' + todaymonth + '/' + 'EmployeeCNCTraining' + FileTime);
                
spark.stop()

                   