from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from pyspark.sql.types import StringType
from pyspark import SQLContext
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import date_sub

EmpployeeRefineInp = sys.argv[1]
StoreRefineInp = sys.argv[2]
StoreDealerAssInp = sys.argv[3]
ATTDealerCodeInp = sys.argv[4]
SalesLeadInp = sys.argv[5]
CompanyInp = sys.argv[6]		
SalesLeadOP = sys.argv[7]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("SalesLeadRefine").getOrCreate()
		   
dfEmpployeeRefine  = spark.read.parquet(EmpployeeRefineInp)
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)
dfSalesLead  = spark.read.parquet(SalesLeadInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
				   
				   
dfEmpployeeRefine.registerTempTable("emprefine")
dfStoreRefine.registerTempTable("storerefine")
dfStoreDealerAss.registerTempTable("storedealerass")
dfATTDealer.registerTempTable("attdealer")
dfSalesLead.registerTempTable("salesleadtable")
dfCustomer.registerTempTable("company")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    

dfCustomer = spark.sql("select a.companycode from company a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer")

Temp_DF = spark.sql("select date_sub(current_timestamp(), 1) as report_date,a.storeid as storenumber,"
+ "case when b.AssociationType = 'Retail' then d.DealerCode end as dealercode,"
+ "f.companycode as companycd,"
+ "e.sourceemployeeid as baeemployeeid,a.bae as bae_employee_name, a.account as customeraccount, a.dos as attdirectorofsales,"
+ "a.arsm as attregionsalesmanager, a.existingcustomer as currencustomerindicator,a.fan as salesrepresentativename,"
+ "a.repname as salesrepemployeeid, a.grossadds as grossactivations, a.sbassistancerequested as sbassistancerequest,"
+ "a.enterdate as entereddate, a.closedate as closeddate,a.lastupdate as lastupdatedate,a.status as leadstatus,"
+ "a.region as attregion, a.market as attmarket, a.storename as attlocationname, a.storeid as attlocationid,a.storename as storename,"
+ "a.springmarket as springmarket,a.springregion as springregion,springdistrict as springdistrict,a.description as description,"
+ "a.followupnotes as notes "
+ "from salesleadtable a inner join storedealerass b "
+ "on a.dealercode = b.DealerCode "
+ "inner join storerefine c "
+ "on b.StoreNumber = c.storenumber "
+ "inner join attdealer d "
+ "on c.StoreNumber = d.TBLoc "
+ "inner join emprefine e "
+ "on a.bae = e.name "
+ "cross join customer f")

FinalJoin_DF = Temp_DF.filter(Temp_DF.dealercode != 'null')
					  
FinalJoin_DF.coalesce(1).select("*").write.parquet(SalesLeadOP);

#FinalJoin_DF.coalesce(1).select("*"). \
#       write.format("com.databricks.spark.csv").\
#       option("header", "true").mode("overwrite").save(SalesLeadOP);
                
spark.stop()