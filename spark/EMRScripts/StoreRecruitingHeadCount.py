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
AppFTEbyLocInp = sys.argv[5]
StoreHeadCountOp = sys.argv[6]
FileTime = sys.argv[7]

spark = SparkSession.builder.\
        appName("StoreRecruitingHeadCount").getOrCreate()
        
#########################################################################################################
#                                 Read the 2 source files                                              #
#########################################################################################################
        
dfStoreRefine  = spark.read.parquet(StoreRefineInp)
dfStoreDealerAss  = spark.read.parquet(StoreDealerAssInp)
dfATTDealer  = spark.read.parquet(ATTDealerCodeInp)
dfAppFTEbyLoc  = spark.read.parquet(AppFTEbyLocInp)
dfCustomer = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
                   
dfAppFTEbyLoc = dfAppFTEbyLoc.withColumnRenamed("Market", "springmarket").\
            withColumnRenamed("Region", "springregion").\
            withColumnRenamed("District", "springdistrict").\
            withColumnRenamed("PlanFTE", "plannedftecount").\
            withColumnRenamed("RDProposed+/-", "rdproposedchangecount").\
            withColumnRenamed("ApprovedFTE", "approvedftecount").\
            withColumnRenamed("ApprovedHeadcount", "approvedheadcount").\
            withColumnRenamed("StoreManager", "storemanagercount").\
            withColumnRenamed("BAM1", "businessassistantmanager1").\
            withColumnRenamed("BAM2", "businessassistantmanager2").\
            withColumnRenamed("FTEC", "fulltimeequivalent").\
            withColumnRenamed("PTEC", "parttimeequivalent").\
            withColumnRenamed("FTFloater", "fulltimeflotercount").\
            withColumnRenamed("DLSCDistrictLeadSalesConsultant", "dlscdistrictleadsalesconsultantcount").\
            withColumnRenamed("MIT", "mitcount").\
            withColumnRenamed("RDApprovedHCStore&MIT", "rdadjustedheadcount").\
            withColumnRenamed("ActualHC", "actualheadcount").\
            withColumnRenamed("Underage", "underage").\
            withColumnRenamed("Overage", "overage")           
            
dfStoreRefine.registerTempTable("storerefine")
dfStoreDealerAss.registerTempTable("storedealerass")
dfATTDealer.registerTempTable("dealer")
dfAppFTEbyLoc.registerTempTable("appfteloc")
dfCustomer.registerTempTable("customer")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

dfCustomer = spark.sql("select a.companycode from customer a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer1")

dfOP = spark.sql("select distinct a.storenumber as storenumber,e.companycode as companycd,' ' as report_date,"
                + "case when b.AssociationType = 'Retail' then c.DealerCode when b.AssociationType = 'SMF' then c.DealerCode  else ' ' end as dealercode, "
                + "d.springmarket,d.springregion,d.springdistrict,d.plannedftecount,d.rdproposedchangecount,d.approvedftecount,"
                + "d.approvedheadcount,d.storemanagercount,d.businessassistantmanager1,d.businessassistantmanager2,d.fulltimeequivalent,"
                + "d.parttimeequivalent,d.fulltimeflotercount,d.dlscdistrictleadsalesconsultantcount,d.mitcount,d.rdadjustedheadcount,"
                + "d.actualheadcount,d.underage,d.overage,'I' as cdcindicator from storerefine a "
                + "inner join storedealerass b "
		+ "on a.storenumber = b.StoreNumber "
                + "inner join dealer c "
                + "on a.storenumber = c.TBLoc OR a.storenumber = c.SMFMapping "
                + "inner join appfteloc d "
                + "on a.Market = d.springmarket "
                + "cross join customer1 e")
              

           
                  
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfOP.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(StoreHeadCountOp)

dfOP.coalesce(1).select("*"). \
write.parquet(StoreHeadCountOp + '/' + todayyear + '/' + todaymonth + '/' + 'StoreHeadCountRefine' + FileTime);
                
spark.stop()


