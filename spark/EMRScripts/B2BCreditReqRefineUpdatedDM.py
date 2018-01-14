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

B2BCreditReqRefineInp = sys.argv[1]
CompanyInp = sys.argv[2]
StoreRefineInp = sys.argv[3]
EmpStoreAssRefineInp = sys.argv[4]
EmployeeRefInp = sys.argv[5]	
B2BCreditReqRefineOP = sys.argv[6]
FileTime = sys.argv[7]

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.\
        appName("B2BCreditReqRefine").getOrCreate()
		   
dfB2BCreditReqRefine  = spark.read.parquet(B2BCreditReqRefineInp)  
dfCompany = spark.read.format("com.databricks.spark.csv").\
                   option("header", "true").\
                   option("treatEmptyValuesAsNulls", "true").\
                   option("inferSchema", "true").\
                   load(CompanyInp)
dfStore = spark.read.parquet(StoreRefineInp)
dfEmpStoreAss = spark.read.parquet(EmpStoreAssRefineInp)
dfEmployeeRefine  = spark.read.parquet(EmployeeRefInp)

				   			   
dfB2BCreditReqRefine.registerTempTable("b2bcreditreqrefine")
dfCompany.registerTempTable("company")
dfStore.registerTempTable("storerefine")
dfEmpStoreAss.registerTempTable("empstoreassrefine")
dfEmployeeRefine.registerTempTable("employee")


####################################################################################################################
#                                           Spark Transformaions                                             #
####################################################################################################################

#todaydate  =  datetime.now().strftime('%Y%m%d')    

dfCustomer = spark.sql("select a.companycode from company a where a.companytype = 'Spring Mobile - AT&T'")
dfCustomer.registerTempTable("customer")

dfRepStoreNumber = spark.sql("select a.repstorenumber from b2bcreditreqrefine a where a.crdapptoloc like 'BAM%'")
dfRepStoreNumber.registerTempTable("repstorenumber")
dfRepStoreNumber.show()

FinalJoin_DF = spark.sql("select date_sub(current_timestamp(), 1) as report_date, f.companycode as companycd,"
+ "'' as reqdatetime, a.invoicenumber as invoicenumber, a.dateoftransaction as dateoftransaction, a.businessname as clientname,"
+ "a.ban as ban, a.rqinvoice as invoicelink, a.refferingrep as salesreprefby, d.sourceemployeeid as salesrepresentativeid, c.storenumber as storerefby,"
+ "b.repstorenumber as repstorenumber, e.sourceemployeeid as sourceemployeeid, a.crdgoesperson as employeename, a.crdgoesstore as storecrdapproved,"
+ "a.b2bmanagername as b2bmanagernmcrdapproved, a.repomanager as b2bmanagerempid, a.approval as approvalstatus, a.completed as crdreqstatus,"
+ "a.crdapptoemp as jobtitlerefby, a.crdapptoloc as storenumberrefby, a.fullgpofinvoice as fullgpinvamount, '' as gpcredit, '' as gpbaedebit,"
+ "a.crugatocrd as noofcrugrossactivation, a.tabletstocrd as nooftablets, a.hsttocrd as noofhsis, a.tvtocrd as nooftv, a.oppstocrd as noofopportunity,"
+ "a.protectedreven as protectedreven, a.protectioneligopps as protectioneligopps, a.accessoryunits as accessoryunits, a.amaunits as amaunits,"
+ "a.acceligopps as acceligopps, a.notes as notes, 'RQ4' as sourcesystemname "
+ "from b2bcreditreqrefine a "
+ "inner join repstorenumber b "
+ "on a.repstorenumber = b.repstorenumber "
+ "inner join storerefine c "
+ "on b.repstorenumber = c.storenumber "
+ "inner join empstoreassrefine d "
+ "on b.repstorenumber = d.storenumber "
+ "inner join employee e "
+ "on a.emailaddress = e.workemail "
+ "cross join customer f")
			  
FinalJoin_DF.coalesce(1).select("*"). \
       write.format("com.databricks.spark.csv").\
       option("header", "true").mode("overwrite").save(B2BCreditReqRefineOP);


todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

FinalJoin_DF.coalesce(1).select("*"). \
write.parquet(B2BCreditReqRefineOP + '/' + todayyear + '/' + todaymonth + '/' + 'B2BCreditRequest' + FileTime);
              
spark.stop()