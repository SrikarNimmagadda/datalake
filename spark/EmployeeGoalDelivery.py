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


# Passing argument for reading the file

EmpGoalRefineInp = sys.argv[1] #attscoredgoal
EmpGoalDeliveryOP = sys.argv[2] 
FileTime = sys.argv[3]

# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("employeeRefine").getOrCreate()
        
dfEmpGoal  = spark.read.parquet(EmpGoalRefineInp)

dfEmpGoal.registerTempTable("empgoal")

#########################################################################################################
#                                 Spark Transformation begins here                                      #
######################################################################################################### 

                                 
dfEmpGoal=spark.sql("select distinct a.companycd as CO_CD,a.report_date as RPT_DT,a.sourceemployeeid as SRC_EMP_ID,a.kpiname as KPI_NM,"
            + "a.goalvalue as GOAL_VAL, 'RQ4' as SRC_SYS_NM "
            + "from empgoal a")
            #or a.StoreName is NULL and b.PrimaryLocation is NULL")
                
                
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

dfEmpGoal.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpGoalDeliveryOP)

                
spark.stop()                  

          

                   
        
'''        
def mapper(line):
    fields = line.split(',')
    Store=str(fields[0])
    District=str(fields[1])
    GrossProfit=str(fields[2])
    AdvTV=str(fields[3])
    DigitalLife=str(fields[4])
    AccGPOpp=str(fields[5])
    CRUGA=str(fields[6])
    Tablets=str(fields[7])
    IntegratedProducts=str(fields[8])
    WTR=str(fields[9])
    GoPhone=str(fields[10])
    OTHours=str(fields[11])
    CloseRate=str(fields[12])
    AccessoryGP=str(fields[13])
    AccessoryAttachRate=str(fields[14])
    Traffic=str(fields[15])
    PPGAVAB=str(fields[16])
    AdvTVVAB=str(fields[17])
    DLifeVAB=str(fields[18])
    CRUVAB=str(fields[19])
    HSI=str(fields[20])
    ApprovedFTE=str(fields[21])
    ApprovedHCfloaters=str(fields[22])
    ApprovedHC=str(fields[23])

    
    #return Row()
    return Row(Store,District,GrossProfit,AdvTV,DigitalLife,AccGPOpp,CRUGA,Tablets,IntegratedProducts,WTR,GoPhone,OTHours,CloseRate,AccessoryGP,AccessoryAttachRate,Traffic,PPGAVAB,AdvTVVAB,DLifeVAB,CRUVAB,HSI,ApprovedFTE,ApprovedHCfloaters,ApprovedHC)

        
data = spark.sparkContext.textFile(ATTScoredGoalInp)
header = data.first() #extract header
data = data.filter(lambda x: x != header)
dataschema = data.map(mapper)

'''
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
   
df = spark.createDataFrame(dataschema)
# df = spark.createDataFrame(data.map (lambda x: Row(x)), schema)

#df.show()

df.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(EmpGoalOP)

'''
#for x in data.collect():
#    print(x)