from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as sf

DealerCodeList = sys.argv[1]
StoreAssociationOutput = sys.argv[2]
AssociationFileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("StoreDealercodeAssociationRefine").getOrCreate()
        

#########################################################################################################
# Reading the source parquet files #
#########################################################################################################
dfDealerCode  = spark.read.parquet(DealerCodeList)

dfStoreAss = dfDealerCode.withColumnRenamed("Company", "CompanyCode").\
            withColumnRenamed("DF","AssociationType").\
            withColumnRenamed("DCstatus","AssociationStatus").\
            withColumnRenamed("TB Loc","TBLoc").\
            withColumnRenamed("SMF Mapping","SMFMapping").registerTempTable("Dealer")

#dfStoreAss = dfDealerCode
dfStoreAss = spark.sql("select a.DealerCode,a.CompanyCode,a.AssociationType,"
                    + "a.AssociationStatus,a.TBLoc,a.SMFMapping,a.RankDescription from Dealer a "
                    + "where a.RankDescription='Open Standard' or a.RankDescription='SMF Only' or "
                    + "a.RankDescription='Bulk' or a.RankDescription Like '%Closed%' or " 
                    + "a.RankDescription Like 'Closed%'")
                    
dfStoreAss = dfStoreAss.filter(dfStoreAss.CompanyCode == 'Spring')
dfStoreAsstemp = dfStoreAss                    

        
dfOpenFil = dfStoreAss.filter(dfStoreAss.RankDescription != 'SMF Only')

#########################################################################################################
# Transformation starts #
#########################################################################################################
                    
dfStoreAssOpen = dfStoreAsstemp.withColumn('StoreNumber',sf.when((dfStoreAsstemp.RankDescription != 'SMF Only'),dfStoreAsstemp.TBLoc).\
otherwise(dfStoreAsstemp.SMFMapping)).\
withColumn('dealercode1',dfStoreAsstemp.DealerCode).\
withColumn('AssociationType1',sf.when((dfStoreAsstemp.RankDescription != 'SMF Only'),'Retail').otherwise('SMF')).\
withColumn('AssociationStatus1',sf.when((dfStoreAsstemp.RankDescription == 'SMF Only') | \
(dfStoreAsstemp.RankDescription == 'Open Standard') | (dfStoreAsstemp.RankDescription == 'Bulk'),'Active').otherwise('Closed')).\
drop(dfStoreAsstemp.DealerCode).\
drop(dfStoreAsstemp.AssociationType).\
drop(dfStoreAsstemp.AssociationStatus).\
select(col('StoreNumber'),col('dealercode1').alias('DealerCode'),col('AssociationType1').alias('AssociationType'),\
col('AssociationStatus1').alias('AssociationStatus'),\
col('TBLoc'),col('SMFMapping'),col('RankDescription'),col('CompanyCode'))

dfStoreAssOpen.registerTempTable("storeAss1")

#########################################################################################################
# Code for new entry fields for Open Standard #
#########################################################################################################

dfOpenFilNewField = dfOpenFil.withColumn('StoreNumber',dfOpenFil.SMFMapping).\
withColumn('dealercode1',dfOpenFil.DealerCode).\
withColumn('AssociationType1',sf.when((dfOpenFil.RankDescription != 'SMF Only'),'SMF')).\
withColumn('AssociationStatus1',sf.when((dfOpenFil.RankDescription != 'SMF Only'),'Active')).\
drop(dfOpenFil.DealerCode).\
drop(dfOpenFil.AssociationType).\
drop(dfOpenFil.AssociationStatus).\
select(col('StoreNumber'),col('dealercode1').alias('DealerCode'),col('AssociationType1').alias('AssociationType'),\
col('AssociationStatus1').alias('AssociationStatus'),\
col('TBLoc'),col('SMFMapping'),col('RankDescription'),col('CompanyCode'))

dfOpenFilNewField.registerTempTable("storeAss2")

#########################################################################################################
# Code for union of two dataframes#
#########################################################################################################

joined_DF =  spark.sql("select StoreNumber,CompanyCode,DealerCode,AssociationType,AssociationStatus from storeAss1 "\
                        + "union "\
                        "select StoreNumber,CompanyCode,DealerCode,AssociationType,AssociationStatus from storeAss2")   

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

#joined_DF.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(StoreAssociationOutput)

joined_DF.coalesce(1).select("*"). \
write.parquet(StoreAssociationOutput + '/' + todayyear + '/' + todaymonth + '/' + 'StoreDealerAssociationRefine' + AssociationFileTime);
                   
spark.stop()



