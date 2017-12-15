from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import size


#import sqlContext.implicits._

DealerCodeList = sys.argv[1]
DealerCodeOutput = sys.argv[2]
DealerCodeFileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        

#########################################################################################################
# Reading the source parquet files #
#########################################################################################################
                    
dfDealerCode  = spark.read.parquet(DealerCodeList)
dfDealerCode = dfDealerCode.withColumnRenamed("Loc#", "ATTLocCode").\
            withColumnRenamed("C&C","CC").\
            withColumnRenamed("WS","WhiteStore").\
            withColumnRenamed("WSExpires","WhiteStoreExpireDate").\
            withColumnRenamed("DCOrigin","DealerCodeOrigin").\
            withColumnRenamed("Old","OldDealerCode").\
            withColumnRenamed("Old2","OldDealerCode2").\
            withColumnRenamed("Company","CompanyCode").\
            withColumnRenamed("SMF Mapping", "SMFMapping").registerTempTable("Dealer") 
            
#############################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################  
            
dfDealerTemp = spark.sql("select a.DealerCode,a.CompanyCode,a.DealerCodeOrigin,a.OldDealerCode,"
                        + "a.OldDealerCode2,a.DFCode,a.DCstatus,a.ATTLocationID,"
                        + "a.ATTLocCode,a.ATTLocationName,a.DisputeMkt,a.ATTMarketName,"
                        + "a.ATTMktAbbrev,a.ATTRegion,a.DF,a.CC,a.OpenDate,a.CloseDate,"
                        + "case when a.WhiteStore = 'Yes' then '1' when a.WhiteStore = 'Off' then '0' else ' ' end as WhiteStoreIndicator,a.WhiteStoreExpireDate,a.SortingRank,"
                        + "a.RankDescription,a.StoreOrigin,a.AcquisitionOrigin,"
                        + "a.BusinessExpert,a.FootprintLevel,a.Notes,a.Notes2,a.SMFMapping from Dealer a "
                        + "where length(a.DealerCode) <= 20")
                        
                        
dfDealerTemp.registerTempTable("Dealer1")   

dfDealerTemp1= spark.sql("select a.* from Dealer1 a INNER JOIN (select DealerCode, max(SortingRank) as value from Dealer1 group by DealerCode) as b on a.DealerCode=b.DealerCode and a.SortingRank=b.value");                      
#########################################################################################################
############# Transformation Starts #
#########################################################################################################
#dfDealerTemp1 = spark.sql("SELECT a.* "
#+ "from (select DealerCode, SortingRank from Dealer1 GROUP BY DealerCode) a " 
#+ "inner join (select DealerCode, SortingRank from Dealer1 GROUP BY DealerCode) b "
#+ "ON a.DealerCode = b.DealerCode AND a.SortingRank > b.sum_score ") 
#newdf = dfDealerTemp.join(dfDealerTemp.groupBy('DealerCode').count(),on='DealerCode')
#newdf1 = newdf.filter((newdf['count'] > 1) & (newdf.DealerCode == newdf.DealerCode) & (newdf.SortingRank > newdf.SortingRank))
dfStoreAss = dfDealerTemp1.filter(dfDealerTemp1.CompanyCode != 'SimplyMac')
dfDealerFil = dfStoreAss.filter(dfStoreAss.RankDescription != 'Pending Standard')
dfDealerFil1 = dfDealerFil.filter((dfDealerFil.DealerCode != '') & (dfDealerFil.DCstatus != 'Closed'))

#dfDealerFil1.withColumn('DealerCode1',sf.when((dfStoreAsstemp.DealerCode.count > 1),)


#########################################################################################################
############# write output in parquet file #
#########################################################################################################
     
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')

#dfDealerFil1.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(DealerCodeOutput)

dfDealerFil1.coalesce(1).select("*"). \
write.parquet(DealerCodeOutput + '/' + todayyear + '/' + todaymonth + '/' + 'ATTDealerCodeRefine' + DealerCodeFileTime);

spark.stop()