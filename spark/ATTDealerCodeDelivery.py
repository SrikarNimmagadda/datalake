from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql.types import *

DealerCodeList = sys.argv[1]
DealerCodeOutput = sys.argv[2]

spark = SparkSession.builder.\
        appName("DealerCodeDelivery").getOrCreate()
        
dfDealerCode  = spark.read.parquet(DealerCodeList)



#############################################################################################
#                                 Reading the source data files                                         #
######################################################################################################### 

'''
dfDealerCode = dfDealerCode.withColumnRenamed("Loc#", "ATTLocCode").\
            withColumnRenamed("C&C","CC").\
            withColumnRenamed("WS","WhiteStore").\
            withColumnRenamed("WSExpires","WhiteStoreExpireDate").\
            withColumnRenamed("DCOrigin","DealerCodeOrigin").\
            withColumnRenamed("Old","OldDealerCode").\
            withColumnRenamed("Old2","OldDealerCode2").\
            withColumnRenamed("Company","CompanyCode").\
            withColumnRenamed("SMF Mapping", "SMFMapping").registerTempTable("Dealer") 
'''            
dfDealerCode.registerTempTable("Dealer")            

dfDealerTemp = spark.sql("select a.DealerCode,a.DealerCodeOrigin ,a.DFCode ,"
                        + "a.DCstatus ,a.ATTLocationID ,a.ATTLocationName ,"
                        + "a.DisputeMkt,a.DF,"
                        + "a.CC,a.OpenDate,a.CloseDate,a.WhiteStoreIndicator,a.WhiteStoreExpireDate,"
                        + "a.SortingRank,a.RankDescription,a.StoreOrigin,"
                        + "a.AcquisitionOrigin,a.Notes,a.Notes2,a.SMFMapping,"
                        + "a.BusinessExpert,a.FootprintLevel,'I' as CDC_IND_CD from Dealer a "
                        + "where length(a.DealerCode) <= 20")
 
#########################################################################################################
############# Transformation Starts #
#########################################################################################################

#dfDealerFil = dfDealerTemp.filter(dfDealerTemp.RankDescription != 'Pending Standard')
dfDealerFil1 = dfDealerTemp.filter((dfDealerTemp.DealerCode != '') & (dfDealerTemp.DCstatus != 'Closed') & (dfDealerTemp.SMFMapping == 0))                       

dfDealerFil1.registerTempTable("Dealer1")  
#########################################################################################################
############# write output in CSV file #
######################################################################################################### 

dfDealerop = spark.sql("select distinct a.DealerCode as DLR_CD,'4' as CO_CD,a.DealerCodeOrigin as DL_CD_ORIG,a.DFCode as DF_CD,"
                        + "a.DCstatus as DC_STAT,a.ATTLocationID as ATT_LOC_ID,a.ATTLocationName as ATT_LOC_NM,"
                        + "a.DisputeMkt as ATT_DSPT_MKT,a.DF as DF_IND,"
                        + "a.CC as C_AND_C,a.OpenDate as OPEN_DT,a.CloseDate as CLOSE_DT,a.WhiteStoreIndicator as WS_IND,a.WhiteStoreExpireDate as WS_EXP_DT,"
                        + "a.SortingRank as SRT_RNK,a.RankDescription as RNK_DESC,a.StoreOrigin as STORE_ORIG,"
                        + "a.AcquisitionOrigin as ORIG,a.Notes as NT,a.Notes2 as NT_2,a.SMFMapping as ATT_LOC_EXT_NM,"
                        + "a.BusinessExpert as BUS_EXPRT,a.FootprintLevel as FTPRT_LVL,'I' as CDC_IND_CD from Dealer a")


dfDealerop.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DealerCodeOutput);

spark.stop()