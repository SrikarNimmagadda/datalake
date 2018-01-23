from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import col,when


class StoreDealerCodeAssociationRefine:

    def __init__(self):
        self.dealerCodeList = sys.argv[1]
        self.associationFilePath = sys.argv[2] + '/' + datetime.now().strftime('%Y/%m') + '/' + \
                                   'StoreDealerAssociationRefine' + '/' + sys.argv[3]

    def loadRefine(self):

        spark = SparkSession.builder.appName("StoreDealerCodeAssociationRefine").getOrCreate()

        dfDealerCode = spark.read.parquet(self.dealerCodeList)

        dfDealerCode = dfDealerCode.filter((col("TBLoc") != 0) & (col("SMFMapping") != 0))

        dfStoreAss = dfDealerCode.withColumnRenamed("Company", "CompanyCode").\
            withColumnRenamed("DF", "AssociationType").withColumnRenamed("DCstatus","AssociationStatus").\
            registerTempTable("Dealer")

        dfStoreAss = spark.sql("select a.DealerCode,a.CompanyCode,a.AssociationType,"
                               + "a.AssociationStatus,a.TBLoc,a.SMFMapping,a.RankDescription from Dealer a "
                               + "where a.RankDescription='Open Standard' or a.RankDescription='SMF Only' or "
                               + "a.RankDescription='Bulk' or a.RankDescription Like '%Close%' or "
                               + "a.RankDescription Like 'Close%'")

        dfStoreAss = dfStoreAss.where(col('CompanyCode').like("Spring%"))
        dfStoreAsstemp = dfStoreAss
        dfOpenFil = dfStoreAss.filter(dfStoreAss.RankDescription == 'SMF Only')

#########################################################################################################
# Transformation starts #
#########################################################################################################

        dfStoreAssOpen = dfStoreAsstemp.\
            withColumn('StoreNumber', when((dfStoreAsstemp.RankDescription != 'SMF Only'),dfStoreAsstemp.TBLoc).
                       otherwise(dfStoreAsstemp.SMFMapping)).withColumn('dealercode1', dfStoreAsstemp.DealerCode).\
            withColumn('AssociationType1', when((dfStoreAsstemp.RankDescription != 'SMF Only'), 'Retail').
                       otherwise('SMF')).\
            withColumn('AssociationStatus1', when((dfStoreAsstemp.RankDescription == 'SMF Only') |
                                                     (dfStoreAsstemp.RankDescription == 'Open Standard') |
                                                     (dfStoreAsstemp.RankDescription == 'Bulk'), 'Active').
                       otherwise('Closed')).drop(dfStoreAsstemp.DealerCode).\
            drop(dfStoreAsstemp.AssociationType).drop(dfStoreAsstemp.AssociationStatus).\
            select(col('StoreNumber'), col('dealercode1').alias('DealerCode'),
                   col('AssociationType1').alias('AssociationType'),col('AssociationStatus1').alias('AssociationStatus'),
                   col('TBLoc'), col('SMFMapping'), col('RankDescription'), col('CompanyCode'))

        dfStoreAssOpen.registerTempTable("storeAss1")

#########################################################################################################
# Code for new entry fields for Open Standard #
#########################################################################################################

        dfOpenFilNewField = dfOpenFil.withColumn('StoreNumber', dfOpenFil.SMFMapping). \
            withColumn('dealercode1', dfOpenFil.DealerCode). \
            withColumn('AssociationType1', when((dfOpenFil.RankDescription != 'SMF Only'), 'SMF')). \
            withColumn('AssociationStatus1', when((dfOpenFil.RankDescription != 'SMF Only'), 'Active')). \
            drop(dfOpenFil.DealerCode). \
            drop(dfOpenFil.AssociationType). \
            drop(dfOpenFil.AssociationStatus). \
            select(col('StoreNumber'), col('dealercode1').alias('DealerCode'), col('AssociationType1').
                   alias('AssociationType'), \
                   col('AssociationStatus1').alias('AssociationStatus'), \
                   col('TBLoc'), col('SMFMapping'), col('RankDescription'), col('CompanyCode'))

        dfOpenFilNewField.registerTempTable("storeAss2")

#########################################################################################################
# Code for union of two dataframes#
#########################################################################################################

        joined_DF = spark.sql("select cast(StoreNumber as integer),DealerCode, 4 as CompanyCode,AssociationType,AssociationStatus from storeAss1 " \
                              + "union " \
                                "select StoreNumber,DealerCode,4 as CompanyCode,AssociationType,AssociationStatus from storeAss2")

        joined_DF.coalesce(1).select("*").write.parquet(self.associationFilePath);

        spark.stop()

if __name__ == "__main__":
    StoreDealerCodeAssociationRefine().loadRefine()