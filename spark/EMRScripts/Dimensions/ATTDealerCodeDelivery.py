from pyspark.sql import SparkSession
import sys

DealerCodeOutput = sys.argv[1]
DealerCodeIn = sys.argv[2]


spark = SparkSession.builder.appName("DealerCodeDelivery").getOrCreate()

dfDealerCode = spark.read.parquet(DealerCodeIn)

#############################################################################################
#                                 Reading the source data files                              #
############################################################################################

dfDealerCode.registerTempTable("Dealer")

dfDealerCode = spark.sql("select a.dealercode as DLR_CD,a.companycode as CO_CD,a.dealercodeorigin as DL_CD_ORIG, "
                         "a.dfcode as DF_CD,a.dcstatus as DC_STAT, a.dfindicator as DF_IND,a.candc as C_AND_C,"
                         "a.opendate as OPEN_DT, a.closedate as CLOSE_DT, a.whitestoreindicator as WS_IND,"
                         "a.whitestoreexpirationdate as WS_EXP_DT,a.sortrank as SRT_RNK,a.rankdescription as RNK_DESC,"
                         "a.storeorigin as STORE_ORIG, a.origin as ORIG, a.businessexpert as BUS_EXPRT,"
                         "a.footprintlevel as FTPRT_LVL,a.attlocationextendedname as ATT_LOC_EXT_NM,"
                         "a.attlocationid as ATT_LOC_ID,a.attlocationname as ATT_LOC_NM,"
                         "a.attdisputemarket as ATT_DSPT_MKT, 'I' as CDC_IND_CD  from Dealer a")

dfDealerCode.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DealerCodeOutput + '/' + 'Current')

spark.stop()
