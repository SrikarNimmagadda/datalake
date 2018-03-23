from pyspark.sql import SparkSession
import sys

DealerCodeIn = sys.argv[2]
DealerCodeOutput = sys.argv[1]

spark = SparkSession.builder.appName("DealerCodeRefine").getOrCreate()

#########################################################################################################
# Reading the source parquet files #
#########################################################################################################

dfDealerCode = spark.read.parquet(DealerCodeIn).registerTempTable("AttDealerCode")

#############################################################################################
#                                 Reading the source data files                               #
###############################################################################################

dfDealerCode = spark.sql("select a.dealercode,'4' as companycode,a.dcorigin as dealercodeorigin,"
                         "a.dfcode,a.dcstatus,case when a.df = 'No' then '0' when a.df = 'Yes' then '1'"
                         " when a.df = 'Off' then '0' when a.df = 'False' then '0' when a.df = 'On' then '1' "
                         "when a.df = 'True' then '1' when a.df = 'DF' then '1' end as dfindicator,a.candc,a.opendate,a.closedate,"
                         "case when a.ws = 'No' then '0' when a.ws = 'Yes' then '1'"
                         " when a.ws = 'Off' then '0' when a.ws = 'False' then '0' when a.ws = 'On' then '1' when"
                         " a.ws = 'True' then '1' end as whitestoreindicator,a.wsexpires as whitestoreexpirationdate,"
                         "a.sortingrank as sortrank,a.rankdescription,a.storeorigin,"
                         "a.acquisitionorigin as origin,a.businessexpert,a.footprintlevel,"
                         "a.location as attlocationextendedname,a.attlocationid,a.attlocationname,a.disputemkt as attdisputemarket,"
                         "a.attmktabbrev as attmarketcode, a.attmarketname as attmarket,"
                         "a.oldcode as olddealercode, "
                         "a.oldcode2 as olddealercode2,"
                         "a.attregion, a.notes, a.notes2,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from AttDealerCode a")
# to change company code to -1 ##

FinalDF1 = dfDealerCode.na.fill({'companycode': -1, })

# to filter null value recolds for column delaercodes ##

FinalDF2 = FinalDF1.where(FinalDF1.dealercode != '')

# dropping duplicates ##
FinalDF = FinalDF2.dropDuplicates(['dealercode'])

#########################################################################################################
# write output in parquet file #
#########################################################################################################

FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(DealerCodeOutput + '/' + 'Working')

FinalDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(DealerCodeOutput)

spark.stop()
