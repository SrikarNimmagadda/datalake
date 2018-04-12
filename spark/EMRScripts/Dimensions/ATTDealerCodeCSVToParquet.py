from pyspark.sql import SparkSession
from pyspark.sql.functions import year, unix_timestamp, from_unixtime, substring
import sys


class ATTDealerCodeCSVToParquet(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.attDealerCodeOut = sys.argv[1]
        self.attDealerCodeIn = sys.argv[2]

    def loadParquet(self):

        dfAttDealerCode1 = self.sparkSession.read.format("com.databricks.spark.csv").option(
            "header", "true").option("treatEmptyValuesAsNulls", "true").load(self.attDealerCodeIn)

        dfAttDealerCode2 = dfAttDealerCode1.filter(~(dfAttDealerCode1.Company.like('SimplyM%%')))

        dfAttDealerCode2 = dfAttDealerCode2.withColumnRenamed("Dealer Code", "dealercode").\
            withColumnRenamed("Loc #", "loc").\
            withColumnRenamed("Location", "location").\
            withColumnRenamed("Retail IQ", "retailiq").\
            withColumnRenamed("District", "district").\
            withColumnRenamed("ATT Mkt Abbrev", "attmktabbrev").\
            withColumnRenamed("ATT Market Name", "attmarketname").\
            withColumnRenamed("Region", "region").\
            withColumnRenamed("Market", "market").\
            withColumnRenamed("Dispute Mkt", "disputemkt").\
            withColumnRenamed("DF", "df").\
            withColumnRenamed("C&C", "candc").\
            withColumnRenamed("WS", "ws").\
            withColumnRenamed("WS Expires", "wsexpires").\
            withColumnRenamed("Footprint Level", "footprintlevel").\
            withColumnRenamed("Business Expert", "businessexpert").\
            withColumnRenamed("DF Code", "dfcode").\
            withColumnRenamed("Old Code", "oldcode").\
            withColumnRenamed("Old Code 2", "oldcode2").\
            withColumnRenamed("ATT Location Name", "attlocationname").\
            withColumnRenamed("ATT Location ID", "attlocationid").\
            withColumnRenamed("ATT Region", "attregion").\
            withColumnRenamed("State", "state").\
            withColumnRenamed("Notes", "notes").\
            withColumnRenamed("Notes2", "notes2").\
            withColumnRenamed("Open Date", "opendate").\
            withColumnRenamed("Close Date", "closedate").\
            withColumnRenamed("DC Origin", "dcorigin").\
            withColumnRenamed("Store Origin", "storeorigin").\
            withColumnRenamed("Acquisition Origin", "acquisitionorigin").\
            withColumnRenamed("TB Loc", "tbloc").\
            withColumnRenamed("SMF Mapping", "smfmapping").\
            withColumnRenamed("SMF Market", "smfmarket").\
            withColumnRenamed("DC status", "dcstatus").\
            withColumnRenamed("Sorting Rank", "sortingrank").\
            withColumnRenamed("Rank Description", "rankdescription").\
            withColumnRenamed("Company", "company")

        dfAttDealerCode3 = dfAttDealerCode2.filter(~(dfAttDealerCode2.rankdescription.like('Pending%%')))

        dfAttDealerCode3.registerTempTable("AttDealerCode")

        FinalDF = self.sparkSession.sql("select dealercode,loc,location,retailiq,district,attmktabbrev,attmarketname,"
                                        "region,market,disputemkt,df,candc,ws,wsexpires,footprintlevel,businessexpert,"
                                        "dfcode,oldcode,oldcode2,attlocationname,attlocationid,attregion,state,notes,notes2,"
                                        "opendate,closedate,dcorigin,storeorigin,acquisitionorigin,tbloc,smfmapping,"
                                        "smfmarket,dcstatus,sortingrank,rankdescription,company from AttDealerCode")

        FinalDF.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
            withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).select("*").write.mode("append").\
            partitionBy('year', 'month').format('parquet').save(self.attDealerCodeOut)

        FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.attDealerCodeOut + '/' + 'Working')

        self.sparkSession.stop()


if __name__ == "__main__":
    ATTDealerCodeCSVToParquet().loadParquet()
