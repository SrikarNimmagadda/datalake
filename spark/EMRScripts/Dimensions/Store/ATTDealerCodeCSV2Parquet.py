from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.types import StructField, StructType, StringType, ByteType, DateType, IntegerType


spark = SparkSession.builder.\
        appName("AttDealerCode").getOrCreate()

AttDealerCodeIn = sys.argv[1]
AttDealerCodeOut = sys.argv[2]
year = datetime.now().strftime('%Y')
month = datetime.now().strftime('%m')

####################################################################
#                                 Reading the source data files    #
####################################################################

schema = StructType([StructField('Dealer Code', StringType(), False),
                    StructField('Loc #', StringType(), True),
                    StructField('Location', StringType(), False),
                    StructField('Retail IQ', StringType(), True),
                    StructField('District', StringType(), True),
                    StructField('ATT Mkt Abbrev', StringType(), True),
                    StructField('ATT Market Name', StringType(), True),
                    StructField('Region', StringType(), True),
                    StructField('Market', StringType(), True),
                    StructField('Dispute Mkt', StringType(), True),
                    StructField('DF', ByteType(), True),
                    StructField('C&C', StringType(), True),
                    StructField('WS', ByteType(), True),
                    StructField('WS Expires', DateType(), True),
                    StructField('Footprint Level', StringType(), True),
                    StructField('Business Expert', StringType(), True),
                    StructField('DF Code', StringType(), True),
                    StructField('Old', StringType(), True),
                    StructField('Old2', StringType(), True),
                    StructField('ATT Location Name', StringType(), True),
                    StructField('ATT Location ID', IntegerType(), True),
                    StructField('ATT Region', StringType(), True),
                    StructField('State', StringType(), True),
                    StructField('Notes', StringType(), False),
                    StructField('Notes2', StringType(), False),
                    StructField('Open Date', DateType(), True),
                    StructField('Close Date', DateType(), True),
                    StructField('DC Origin', StringType(), True),
                    StructField('Store Origin', StringType(), True),
                    StructField('Acquisition Origin', StringType(), True),
                    StructField('TB Loc', StringType(), True),
                    StructField('SMF Mapping', StringType(), True),
                    StructField('SMF Marke', StringType(), True),
                    StructField('DC status', StringType(), True),
                    StructField('Sorting Rank', IntegerType(), True),
                    StructField('Rank Description', StringType(), True),
                    StructField('Company', IntegerType(), False)])


dfAttDealerCode = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


dfAttDealerCode = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        load(AttDealerCodeIn)

dfAttDealerCode = dfAttDealerCode.withColumnRenamed("Dealer Code", "dealercode").\
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
                  withColumnRenamed("Old", "old").\
                  withColumnRenamed("Old 2", "old2").\
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


dfAttDealerCode = dfAttDealerCode.registerTempTable("AttDealerCode")


FinalDF = spark.sql("select dealercode,loc,location,retailiq,district,attmktabbrev,attmarketname,region,market,disputemkt,df,candc,ws,wsexpires,footprintlevel,businessexpert,dfcode,old,old2,attlocationname,attlocationid,attregion,state,notes,notes2,opendate,closedate,dcorigin,storeorigin,acquisitionorigin,tbloc,smfmapping,smfmarket,dcstatus,sortingrank,rankdescription,company,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from AttDealerCode")

FinalDF.printSchema()
FinalDF.show(n=2)

FinalDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(AttDealerCodeOut)

FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(AttDealerCodeOut + '/' + 'working')

spark.stop()