from pyspark.sql import SparkSession
import sys
import csv
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit, regexp_replace, rtrim

spark = SparkSession.builder.appName("ATTSalesActuals").getOrCreate()

AttSalesActualsOutput = sys.argv[1]
AttSalesActualsInput = sys.argv[2]
ATTMyResultsInp2 = sys.argv[3]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

schema = StructType([StructField('Location', StringType(), False), StructField('Loc Id', StringType(), False), StructField('Dlr1 Code', StringType(), False), StructField('KPI', StringType(), False), StructField('KPI ID', StringType(), False), StructField('Dec 2017 Actual', StringType(), True), StructField('Dec 2017 Projected', StringType(), True), StructField('Dec 2017 Target', StringType(), True)])

dfATTSalesActuals = spark.sparkContext.textFile(AttSalesActualsInput).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: ''.join(line).strip() != '' and line[5] != 'Location' and len(line[0]) != 0).toDF(schema)

dfATTSalesActuals1 = dfATTSalesActuals.filter(~(dfATTSalesActuals.Location.like('Histori%%')))
dfATTSalesActuals2 = dfATTSalesActuals1.filter(~(dfATTSalesActuals.Location.like('AT&T MyRe%%')))
dfATTSalesActuals3 = dfATTSalesActuals2.filter(~(dfATTSalesActuals.Location.like('AR : SPRI%%')))
dfATTSalesActuals4 = dfATTSalesActuals3.filter(~(dfATTSalesActuals.Location.like('Generated%%')))
dfAttSalesActualsInput = dfATTSalesActuals4.filter(~(dfATTSalesActuals.Location.like('Locat%%')))

FinalHistDF1 = dfAttSalesActualsInput.withColumnRenamed("Location", "attlocationname").\
                                                withColumnRenamed("Loc Id", "locid").\
                                                withColumnRenamed("Dlr1 Code", "dealercode").\
                                                withColumnRenamed("KPI", "kpiname").\
                                                withColumnRenamed("KPI ID", "kpiid").\
                                                withColumnRenamed("Dec 2017 Actual", "actual_value").\
                                                withColumnRenamed("Dec 2017 Projected", "projected_value").\
                                                withColumnRenamed("Dec 2017 Target", "targetvalue")

FinalHistDF1 = FinalHistDF1.withColumn("actualvalue", rtrim(regexp_replace("actual_value", '\\$|\\%', '')))
FinalHistDF1 = FinalHistDF1.withColumn("projectedvalue", rtrim(regexp_replace("projected_value", '\\$|\\%', '')))

dfATTSalesActualsRPT = spark.sparkContext.textFile(ATTMyResultsInp2).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: ''.join(line).strip() != '' and line[5] != 'Location' and len(line[0]) != 0).toDF(schema)


dfATTSalesActualsRPT1 = dfATTSalesActualsRPT.filter(~(dfATTSalesActualsRPT.Location.like('Histori%%')))
dfATTSalesActualsRPT2 = dfATTSalesActualsRPT1.filter(~(dfATTSalesActualsRPT.Location.like('AT&T MyRe%%')))
dfATTSalesActualsRPT3 = dfATTSalesActualsRPT2.filter(~(dfATTSalesActualsRPT.Location.like('AR : SPRI%%')))
dfATTSalesActualsRPT4 = dfATTSalesActualsRPT3.filter(~(dfATTSalesActualsRPT.Location.like('Generated%%')))
dfAttSalesActualsInputRPT = dfATTSalesActualsRPT4.filter(~(dfATTSalesActualsRPT.Location.like('Locat%%')))

FinalRPTDF1 = dfAttSalesActualsInputRPT.withColumnRenamed("Location", "attlocationname2").\
                                                withColumnRenamed("Loc Id", "locid2").\
                                                withColumnRenamed("Dlr1 Code", "dealercode2").\
                                                withColumnRenamed("KPI", "kpiname2").\
                                                withColumnRenamed("KPI ID", "kpiid2").\
                                                withColumnRenamed("Dec 2017 Actual", "actual_value2").\
                                                withColumnRenamed("Dec 2017 Projected", "projected_value2").\
                                                withColumnRenamed("Dec 2017 Target", "targetvalue2")

FinalRPTDF1 = FinalRPTDF1.withColumn("actualvalue2", rtrim(regexp_replace("actual_value2", '\\$|\\%', '')))
FinalRPTDF1 = FinalRPTDF1.withColumn("projectedvalue2", rtrim(regexp_replace("projected_value2", '\\$|\\%', '')))

today = datetime.now().strftime('%m/%d/%Y')
FinalHistDF1 = FinalHistDF1.withColumn('reportdate', lit(today))
FinalHistDF1.registerTempTable("HIST")
FinalRPTDF1 = FinalRPTDF1.withColumn('reportdate2', lit(today))
FinalRPTDF1.registerTempTable("RPT")

FinalHistDF = spark.sql("select attlocationname,locid,dealercode,kpiname,kpiid,actualvalue,projectedvalue,"
                        "targetvalue,reportdate,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                        "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from HIST")


FinalRPTDF = spark.sql("select attlocationname2,locid2,dealercode2,kpiname2,kpiid2,"
                       "actualvalue2,projectedvalue2,targetvalue2,reportdate2,"
                       "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from RPT")

FinalHistDF.coalesce(1).select("*").write.mode("overwrite").parquet(AttSalesActualsOutput + '/' + 'Working1')

FinalRPTDF.coalesce(1).select("*").write.mode("overwrite").parquet(AttSalesActualsOutput + '/' + 'Working2')

FinalHistDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(AttSalesActualsOutput)
FinalRPTDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(AttSalesActualsOutput)

spark.stop()
