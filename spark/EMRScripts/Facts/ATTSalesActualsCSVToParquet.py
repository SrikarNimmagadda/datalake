from pyspark.sql import SparkSession
import sys
import csv
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit, regexp_replace, rtrim


class AttSalesActualsCsvtoParquet(object):

        def __init__(self):

                self.appName = self.__class__.__name__
                self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
                self.attSalesActualsOutput = sys.argv[1]
                self.attSalesActualsInput = sys.argv[2]
                self.attMyResultsInp2 = sys.argv[3]

                #########################################################################################################
                #                                 Reading the source data files                                         #
                #########################################################################################################

        def loadParquet(self):

                schema = StructType([StructField('Location', StringType(), False), StructField('Loc Id', StringType(), False), StructField('Dlr1 Code', StringType(), False), StructField('KPI', StringType(), False), StructField('KPI ID', StringType(), False), StructField('Dec 2017 Actual', StringType(), True), StructField('Dec 2017 Projected', StringType(), True), StructField('Dec 2017 Target', StringType(), True)])

                dfAttSalesActuals = self.sparkSession.sparkContext.textFile(self.attSalesActualsInput).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: ''.join(line).strip() != '' and line[5] != 'Location' and len(line[0]) != 0).toDF(schema)

                dfAttSalesActuals1 = dfAttSalesActuals.filter(~(dfAttSalesActuals.Location.like('Histori%%')))
                dfAttSalesActuals2 = dfAttSalesActuals1.filter(~(dfAttSalesActuals.Location.like('AT&T MyRe%%')))
                dfAttSalesActuals3 = dfAttSalesActuals2.filter(~(dfAttSalesActuals.Location.like('AR : SPRI%%')))
                dfATTSalesActuals4 = dfAttSalesActuals3.filter(~(dfAttSalesActuals.Location.like('Generated%%')))
                dfAttSalesActualsInput = dfATTSalesActuals4.filter(~(dfAttSalesActuals.Location.like('Locat%%')))

                finalHistDf1 = dfAttSalesActualsInput.withColumnRenamed("Location", "attlocationname").\
                    withColumnRenamed("Loc Id", "locid").\
                    withColumnRenamed("Dlr1 Code", "dealercode").\
                    withColumnRenamed("KPI", "kpiname").\
                    withColumnRenamed("KPI ID", "kpiid").\
                    withColumnRenamed("Dec 2017 Actual", "actual_value").\
                    withColumnRenamed("Dec 2017 Projected", "projected_value").\
                    withColumnRenamed("Dec 2017 Target", "targetvalue")

                finalHistDf1 = finalHistDf1.withColumn("actualvalue", rtrim(regexp_replace("actual_value", '\\$|\\%', '')))
                finalHistDf1 = finalHistDf1.withColumn("projectedvalue", rtrim(regexp_replace("projected_value", '\\$|\\%', '')))

                dfAttSalesActualsRPT = self.sparkSession.sparkContext.textFile(self.attMyResultsInp2).mapPartitions(lambda partition: csv.reader([line.encode('utf-8').replace('\0', '').replace('\n', '') for line in partition], delimiter=',', quotechar='"')).filter(lambda line: ''.join(line).strip() != '' and line[5] != 'Location' and len(line[0]) != 0).toDF(schema)

                dfAttSalesActualsRPT1 = dfAttSalesActualsRPT.filter(~(dfAttSalesActualsRPT.Location.like('Histori%%')))
                dfAttSalesActualsRPT2 = dfAttSalesActualsRPT1.filter(~(dfAttSalesActualsRPT.Location.like('AT&T MyRe%%')))
                dfAttSalesActualsRPT3 = dfAttSalesActualsRPT2.filter(~(dfAttSalesActualsRPT.Location.like('AR : SPRI%%')))
                dfAttSalesActualsRPT4 = dfAttSalesActualsRPT3.filter(~(dfAttSalesActualsRPT.Location.like('Generated%%')))
                dfAttSalesActualsInputRPT = dfAttSalesActualsRPT4.filter(~(dfAttSalesActualsRPT.Location.like('Locat%%')))

                finalRptDf1 = dfAttSalesActualsInputRPT.withColumnRenamed("Location", "attlocationname2").\
                    withColumnRenamed("Loc Id", "locid2").\
                    withColumnRenamed("Dlr1 Code", "dealercode2").\
                    withColumnRenamed("KPI", "kpiname2").\
                    withColumnRenamed("KPI ID", "kpiid2").\
                    withColumnRenamed("Dec 2017 Actual", "actual_value2").\
                    withColumnRenamed("Dec 2017 Projected", "projected_value2").\
                    withColumnRenamed("Dec 2017 Target", "targetvalue2")

                finalRptDf1 = finalRptDf1.withColumn("actualvalue2", rtrim(regexp_replace("actual_value2", '\\$|\\%', '')))
                finalRptDf1 = finalRptDf1.withColumn("projectedvalue2", rtrim(regexp_replace("projected_value2", '\\$|\\%', '')))

                today = datetime.now().strftime('%m/%d/%Y')
                finalHistDf1 = finalHistDf1.withColumn('reportdate', lit(today))
                finalHistDf1.registerTempTable("HIST")
                finalRptDf1 = finalRptDf1.withColumn('reportdate2', lit(today))
                finalRptDf1.registerTempTable("RPT")

                finalHistDf = self.sparkSession.sql("select attlocationname,locid,dealercode,kpiname,kpiid,actualvalue,projectedvalue,"
                                                    "targetvalue,reportdate,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                                    "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from HIST")

                finalRptDf = self.sparkSession.sql("select attlocationname2,locid2,dealercode2,kpiname2,kpiid2,"
                                                   "actualvalue2,projectedvalue2,targetvalue2,reportdate2,"
                                                   "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from RPT")

                finalHistDf.coalesce(1).select("*").write.mode("overwrite").parquet(self.attSalesActualsOutput + '/' + 'Working1')

                finalRptDf.coalesce(1).select("*").write.mode("overwrite").parquet(self.attSalesActualsOutput + '/' + 'Working2')

                finalHistDf.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.attSalesActualsOutput)
                finalRptDf.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(self.attSalesActualsOutput)

                self.sparkSession.stop()


if __name__ == "__main__":
        AttSalesActualsCsvtoParquet().loadParquet()
