# ********************************************************************************************************************************************
#   SPARK Python Pgm Name : DimCustExpDelivery                                                                                               *
#   Written By            : Aakash Basu                                                                                                      *
#   Date Written          : October 2017                                                                                                     *
#   Project Name          : GameStop                                                                                                         *
#   Version               : 1.0                                                                                                              *
#   Description           : This code is for retrieving the customer experience of stores                                                    *
#   Change                : None																                                             *
#   History 	          : None                                                                                                             *
#   NOTES		          : Complete                                                                                                         *
#   Spark Submit Command  : spark-submit --class spark.FactDeposits --master yarn-client --num-executors 30 --executor-cores 4               *
#                          --executor-memory 24G --driver-memory 4G                                                                          *
#                          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0                               *
#                          target/scala-2.11/factdeposits_2.11-1.0.jar <FactSnapShot HDFS Path> <DimCustomer HDFS Path>                      *
#                          <DimCalendar HDFS Path> <DimProduct HDFS Path> <DimScenario  HDFS Path> <PrmMain HDFS Path>                       *
#                          <BplData_norm HDFS Path> <parallelism config value> <shuffle partitions config value>                             *
#                          <storage memoryfraction config value> <maxresultsize config value> <shuffle spillafterread config value>          *
#                          <executor memoryoverhead config value>                                                                            *
# ********************************************************************************************************************************************

from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.\
        appName("DimCompliance").getOrCreate()

StoreTrafficInput = sys.argv[1]
StoreTrafficOutput = sys.argv[2]

StoreTrafficTable_DF = spark.read.parquet(StoreTrafficInput).registerTempTable("StoreTrafficTable")

Final_DF = spark.sql("select a.reportdate as RPT_DT,a.storenumber as STORE_NUM , a.trafficdate TRAFFIC_DT, a.traffictime TRAFFIC_TM, a.companycd CO_CD,"
                     + "a.sourcesystemlocationid  as SRC_SYS_LOC_ID, a.sourcesystemname SRC_SYS_NM , a.traffictype TRAFFIC_TYP, a.trafficcount TRAFFIC_CNT "
                     + "from StoreTrafficTable a")

Final_DF.coalesce(1).select("*"). \
        write.format("com.databricks.spark.csv").option("quoteMode", "All").\
        option("header", "true").mode("overwrite").save(StoreTrafficOutput+'/'+'Current')

spark.stop()
