from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("EmpTransAdj").getOrCreate()

EmpTransAdjOut = sys.argv[1]
EmpTransAdjIn = sys.argv[2]

#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

dfEmpTrnasAdj = spark.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").load(EmpTransAdjIn)

dfEmpTrnasAdj = dfEmpTrnasAdj.withColumnRenamed("Market", "market").\
    withColumnRenamed("Region", "region").\
    withColumnRenamed("District", "district").\
    withColumnRenamed("Location", "location").\
    withColumnRenamed("Loc", "loc").\
    withColumnRenamed("SalesPerson", "salesperson").\
    withColumnRenamed("SalesPersonID", "salespersonid").\
    withColumnRenamed(" DF Accessory Adjustment ", "DFAccessoryAdjustment_wxyz").\
    withColumnRenamed(" Feature Adjustment ", "FeatureAdjustment_wxyz").\
    withColumnRenamed(" Incorrect ActUpg ", "IncorrectActUpg_wxyz").\
    withColumnRenamed(" Incorrect BYOD ", "IncorrectBYOD_wxyz").\
    withColumnRenamed(" Incorrect DF ", "IncorrectDF_wxyz").\
    withColumnRenamed(" Incorrect Prepaid ", "IncorrectPrepaid_wxyz").\
    withColumnRenamed(" Incorrect Transactions ", "IncorrectTransactions_wxyz").\
    withColumnRenamed(" DirecTV NOW Adjustment ", "DirecTVNOWAdjustment_wxyz").\
    withColumnRenamed(" TV Disputes ", "TVDisputes_wxyz").\
    withColumnRenamed(" TV Extras Adjustment ", "TVExtrasAdjustment_wxyz").\
    withColumnRenamed(" Wired Disputes ", "WiredDisputes_wxyz").\
    withColumnRenamed(" Wired Extras Adjustment ", "WiredExtrasAdjustment_wxyz").\
    withColumnRenamed(" Deposit Adjustment ", "DepositAdjustment_wxyz").\
    withColumnRenamed(" Total Adjustment ", "TotalAdjustment_wxyz")

today = datetime.now().strftime('%m/%d/%Y')
dfEmpTrnasAdj = dfEmpTrnasAdj.withColumn('reportdate', lit(today))

dfEmpTrnasAdj = dfEmpTrnasAdj.registerTempTable("EmpTransAdj")

FinalDF = spark.sql("select market,region,district,location,loc,salesperson,salespersonid,DFAccessoryAdjustment_wxyz,"
                    "FeatureAdjustment_wxyz,IncorrectActUpg_wxyz,IncorrectBYOD_wxyz,IncorrectDF_wxyz,"
                    "IncorrectPrepaid_wxyz,IncorrectTransactions_wxyz,DirecTVNOWAdjustment_wxyz,"
                    "TVDisputes_wxyz,TVExtrasAdjustment_wxyz,WiredDisputes_wxyz,"
                    "WiredExtrasAdjustment_wxyz,DepositAdjustment_wxyz,TotalAdjustment_wxyz,reportdate,"
                    "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from EmpTransAdj")

FinalDF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').format('parquet').save(EmpTransAdjOut)

FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(EmpTransAdjOut + '/' + 'Working')

spark.stop()
