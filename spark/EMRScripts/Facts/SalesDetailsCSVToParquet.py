from datetime import datetime
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DoubleType, DateType
from pyspark.sql.functions import lit
import sys
from pyspark.sql import SparkSession
SalesDetailsInp = sys.argv[1]
SalesDetailsOP = sys.argv[2]

spark = SparkSession.builder.appName("SalesDetails_CSVToParquet").getOrCreate()

schema = StructType([StructField('ChannelName', StringType(), True),
                    StructField('StoreID', IntegerType(), True),
                    StructField('StoreName', StringType(), True),
                    StructField('CustomerID', IntegerType(), False),
                    StructField('CustomerName', StringType(), False),
                    StructField('EmployeeID', IntegerType(), True),
                    StructField('DateCreated', DateType(), False),
                    StructField('InvoiceNumber', StringType(), True),
                    StructField('LineID', IntegerType(), False),
                    StructField('ProductSKU', StringType(), True),
                    StructField('Price', DoubleType(), False),
                    StructField('Cost', DoubleType(), False),
                    StructField('RQPriority', IntegerType(), False),
                    StructField('Quantity', IntegerType(), False),
                    StructField('EmployeeName', StringType(), True),
                    StructField('SerialNumber', StringType(), False),
                    StructField('SpecialProductID', IntegerType(), False)])

dfSalesDetails = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

dfSalesDetails = spark.read.format("com.databricks.spark.csv").option("header", "true").option("treatEmptyValuesAsNulls", "true").load(SalesDetailsInp)

dfSalesDetails = dfSalesDetails.withColumnRenamed("ChannelName", "channelname").withColumnRenamed("StoreID", "storeid").withColumnRenamed("StoreName", "storename").withColumnRenamed("CustomerID", "customerid").withColumnRenamed("CustomerName", "customername").withColumnRenamed("EmployeeID", "employeeid").withColumnRenamed("DateCreated", "datecreated").withColumnRenamed("InvoiceNumber", "invoicenumber").withColumnRenamed("LineID", "lineid").withColumnRenamed("ProductSKU", "productsku").withColumnRenamed("Price", "price").withColumnRenamed("Cost", "cost").withColumnRenamed("RQPriority", "rqpriority").withColumnRenamed("Quantity", "quantity").withColumnRenamed("EmployeeName", "employeename").withColumnRenamed("SerialNumber", "serialnumber").withColumnRenamed("SpecialProductID", "specialproductid")

today = datetime.now().strftime('%m/%d/%Y')
dfSalesDetails = dfSalesDetails.withColumn('reportdate', lit(today))


dfSalesDetails.registerTempTable("SalesDetails")
dfSalesDetailsFinal = spark.sql("select a.channelname,a.storeid, a.storename,a.customerid,a.customername,a.employeeid,a.datecreated,a.invoicenumber,a.lineid,a.productsku,a.price,a.cost,a.rqpriority,a.quantity,a.employeename,a.serialnumber,a.specialproductid,YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month,a.reportdate from SalesDetails a ")


dfSalesDetailsFinal.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(SalesDetailsOP)

dfSalesDetailsFinal.coalesce(1).select("*").write.mode("overwrite").parquet(SalesDetailsOP + '/' + 'Working')
spark.stop()
