from pyspark.sql import SparkSession
import sys


class SalesDetailsDelivery(object):

    def loadDelivery(self):

        SalesDeailsInp = sys.argv[1]
        SalesDetailsOP = sys.argv[2]

        # Create a SparkSession (Note, the config section is only for Windows!)
        spark = SparkSession.builder.appName("SalesLeadDelivery").getOrCreate()

        # Reading Parquert and creating dataframe
        dfSalesDetails = spark.read.parquet(SalesDeailsInp)

        # Register spark temp schema
        dfSalesDetails.registerTempTable("salesdetails")

        Temp_DF = spark.sql("select a.reportdate as RPT_DT,"
                            " a.storenumber as STORE_NUM,"
                            " a.productsku as PROD_SKU,"
                            " a.invoicenumber as INVOICE_NBR,"
                            " a.lineid as LINE_ID,"
                            " a.sourceemployeeid as SRC_EMP_ID,"
                            " a.companycd as CO_CD,"
                            " a.sourcesystemname as SRC_SYS_NM,"
                            " a.invoicedate as INVOICE_DT,"
                            " a.sourcecustomerid as SRC_CUST_ID,"
                            " a.productserialnumber as PROD_SN,"
                            " a.price as PRC,"
                            " a.cost as CST,"
                            " a.rqpriority as RQ_PRITY,"
                            " a.quantity as QTY,"
                            " a.specialproductid as SPCL_PROD_ID,"
                            " a.rawgrossprofit as RAW_GRS_PRFT,"
                            " a.saleamount as SALE_AMT"
                            " from salesdetails a")

        Temp_DF.coalesce(1).\
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(SalesDetailsOP + '/' + 'Current')

        Temp_DF.coalesce(1).\
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("append").save(SalesDetailsOP + '/' + 'Previous')

        spark.stop()


if __name__ == "__main__":

    SalesDetailsDelivery().loadDelivery()
