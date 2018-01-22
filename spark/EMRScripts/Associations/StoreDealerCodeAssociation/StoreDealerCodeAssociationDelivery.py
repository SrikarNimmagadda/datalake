from pyspark.sql import SparkSession
import sys,os
from pyspark.sql.functions import col


class StoreDealerCodeAssociationDelivery:

    def __init__(self):
        self.storeAssociationInput = sys.argv[1]
        self.storeAssociationOutput = sys.argv[2]

    def loadDelivery(self):
        spark = SparkSession.builder.appName("StoreDealerCodeAssociationDelivery").getOrCreate()
        dfStoreDealer = spark.read.parquet(self.storeAssociationInput).filter(col("DealerCode") != '').registerTempTable("storeAss")
        dfStoreDealerTemp = spark.sql(
            "select cast(a.StoreNumber as integer) as STORE_NUM,a.DealerCode as DLR_CD,a.CompanyCode as CO_CD,a.AssociationType as ASSOC_TYP,"
            + "a.AssociationStatus as ASSOC_STAT from storeAss a")

        dfStoreDealerTemp.coalesce(1).select("*").write.mode("overwrite").csv(self.storeAssociationOutput, header=True);

        spark.stop()

if __name__ == "__main__":
    StoreDealerCodeAssociationDelivery().loadDelivery()