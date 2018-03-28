from pyspark.sql import SparkSession
import sys


class EmpOprEffDelivery(object):

    def loadDelivery(self):
        spark = SparkSession.builder.appName("DimEmployeeOperationalEfficiencyDelivery").getOrCreate()
        OperationalEfficiencyInput = sys.argv[1]
        DimEmpOperEffOutput = sys.argv[2]
        DimOpEff_DF = spark.read.parquet(OperationalEfficiencyInput)
        DimOpEff_DF.registerTempTable("OperationalEfficiency")
        Final_Joined_DF = spark.sql("select distinct a.reportdate RPT_DT,  a.sourceemployeeid SRC_EMP_ID,a.companycd CO_CD, 'GoogleSheet' as SRC_SYS_NM,a.totallossamount TTL_LOSS_AMT, a.totalissuescount TTL_ISUS_CNT, a.actiontaken ACTN_TAKN, a.hrconsultationindicator HR_CNSLTATION_IND, a.transactionerrorsamount TRANS_ERRS_AMT, a.transactionerrorscount TRANS_ERRS_CNT, a.nexttradesamount NXT_TRDS_AMT, a.nexttradesdevicecount NXT_TRDS_DVC_CNT, a.hylalossamount HYLA_LOSS_AMT, a.hyladevicecount HYLA_DVC_CNT, a.deniedrmadevicesamount DNY_RMA_DVC_AMT, a.deniedrmadevicescount DNY_RMA_DVC_CNT, a.cashdepositsamount CSH_DPSTS_AMT, a.totalmissingdepositscount as TTL_MISSING_DPSTS_CNT 	, a.totalshortdepositscount TTL_SHRT_DPSTS_CNT, a.shrinkageamount SHRNKAGE_AMT, a.losscomments LOSS_CMNT, a.storenumber as STORE_NUM from OperationalEfficiency a")
        Final_Joined_DF.dropDuplicates().coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DimEmpOperEffOutput + '/' + 'Current')
        Final_Joined_DF.dropDuplicates().coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(DimEmpOperEffOutput + '/' + 'Previous')
        spark.stop()


if __name__ == "__main__":
    EmpOprEffDelivery().loadDelivery()
