from pyspark.sql import SparkSession
import sys


class StoreRecHeadcountRefinedToDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.outputPath = sys.argv[1]
        self.storeRecHCInputPath = sys.argv[2]

    def loadDelivery(self):

        self.sparkSession.read.parquet(self.storeRecHCInputPath + '/StoreRecruitingHeadcount/Working').registerTempTable("StoreRecHC")

        #########################################################################################################
        #                                 Spark Transformation begins here                                      #
        #########################################################################################################

        dfStoreRecHCFinal = self.sparkSession.sql("select report_date as RPT_DT,store_number as STORE_NUM,companycd as CO_CD,"
                                                  "approved_headcount as APRV_HEAD_CNT,"
                                                  "store_managers_count as SMS_CNT,business_assistant_manager_count as BUS_ASST_MGR_CNT,"
                                                  "fulltime_equivalent_count as FTM_EQUIV_CNT,parttime_equivalent_count as PRTTM_EQUIV_CNT,"
                                                  "fulltime_floater_count as FTM_FLOATER_CNT,district_lead_sales_consultant_count as DSTRC_LEAD_SALES_CNSLT_CNT,"
                                                  "mit_count as MIT_CNT,seasonal_count as SEAS_CNT,actual_headcount as ACTL_HDCT,a.dealer_code as DLR_CD"
                                                  " from StoreRecHC a")

        dfStoreRecHCFinal.coalesce(1). \
            write.format("com.databricks.spark.csv"). \
            option("header", "true").mode("overwrite").save(self.outputPath + '/' + 'Current')

        dfStoreRecHCFinal.coalesce(1). \
            write.format("com.databricks.spark.csv"). \
            option("header", "true").mode("append").save(self.outputPath + '/' + 'Previous')

        self.sparkSession.stop()


if __name__ == "__main__":
    StoreRecHeadcountRefinedToDelivery().loadDelivery()
