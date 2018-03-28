from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import sys
from pyspark.sql.functions import col


class EmpOprEffRefined(object):

    def loadRefined(self):

        spark = SparkSession.builder.appName("DimEmployeeOperationalEfficiency").getOrCreate()
        OperationalEfficiencyScoreCard = sys.argv[1]
        DimEmpStoreAssInput = sys.argv[2]
        DimStoreRefinedInput = sys.argv[3]
        DimEmpRefined = sys.argv[4]
        DimEmployeeOperationalEfficiencyOutput = sys.argv[5]
        dataProcessingErrorPath = sys.argv[6]
        print(dataProcessingErrorPath)
        DimOpEff_DF = spark.read.parquet(OperationalEfficiencyScoreCard)
        split_col = split(DimOpEff_DF['location'], ' ')
        DimOpEff_DF = DimOpEff_DF.withColumn('att_loc_id', split_col.getItem(0))
        DimOpEff_DF = DimOpEff_DF.withColumn('att_loc_name', split_col.getItem(1))
        DimOpEff_DF.registerTempTable("DimOperEff")
        DimEmpStoreAss_DF = spark.read.parquet(DimEmpStoreAssInput)
        DimEmpStoreAss_DF.registerTempTable("DimEmpStoreAss")
        DimStoreRefined_DF = spark.read.parquet(DimStoreRefinedInput)
        DimStoreRefined_DF.registerTempTable("DimStoreRefined")
        DimEmpRefined_DF = spark.read.parquet(DimEmpRefined)
        DimOpEff_DF.na.drop(subset=["workdayid"])
        DimEmpRefined_DF.na.drop(subset=["workdayid"])
        DimEmpRefined_DF.registerTempTable("DimEmpRefinedTT")
        Final_Joined_DF = spark.sql("select a.reportdate as reportdate,c.sourceemployeeid as sourceemployeeid, '4' as companycd, 'GoogleSheet' as sourcesystemname , a.salesperson as employeename, a.totalloss as totallossamount, a.totalissues as totalissuescount, a.actiontaken as actiontaken, case when a.hrconsultedbeforetermination = 'Yes' then '1' when a.hrconsultedbeforetermination = 'No' then '0' else '' end  as hrconsultationindicator, a.transactionerrors as transactionerrorsamount, a.totalerrors as transactionerrorscount, a.nexttrades as nexttradesamount, a.totaldevices12 as nexttradesdevicecount, a.hylaloss as hylalossamount, a.totaldevices14 as hyladevicecount, a.deniedrmadevices as deniedrmadevicesamount, a.totaldevice16 as deniedrmadevicescount, a.cashdeposits as cashdepositsamount, a.totalmissingdeposits as totalmissingdepositscount, a.totalshortdeposits as totalshortdepositscount, a.shrinkage as shrinkageamount, a.comments as losscomments, a.att_loc_id as storenumber,a.att_loc_name as locationname, a.market as springmarket, a.region as springregion, a.district as springdistrict , YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from DimOperEff a left join DimEmpRefinedTT c on a.workdayid = c.workdayid ")

        Final_Joined_DF = Final_Joined_DF.dropDuplicates(['sourceemployeeid', 'companycd', 'sourcesystemname'])
        Final_Joined_DF = Final_Joined_DF.where(col("sourceemployeeid").isNotNull())
        Final_Joined_DF.coalesce(1).select("*").write.mode("overwrite").partitionBy('year', 'month').parquet(DimEmployeeOperationalEfficiencyOutput)
        Final_Joined_DF.coalesce(1).select("*").write.mode("overwrite").parquet(DimEmployeeOperationalEfficiencyOutput + '/' + 'Working')
        spark.stop()


if __name__ == "__main__":
    EmpOprEffRefined().loadRefined()
