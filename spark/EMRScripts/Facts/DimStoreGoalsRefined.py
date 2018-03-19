from pyspark.sql import SparkSession
import sys
import boto3
from pyspark.sql.functions import year, substring, unix_timestamp, from_unixtime, explode, split, lit, regexp_replace,\
    col
from datetime import datetime


class DimStoreGoalsRefined(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryBucketWorking = sys.argv[1]
        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]
        self.refinedBucketWorking = sys.argv[2]
        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.storeGoalsName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[2]

        self.storeGoalsWorkingPath = 's3://' + self.refinedBucket + '/' + self.storeGoalsName + '/' + self.workingName
        self.storeGoalsPartitionPath = 's3://' + self.refinedBucket + '/' + self.storeGoalsName
        self.prefixStoreGoalsDiscoveryPath = self.storeGoalsName
        self.prefixStoreTransactionAdjRefinedPath = 'StoreTransactionAdj'
        self.prefixStoreRefinedPath = 'Store'

        self.storeGoalTransposeColumns = "GrossProfit,PremiumVideo,DigitalLife,AccGPOROpp,CRUOpps,Tablets," \
                                         "IntegratedProducts,WTR,GoPhone,Overtime,DTVNow,AccessoryAttachRate," \
                                         "Broadband,Traffic,ApprovedFTE,ApprovedHC,GoPhoneAutoEnrollment,Opps," \
                                         "SMTraffic,Entertainment"

        self.columnsDict = {'GrossProfit': 'Gross Profit', 'PremiumVideo': 'Premium Video',
                            'DigitalLife': 'Digital Life', 'AccGPOROpp': 'Acc GP/Opp', 'CRUOpps': 'CRU Opps',
                            'IntegratedProducts': 'Integrated Products', 'GoPhone': 'Go Phone', 'DTVNow': 'DTV Now',
                            'AccessoryAttachRate': 'Accessory Attach Rate', 'ApprovedFTE': 'Approved FTE',
                            'ApprovedHC': 'Approved HC', 'GoPhoneAutoEnrollment': 'GoPhone Auto Enrollment %',
                            'SMTraffic': 'SM Traffic'}
        self.tempStr = ','.join(["'" + x + "&',nvl(" + x + ",'0.00'),','" for x in
                                 self.storeGoalTransposeColumns.split(',')])
        self.storeTransposeConcat = self.tempStr[:len(self.tempStr) - 4]

    def findLastModifiedFile(self, bucketNode, prefixType, bucket):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
        self.log.info("prefixPath is " + prefixPath)
        partitionName = bucketNode.objects.filter(Prefix=prefixPath)
        all_values_dict = {}
        req_values_dict = {}
        for obj in partitionName:
            all_values_dict[obj.key] = obj.last_modified
        for k, v in all_values_dict.items():
            if 'part-0000' in k:
                req_values_dict[k] = v
        revSortedFiles = sorted(req_values_dict, key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        s3 = boto3.resource('s3')

        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedStoreGoalsFile = self.findLastModifiedFile(discoveryBucketNode,
                                                              self.prefixStoreGoalsDiscoveryPath, self.discoveryBucket)

        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        lastUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreRefinedPath,
                                                         self.refinedBucket)

        dfStoreGoalsDisc = self.sparkSession.read.parquet(lastUpdatedStoreGoalsFile).filter("Store != ' '")

        dfStoreGoalsDisc.withColumn('StoreNumber', regexp_replace(col("Store"), '\D', '')).drop("Store").\
            registerTempTable("StoreGoalsTT")

        dfFlatteningStoreGoals = self.sparkSession.sql("select StoreNumber, ReportDate,concat(" +
                                                       self.storeTransposeConcat + ") as concatenated from "
                                                                                   "StoreGoalsTT")

        dfFlatTableExplodedFrame = dfFlatteningStoreGoals.\
            select("StoreNumber", "ReportDate", explode(split(dfFlatteningStoreGoals.concatenated, ",")).
                   alias("Goal_ValueTemp"))

        splitGoalValues = split(dfFlatTableExplodedFrame['Goal_ValueTemp'], '&')

        dfFlatTableExplodedFrame.withColumn('KPIName', splitGoalValues.getItem(0)).\
            withColumn('GoalValue', splitGoalValues.getItem(1)).\
            withColumn('CompanyCode', lit('4')).drop("Goal_ValueTemp").registerTempTable("StoreGoalsTransposed_TT")

        self.sparkSession.read.parquet(lastUpdatedStoreFile).registerTempTable("StoreRefined_TT")

        sqlKPINameQuery = "case "
        for name, newName in self.columnsDict.items():
            sqlKPINameQuery = sqlKPINameQuery + "when a.KPIName = '" + name + "' then '" + newName + "' "
        sqlKPINameQuery = sqlKPINameQuery + " else a.KPIName end as KPIName"

        self.sparkSession.sql("select distinct a.ReportDate as ReportDate, " + sqlKPINameQuery +
                              ", a.StoreNumber, '4' as CompanyCd, a.GoalValue as GoalValue, "
                              "b.LocationName as LocationName, b.SpringMarket as SpringMarket, "
                              "b.SpringRegion as SpringRegion, b.SpringDistrict as SpringDistrict "
                              "from StoreGoalsTransposed_TT a inner join StoreRefined_TT b on "
                              "a.StoreNumber = b.StoreNumber").registerTempTable("StoreRefined_SP")

        dfStoreGoals = self.sparkSession.sql("select a.ReportDate, a.KPIName, a.StoreNumber, a.CompanyCd, "
                                             "case when a.GoalValue rlike '%' then cast(cast(regexp_replace(a.GoalValue,'%','') as float)/100 as decimal(8,4)) else a.GoalValue end as GoalValue, "
                                             "a.LocationName, a.SpringMarket, a.SpringRegion, a.SpringDistrict "
                                             "from StoreRefined_SP a ")

        dfStoreGoals.coalesce(1).write.mode("overwrite").parquet(self.storeGoalsWorkingPath)
        dfStoreGoals.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp())))\
            .withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).\
            write.mode('append').partitionBy('year', 'month').format('parquet').save(self.storeGoalsPartitionPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreGoalsRefined().loadRefined()
