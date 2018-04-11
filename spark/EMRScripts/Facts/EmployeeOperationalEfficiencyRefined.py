from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import sys
import os
from pyspark.sql.functions import col
import boto3
from urlparse import urlparse


class EmpOprEffRefined(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.sparkSession.sparkContext.setLogLevel("ERROR")
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.fileFormat = ".parquet"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.discoveryBucketWorking = sys.argv[1]
        self.refinedBucketWorking = sys.argv[3]
        self.OperationalEfficiencyScoreCard = sys.argv[1]
        self.DimEmpStoreAssInput = sys.argv[2]
        self.DimStoreRefinedInput = sys.argv[3]
        self.DimEmpRefined = sys.argv[4]
        self.DimEmployeeOperationalEfficiencyOutput = sys.argv[5]
        self.dataProcessingErrorPath = sys.argv[6]

        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.EmpOprEffName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[2]

        self.empOffEffFile = self.searchFile(self.OperationalEfficiencyScoreCard, self.EmpOprEffName)

        self.empOprEffWorkingPath = 's3://' + self.refinedBucket + '/' + self.EmpOprEffName + '/' + self.workingName
        self.empOprEffPartitionPath = 's3://' + self.refinedBucket + '/' + self.EmpOprEffName
        self.prefixEmpOprEffDiscoveryPath = self.EmpOprEffName

    def copyFile(self, strS3url, newS3PathURL):

        self.log.error('============ strS3url=[' + strS3url + '] newS3PathURL=' + newS3PathURL + ']')
        filename, fileExtension = os.path.splitext(os.path.basename(self.empOffEffFile))
        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.log.error("bucket[" + bucket + "] newBucket=[" + newBucket + "] originalName=[" + originalName + "]")
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName + filename + fileExtension, Key=newPath)
        self.log.error('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def searchFile(self, strS3url, name):

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        path = bucketWithPath.path.lstrip('/')
        mybucket = self.s3.Bucket(bucket)
        objs = mybucket.objects.filter(Prefix=path)
        filePath = ''
        fileName = ''
        file = ''
        # body = ''

        for s3Object in objs:
            path, filename = os.path.split(s3Object.key)
            filePath = path
            fileName = filename
            file = "s3://" + bucket + "/" + s3Object.key
        self.log.info('File name ' + fileName + ' exists in path  ' + filePath)

        return file

    def loadRefined(self):

        spark = self.sparkSession
        OperationalEfficiencyScoreCard = self.OperationalEfficiencyScoreCard
        DimEmpStoreAssInput = self.DimEmpStoreAssInput
        DimStoreRefinedInput = self.DimStoreRefinedInput
        DimEmpRefined = self.DimEmpRefined
        DimEmployeeOperationalEfficiencyOutput = self.DimEmployeeOperationalEfficiencyOutput
        dataProcessingErrorPath = self.dataProcessingErrorPath
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
        # ######################### EXCEPTION HANDLING STARTS ##########################

        self.log.info("========== EmpOprEffRefined Exception Handling STARTS")
        isValidFile = True
        cnt = 0

        dfError = DimOpEff_DF.filter("trim(workdayid) = '' or workdayid is null")

        if cnt > 0:
            self.log.error("****ERROR: Records having invalid 'reportdate' column values. Record Count=" + str(cnt))
            DimOpEff_DF = DimOpEff_DF.filter("trim(workdayid) = '' or workdayid is null")
            isValidFile = False

        dfTmp = DimOpEff_DF.filter(DimOpEff_DF["reportdate"].rlike("((((19|20)[0-9][0-9])|(0[1-9]|([12][0-9]|3[01]))|([0-9]))(-|/)((0[0-9]|1[0-2])|([0-9])|([0-9][0-9]))(-|/)((0[1-9]|([12][0-9]|3[01]))|((19|20)[0-9][0-9])))"))
        cnt = 0
        cnt = DimOpEff_DF.subtract(dfTmp).count()

        if cnt > 0:
            self.log.error("****ERROR: Records having invalid 'reportdate' column values. Record Count=" + str(cnt))
            dfError = dfError.union(DimOpEff_DF.subtract(dfTmp))
            DimOpEff_DF = DimOpEff_DF.filter(DimOpEff_DF["reportdate"].rlike("((((19|20)[0-9][0-9])|(0[1-9]|([12][0-9]|3[01]))|([0-9]))(-|/)((0[0-9]|1[0-2])|([0-9])|([0-9][0-9]))(-|/)((0[1-9]|([12][0-9]|3[01]))|((19|20)[0-9][0-9])))"))
            isValidFile = False

        if not isValidFile:

            self.log.error("EmpOprEffRefined Data Validation failed for few records. Check the Error bucket:" + dataProcessingErrorPath)
            self.log.error("Copy the source files to data processing error path and return.")
            dfError.coalesce(1).write.mode('append').csv(dataProcessingErrorPath + '/' + 'EmployeeOperationalEfficiencyError', header=True)
            self.log.error("empOffEffFile = " + self.empOffEffFile)
            filename, fileExtension = os.path.splitext(os.path.basename(self.empOffEffFile))
            self.copyFile(OperationalEfficiencyScoreCard, dataProcessingErrorPath + '/' + filename + fileExtension)
            # return

        self.log.info("========== EmpOprEffRefined Exception Handling ENDS")

        ###############################################################################

        DimOpEff_DF.na.drop(subset=["workdayid"])
        DimEmpRefined_DF.na.drop(subset=["workdayid"])

        DimOpEff_DF.registerTempTable("DimOperEff")
        DimEmpRefined_DF.registerTempTable("DimEmpRefinedTT")

        Final_Joined_DF = spark.sql("select a.reportdate as reportdate,c.sourceemployeeid as sourceemployeeid, '4' as companycd, 'GoogleSheet' as sourcesystemname , a.salesperson as employeename, a.totalloss as totallossamount, a.totalissues as totalissuescount, a.actiontaken as actiontaken, case when a.hrconsultedbeforetermination = 'Yes' then '1' when a.hrconsultedbeforetermination = 'No' then '0' else '' end  as hrconsultationindicator, a.transactionerrors as transactionerrorsamount, a.totalerrors as transactionerrorscount, a.nexttrades as nexttradesamount, a.totaldevices12 as nexttradesdevicecount, a.hylaloss as hylalossamount, a.totaldevices14 as hyladevicecount, a.deniedrmadevices as deniedrmadevicesamount, a.totaldevice16 as deniedrmadevicescount, a.cashdeposits as cashdepositsamount, a.totalmissingdeposits as totalmissingdepositscount, a.totalshortdeposits as totalshortdepositscount, a.shrinkage as shrinkageamount, a.comments as losscomments, a.att_loc_id as storenumber,a.att_loc_name as locationname, a.market as springmarket, a.region as springregion, a.district as springdistrict , YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from DimOperEff a left join DimEmpRefinedTT c on a.workdayid = c.workdayid ")

        Final_Joined_DF = Final_Joined_DF.dropDuplicates(['reportdate', 'sourceemployeeid', 'companycd', 'sourcesystemname'])
        Final_Joined_DF = Final_Joined_DF.where(col("sourceemployeeid").isNotNull())
        Final_Joined_DF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(DimEmployeeOperationalEfficiencyOutput)
        Final_Joined_DF.coalesce(1).select("*").write.mode("overwrite").parquet(DimEmployeeOperationalEfficiencyOutput + '/' + 'Working')
        spark.stop()


if __name__ == "__main__":
    EmpOprEffRefined().loadRefined()
