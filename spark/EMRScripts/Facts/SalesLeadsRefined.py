from urlparse import urlparse
from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, regexp_replace


class InvalidInputError(RuntimeError):
    def __init__(self, arg):
        self.args = arg


class SalesLeadsRefined(object):

    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.sparkSession.sparkContext.setLogLevel("ERROR")
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.fileFormat = ".parquet"
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

        self.discoveryBucketWorking = sys.argv[5]
        self.refinedBucketWorking = sys.argv[6]

        self.EmpployeeRefineInp = sys.argv[1]
        self.StoreRefineInp = sys.argv[2]
        self.StoreDealerAssInp = sys.argv[3]
        self.ATTDealerCodeInp = sys.argv[4]
        self.SalesLeadInp = sys.argv[5]
        self.SalesLeadOP = sys.argv[6]
        self.dataProcessingErrorPath = sys.argv[7]

        self.discoveryBucket = self.discoveryBucketWorking[self.discoveryBucketWorking.index('tb'):].split("/")[0]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]
        self.salesLeadsName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.workingName = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[2]

        self.salesLeadsWorkingPath = 's3://' + self.refinedBucket + '/' + self.salesLeadsName + '/' + self.workingName
        self.salesLeadsPartitionPath = 's3://' + self.refinedBucket + '/' + self.salesLeadsName
        self.prefixSalesLeadsDiscoveryPath = self.salesLeadsName

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

    def copyFile(self, strS3url, newS3PathURL):

        self.log.info('============ strS3url=[' + strS3url + '] newS3PathURL=' + newS3PathURL + ']')
        newBucketWithPath = urlparse(newS3PathURL)
        newBucket = newBucketWithPath.netloc
        newPath = newBucketWithPath.path.lstrip('/')

        bucketWithPath = urlparse(strS3url)
        bucket = bucketWithPath.netloc
        originalName = bucketWithPath.path.lstrip('/')
        self.log.error("bucket[" + bucket + "] newBucket=[" + newBucket + "] originalName=[" + originalName + "]")
        self.client.copy_object(Bucket=newBucket, CopySource=bucket + '/' + originalName, Key=newPath)
        self.log.info('File name ' + originalName + ' within path  ' + bucket + " copied to new path " + newS3PathURL)

    def loadRefined(self):

        # discoveryBucketNode = self.s3.Bucket(name=self.discoveryBucket)
        # refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

        EmpployeeRefineInp = self.EmpployeeRefineInp
        StoreRefineInp = self.StoreRefineInp
        StoreDealerAssInp = self.StoreDealerAssInp
        ATTDealerCodeInp = self.ATTDealerCodeInp
        SalesLeadInp = self.SalesLeadInp
        # SalesLeadOP = self.SalesLeadOP

        # #################
        # Create a SparkSession (Note, the config section is only for Windows!)
        dfSalesLead = self.sparkSession.read.parquet(SalesLeadInp)
        dfEmpployeeRefine = self.sparkSession.read.parquet(EmpployeeRefineInp)
        dfStoreRefine = self.sparkSession.read.parquet(StoreRefineInp)
        dfStoreDealerAss = self.sparkSession.read.parquet(StoreDealerAssInp)
        dfATTDealer = self.sparkSession.read.parquet(ATTDealerCodeInp)
        '''
        dfEmpployeeRefine.registerTempTable("emprefine")
        dfStoreRefine.registerTempTable("storerefine")
        dfStoreDealerAss.registerTempTable("storedealerass")
        dfATTDealer.registerTempTable("attdealer")
        dfSalesLead.registerTempTable("salesleadtable")
        '''
        # ######################### EXCEPTION HANDLING STARTS ##########################
        self.log.info("========== SalesLeads Exception Handling STARTS")
        isValidFile = True

        cnt = 0
        dfTmp = dfSalesLead.filter(dfSalesLead["enterdate"].rlike("((((19|20)[0-9][0-9])|(0[1-9]|([12][0-9]|3[01]))|([0-9]))(-|/)((0[0-9]|1[0-2])|([0-9])|([0-9][0-9]))(-|/)((0[1-9]|([12][0-9]|3[01]))|((19|20)[0-9][0-9])))"))
        cnt = dfSalesLead.subtract(dfTmp).count()
        dfError = dfSalesLead.subtract(dfTmp)
        if cnt > 0:
            self.log.error("****ERROR: Records having invalid 'enterdate' column values. Record Count=" + str(cnt))
            # dfError = dfError.union(dfSalesLead.subtract(dfTmp))
            dfSalesLead = dfSalesLead.filter(dfSalesLead["enterdate"].rlike("((((19|20)[0-9][0-9])|(0[1-9]|([12][0-9]|3[01]))|([0-9]))(-|/)((0[0-9]|1[0-2])|([0-9])|([0-9][0-9]))(-|/)((0[1-9]|([12][0-9]|3[01]))|((19|20)[0-9][0-9])))"))
            isValidFile = False

        if not isValidFile:

            self.log.info("SalesLeads Data Validation failed for few records. Check the Error bucket:" + self.dataProcessingErrorPath)
            self.log.info("Copy the source files to data processing error path and return.")
            dfError.coalesce(1).write.mode('append').csv(self.dataProcessingErrorPath + '/' + 'SalesLeads_enterdateError', header=True)
            self.log.error("----------------")
            self.log.error(self.SalesLeadInp)
            self.log.error(self.dataProcessingErrorPath)
            self.log.error(self.salesLeadsName)
            self.log.error(self.fileFormat)
            # self.copyFile(self.SalesLeadInp, self.dataProcessingErrorPath + '/' + self.salesLeadsName + self.fileFormat)

            # return

        cnt = 0
        cnt = dfStoreRefine.filter(dfStoreRefine["StoreNumber"].rlike("[^\d]")).count()
        dfError1 = dfStoreRefine.filter(dfStoreRefine["StoreNumber"].rlike("[^\d]"))
        if cnt > 0:
            self.log.error("****ERROR: Records having invalid 'storenumber' column values. Record Count=" + str(cnt))
            dfError1.coalesce(1).write.mode('append').csv(self.dataProcessingErrorPath + '/' + 'StoreRefine_Error', header=True)
            dfStoreRefine = dfStoreRefine.filter(dfStoreRefine["StoreNumber"].rlike("[\d]"))
            isValidFile = False

        cnt = 0
        cnt = dfStoreDealerAss.filter(dfStoreDealerAss["storenumber"].rlike("[^\d]")).count()
        dfError1 = dfStoreDealerAss.filter(dfStoreDealerAss["storenumber"].rlike("[^\d]"))
        if cnt > 0:
            self.log.error("****ERROR: Records having invalid 'storenumber' column values. Record Count=" + str(cnt))
            dfError1.coalesce(1).write.mode('append').csv(self.dataProcessingErrorPath + '/' + 'StoreDealerAss_Error', header=True)
            dfStoreDealerAss = dfStoreDealerAss.filter(dfStoreDealerAss["storenumber"].rlike("[\d]"))
            isValidFile = False

        self.log.info("========== SalesLeads Exception Handling ENDS")
        # ######################### EXCEPTION HANDLING ENDS ##########################

        self.log.info("========== SalesLeads Processing STARTS")
        dfSalesLead = dfSalesLead.withColumn('account', regexp_replace(col('account'), '[\\r\\n\\\\\/\\"\\\"\\\""]', ' '))
        dfSalesLead = dfSalesLead.withColumn('repname', regexp_replace(col('repname'), '[\\r\\n\\\\\/\\"\\\"\\\""]', ' '))

        dfEmpployeeRefine.registerTempTable("emprefine")
        dfStoreRefine.registerTempTable("storerefine")
        dfStoreDealerAss.registerTempTable("storedealerass")
        dfATTDealer.registerTempTable("attdealer")
        dfSalesLead.registerTempTable("salesleadtable")

        ####################################################################################################################
        #                                           Spark Transformaions                                             #
        ####################################################################################################################
        Temp_DF = self.sparkSession.sql("select a.reportdate as reportdate, b.storenumber as storenumber, a.dealercode as dealercode, '4' as companycd, a.SpringMarket as springmarket, a.SpringRegion as springregion, a.SpringDistrict as springdistrict, a.dos as attdirectorofsales, a.arsm as attregionsalesmanager, a.account as customeraccountname, a.existingcustomer as currencustomerindicator, a.repname as salesrepresentativename, a.status as leadstatus, a.grossadds as grossactivations, a.sbassistancerequested as sbassistancerequestindicator, a.enterdate as entereddate, a.closedate as closeddate, a.lastupdate as lastupdatedate, a.bae as baeemployeename, a.description as description, a.contactinfo as contactinfo, a.fan as fan, d.attlocationid as attlocationid, d.attlocationname as attlocationname, a.market as attmarket, a.region as attregion, c.LocationName as locationname, 'GoogleFile' as sourcesystemname, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year, SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from salesleadtable a left join storedealerass b on a.dealercode = b.dealercode left join storerefine c on b.storenumber = c.StoreNumber left join attdealer d on b.dealercode = d.dealercode where b.associationtype = 'Retail' and  b.associationstatus='Active'")

        Temp_DF = Temp_DF.dropDuplicates(['storenumber', 'dealercode', 'companycd', 'reportdate', 'springmarket', 'springregion', 'springdistrict', 'attdirectorofsales', 'attregionsalesmanager', 'customeraccountname', 'currencustomerindicator', 'salesrepresentativename', 'leadstatus', 'grossactivations', 'sbassistancerequestindicator', 'entereddate', 'closeddate', 'lastupdatedate', 'baeemployeename', 'description', 'contactinfo', 'fan', 'attlocationid', 'attlocationname', 'attmarket', 'attregion', 'locationname', 'sourcesystemname'])

        Temp_DF.coalesce(1).select("*").write.mode("append").partitionBy('year', 'month').parquet(self.salesLeadsPartitionPath)
        # Temp_DF.coalesce(1).select("*").write.mode("overwrite").parquet(self.salesLeadsWorkingPath)
        Temp_DF.coalesce(1).select("*").write.mode("overwrite").parquet(self.SalesLeadOP)
        self.sparkSession.stop()

        ##################
        self.log.info("========== SalesLeads Processing ENDS" + self.salesLeadsPartitionPath + "->" + self.salesLeadsWorkingPath)
        self.sparkSession.stop()


if __name__ == "__main__":
        SalesLeadsRefined().loadRefined()
