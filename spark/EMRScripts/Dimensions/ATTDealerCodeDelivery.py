from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class ATTDealerCodeDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.dealerCodeOutput = sys.argv[1]
        self.dealerCodeIn = sys.argv[2]
        self.refinedBucket = self.dealerCodeIn[self.dealerCodeIn.index('tb'):].split("/")[0]
        self.prefixAttDealerRefinePath = self.dealerCodeIn[self.dealerCodeIn.index('tb'):].split("/")[1]
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)

    def makeZeroByteFile(self, destinationPath, fileName):

        newBucketWithPath = urlparse(destinationPath)
        newBucket = newBucketWithPath.netloc
        newBucketNode = self.s3.Bucket(name=newBucket)
        newPath = newBucketWithPath.path.lstrip('/')
        objs = newBucketNode.objects.filter(Prefix=newPath)
        for s3Object in objs:
            s3Object.delete()

        newFile = open(fileName, 'w')
        newFile.close()
        self.client.upload_file(fileName, newBucket, newPath + '/' + fileName)

    def findLastModifiedFile(self, bucketNode, prefixType, bucket, currentOrPrev=1):

        prefixPath = prefixType + '/year=' + datetime.now().strftime('%Y')
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
        lastPreviousRefinedPath = ''
        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastUpdatedFilePath)

        if numFiles > 1:
            secondLastModifiedFileName = str(revSortedFiles[1])
            lastPreviousRefinedPath = "s3://" + bucket + "/" + secondLastModifiedFileName
            self.log.info("Last Modified file in s3 format is : " + lastPreviousRefinedPath)

        if currentOrPrev == 0:
            return lastPreviousRefinedPath

        return lastUpdatedFilePath

    def loadDelivery(self):

        # dfDealerCode = self.sparkSession.read.parquet(self.dealerCodeIn)
        dfDealerCodeCurrFile = self.findLastModifiedFile(self.refinedBucketNode, self.prefixAttDealerRefinePath, self.refinedBucket)
        dfDealerCode = self.sparkSession.read.parquet(dfDealerCodeCurrFile)

        #############################################################################################
        #                                 Reading the source data files                              #
        ############################################################################################

        lastPrevAttDealerCodeFile = self.findLastModifiedFile(self.refinedBucketNode, self.prefixAttDealerRefinePath,
                                                              self.refinedBucket, 0)
        if lastPrevAttDealerCodeFile != '':
            dfAttDealerCodePrev = self.sparkSession.read.parquet(lastPrevAttDealerCodeFile)

            dfDealerCode.subtract(dfAttDealerCodePrev).registerTempTable("att_dealer_delta")
            dfAttDealerCodePrev.registerTempTable("att_dealer_prev")

            dfAttDealerCodeNew = self.sparkSession.sql(
                "select a.dealercode, a.companycode, a.dealercodeorigin, a.dfcode, a.dcstatus, a.dfindicator, a.candc, "
                "a.opendate, a.closedate, a.whitestoreindicator, a.whitestoreexpirationdate, a.sortrank, "
                "a.rankdescription, a.storeorigin, a.origin, a.businessexpert, a.footprintlevel, "
                "a.attlocationextendedname, a.attlocationid, a.attlocationname, a.attdisputemarket, a.attmarketcode, "
                "a.attmarket, a.olddealercode, a.olddealercode2, a.attregion, a.notes, a.notes2,'I' as cdc_ind_cd from "
                "att_dealer_delta a left join att_dealer_prev b on a.dealercode = b.dealercode "
                "where b.dealercode is null")
            rowCountNewRecords = dfAttDealerCodeNew.count()

            dfAttDealerCodeUpdated = self.sparkSession.sql(
                "select a.dealercode, a.companycode, a.dealercodeorigin, a.dfcode, a.dcstatus, a.dfindicator, a.candc, "
                "a.opendate, a.closedate, a.whitestoreindicator, a.whitestoreexpirationdate, a.sortrank, "
                "a.rankdescription, a.storeorigin, a.origin, a.businessexpert, a.footprintlevel, "
                "a.attlocationextendedname, a.attlocationid, a.attlocationname, a.attdisputemarket, a.attmarketcode, "
                "a.attmarket, a.olddealercode, a.olddealercode2, a.attregion, a.notes, a.notes2,'C' as cdc_ind_cd from "
                "att_dealer_delta a left join att_dealer_prev b on a.dealercode = b.dealercode "
                "where b.dealercode is not null")
            rowCountUpdateRecords = dfAttDealerCodeUpdated.count()

            dfAttDealerCodeUpdated.registerTempTable("att_dealer_updated_data")

            dfAttDealerCodeNew.registerTempTable("att_dealer_new_data")

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                self.sparkSession.sql("select * from att_dealer_updated_data union all select * from "
                                      "att_dealer_new_data").registerTempTable("Dealer")
                dfDealerCode = self.sparkSession.sql(
                    "select a.dealercode as DLR_CD,a.companycode as CO_CD,a.dealercodeorigin as DL_CD_ORIG, "
                    "a.dfcode as DF_CD,a.dcstatus as DC_STAT, a.dfindicator as DF_IND,a.candc as C_AND_C,"
                    "a.opendate as OPEN_DT, a.closedate as CLOSE_DT, a.whitestoreindicator as WS_IND,"
                    "a.whitestoreexpirationdate as WS_EXP_DT,a.sortrank as SRT_RNK,a.rankdescription as RNK_DESC,"
                    "a.storeorigin as STORE_ORIG, a.origin as ORIG, a.businessexpert as BUS_EXPRT,"
                    "a.footprintlevel as FTPRT_LVL,a.attlocationextendedname as ATT_LOC_EXT_NM,"
                    "a.attlocationid as ATT_LOC_ID,a.attlocationname as ATT_LOC_NM,"
                    "a.attdisputemarket as ATT_DSPT_MKT, a.cdc_ind_cd  from Dealer a")
                dfDealerCode.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header",
                                                                                                     "true").mode(
                    "overwrite").save(self.dealerCodeOutput + '/' + 'Current')

                dfDealerCode.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header",
                                                                                                     "true").mode(
                    "append").save(self.dealerCodeOutput + '/' + 'Previous')
            else:
                self.makeZeroByteFile(self.dealerCodeOutput + '/' + 'Current', 'wt_att_delr_cds.csv')
                self.log.info("The prev and current files same.So zero size delta file generated in delivery bucket.")

        else:
            dfDealerCode.registerTempTable("Dealer")
            dfDealerCode = self.sparkSession.sql(
                "select a.dealercode as DLR_CD,a.companycode as CO_CD,a.dealercodeorigin as DL_CD_ORIG, "
                "a.dfcode as DF_CD,a.dcstatus as DC_STAT, a.dfindicator as DF_IND,a.candc as C_AND_C,"
                "a.opendate as OPEN_DT, a.closedate as CLOSE_DT, a.whitestoreindicator as WS_IND,"
                "a.whitestoreexpirationdate as WS_EXP_DT,a.sortrank as SRT_RNK,a.rankdescription as RNK_DESC,"
                "a.storeorigin as STORE_ORIG, a.origin as ORIG, a.businessexpert as BUS_EXPRT,"
                "a.footprintlevel as FTPRT_LVL,a.attlocationextendedname as ATT_LOC_EXT_NM,"
                "a.attlocationid as ATT_LOC_ID,a.attlocationname as ATT_LOC_NM,"
                "a.attdisputemarket as ATT_DSPT_MKT, 'I' as CDC_IND_CD  from Dealer a")

            dfDealerCode.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode(
                "overwrite").save(self.dealerCodeOutput + '/' + 'Current')

            dfDealerCode.coalesce(1).select("*").write.format("com.databricks.spark.csv").option("header", "true").mode(
                "append").save(self.dealerCodeOutput + '/' + 'Previous')

        self.sparkSession.stop()


if __name__ == "__main__":
    ATTDealerCodeDelivery().loadDelivery()
