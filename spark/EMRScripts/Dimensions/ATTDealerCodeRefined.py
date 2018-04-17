from pyspark.sql import SparkSession
from pyspark.sql.functions import hash, year, from_unixtime, unix_timestamp, substring
import sys
import boto3
from datetime import datetime


class ATTDealerCodeRefine(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.dealerCodeIn = sys.argv[2]
        self.dealerCodeOutput = sys.argv[1]
        self.refinedBucket = self.dealerCodeOutput[self.dealerCodeOutput.index('tb'):].split("/")[0]
        self.s3 = boto3.resource('s3')
        self.refinedBucketNode = self.s3.Bucket(name=self.refinedBucket)
        self.prefixAttDealerCodeRefinePath = self.dealerCodeOutput[self.dealerCodeOutput.index('tb'):].split("/")[1]

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
            lastUpdatedFilePath = "s3n://" + bucket + "/" + lastModifiedFileName
            self.log.info("Last Modified " + prefixType + " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefined(self):

        self.sparkSession.read.parquet(self.dealerCodeIn).registerTempTable("AttDealerCode")

        dfDealerCode = self.sparkSession.sql(
            "select a.dealercode,'4' as companycode,a.dcorigin as dealercodeorigin,"
            "a.dfcode,a.dcstatus,case when a.df = 'No' then '0' when a.df = 'Yes' then '1'"
            " when a.df = 'Off' then '0' when a.df = 'False' then '0' when a.df = 'On' then '1' "
            "when a.df = 'True' then '1' when a.df = 'DF' then '1' end as dfindicator,a.candc,a.opendate,a.closedate,"
            "case when a.ws = 'No' then '0' when a.ws = 'Yes' then '1'"
            " when a.ws = 'Off' then '0' when a.ws = 'False' then '0' when a.ws = 'On' then '1' when"
            " a.ws = 'True' then '1' end as whitestoreindicator,a.wsexpires as whitestoreexpirationdate,"
            "a.sortingrank as sortrank,a.rankdescription,a.storeorigin,"
            "a.acquisitionorigin as origin,a.businessexpert,a.footprintlevel,"
            "a.location as attlocationextendedname,a.attlocationid,a.attlocationname,a.disputemkt as attdisputemarket,"
            "a.attmktabbrev as attmarketcode, a.attmarketname as attmarket,"
            "a.oldcode as olddealercode, "
            "a.oldcode2 as olddealercode2,"
            "a.attregion, a.notes, a.notes2 from AttDealerCode a")
        # to change company code to -1 ##

        FinalDF1 = dfDealerCode.na.fill({'companycode': -1, })

        # to filter null value recolds for column delaercodes ##

        FinalDF2 = FinalDF1.where(FinalDF1.dealercode != '')

        # dropping duplicates ##
        FinalDF = FinalDF2.dropDuplicates(['dealercode'])
        previousAttDealerCodeFile = self.findLastModifiedFile(self.refinedBucketNode,
                                                              self.prefixAttDealerCodeRefinePath, self.refinedBucket)
        if previousAttDealerCodeFile != '':
            FinalDF.withColumn("Hash_Column",
                               hash("dealercode", "companycode", "dealercodeorigin", "dfcode", "dcstatus",
                                    "dfindicator", "candc", "opendate", "closedate", "whitestoreindicator",
                                    "whitestoreexpirationdate", "sortrank", "rankdescription", "storeorigin",
                                    "origin", "businessexpert", "footprintlevel", "attlocationextendedname",
                                    "attlocationid", "attlocationname", "attdisputemarket", "attmarketcode",
                                    "attmarket", "olddealercode", "olddealercode2", "attregion", "notes",
                                    "notes2")).registerTempTable("att_dealer_code_curr")

            self.sparkSession.read.parquet(previousAttDealerCodeFile).withColumn("Hash_Column", hash(
                "dealercode", "companycode", "dealercodeorigin", "dfcode", "dcstatus", "dfindicator", "candc",
                "opendate", "closedate", "whitestoreindicator", "whitestoreexpirationdate", "sortrank",
                "rankdescription", "storeorigin", "origin", "businessexpert", "footprintlevel",
                "attlocationextendedname", "attlocationid", "attlocationname", "attdisputemarket", "attmarketcode",
                "attmarket", "olddealercode", "olddealercode2", "attregion", "notes", "notes2")).registerTempTable(
                "att_dealer_code_prev")

            self.sparkSession.sql("select a.dealercode, a.companycode, a.dealercodeorigin, a.dfcode, a.dcstatus, "
                                  "a.dfindicator, a.candc, a.opendate, a.closedate, a.whitestoreindicator, "
                                  "a.whitestoreexpirationdate, a.sortrank, a.rankdescription, a.storeorigin, a.origin, "
                                  "a.businessexpert, a.footprintlevel, a.attlocationextendedname, a.attlocationid, "
                                  "a.attlocationname, a.attdisputemarket, a.attmarketcode, a.attmarket, "
                                  "a.olddealercode, a.olddealercode2, a.attregion, a.notes, a.notes2 from "
                                  "att_dealer_code_prev a left join att_dealer_code_curr b on "
                                  "a.dealercode = b.dealercode where a.Hash_Column = b.Hash_Column").\
                registerTempTable("att_dealer_no_change_data")

            dfAttDealerCodeUpdated = self.sparkSession.sql(
                "select a.dealercode, a.companycode, a.dealercodeorigin, a.dfcode, a.dcstatus, a.dfindicator, a.candc,"
                "a.opendate, a.closedate, a.whitestoreindicator, a.whitestoreexpirationdate, a.sortrank, "
                "a.rankdescription, a.storeorigin, a.origin, a.businessexpert, a.footprintlevel, "
                "a.attlocationextendedname, a.attlocationid, a.attlocationname, a.attdisputemarket, a.attmarketcode,"
                "a.attmarket, a.olddealercode, a.olddealercode2, a.attregion, a.notes, a.notes2 from "
                "att_dealer_code_curr a left join att_dealer_code_prev b on a.dealercode = b.dealercode "
                "where a.Hash_Column <> b.Hash_Column")
            updateRowsCount = dfAttDealerCodeUpdated.count()
            dfAttDealerCodeUpdated.registerTempTable("att_dealer_updated_data")

            dfAttDealerCodeNew = self.sparkSession.sql(
                "select a.dealercode, a.companycode, a.dealercodeorigin, a.dfcode, a.dcstatus, a.dfindicator, a.candc,"
                "a.opendate, a.closedate, a.whitestoreindicator, a.whitestoreexpirationdate, a.sortrank, "
                "a.rankdescription, a.storeorigin, a.origin, a.businessexpert, a.footprintlevel, "
                "a.attlocationextendedname, a.attlocationid, a.attlocationname, a.attdisputemarket, a.attmarketcode,"
                "a.attmarket, a.olddealercode, a.olddealercode2, a.attregion, a.notes, a.notes2 from "
                "att_dealer_code_curr a left join att_dealer_code_prev b on a.dealercode = b.dealercode "
                "where b.dealercode = null")
            newRowsCount = dfAttDealerCodeNew.count()
            dfAttDealerCodeNew.registerTempTable("att_dealer_new_data")

            if updateRowsCount > 0 or newRowsCount > 0:
                self.sparkSession.sql("select * from att_dealer_no_change_data union "
                                      "select * from att_dealer_updated_data union "
                                      "select * from att_dealer_new_data").registerTempTable("att_dealer_cdc")
                self.log.info("Updated file has arrived..")
                FinalDF = self.sparkSession.sql("select dealercode, companycode, dealercodeorigin, dfcode, dcstatus,"
                                                " dfindicator, candc, opendate, closedate, whitestoreindicator, "
                                                "whitestoreexpirationdate, sortrank, rankdescription, storeorigin, "
                                                "origin, businessexpert, footprintlevel, attlocationextendedname, "
                                                "attlocationid, attlocationname, attdisputemarket, attmarketcode, "
                                                "attmarket, olddealercode, olddealercode2, attregion, notes, notes2"
                                                " from att_dealer_cdc")
                FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.dealerCodeOutput + '/' + 'Working')
                # FinalDF.coalesce(1).select("*").write.mode("overwrite").csv(self.dealerCodeOutput + '/csv', header=True)

                FinalDF.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))). \
                    withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).select("*").write.mode(
                    "append").partitionBy('year', 'month').format('parquet').save(self.dealerCodeOutput)
            else:
                FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.dealerCodeOutput + '/' + 'Working')
                # FinalDF.coalesce(1).select("*").write.mode("overwrite").csv(self.dealerCodeOutput + '/csv', header=True)
                FinalDF.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))). \
                    withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).select("*").write.mode(
                    "append"). \
                    partitionBy('year', 'month').format('parquet').save(
                    self.dealerCodeOutput)
                self.log.info("The prev and current files are same. So full file will be generated in refined bucket.")

        else:
            #########################################################################################################
            # write output in parquet file #
            #########################################################################################################
            self.log.info(" This is the first transaformation call, So keeping the file in refined bucket.")
            FinalDF.coalesce(1).select("*").write.mode("overwrite").parquet(self.dealerCodeOutput + '/' + 'Working')
            # FinalDF.coalesce(1).select("*").write.mode("overwrite").csv(self.dealerCodeOutput + '/csv', header=True)
            FinalDF.coalesce(1).withColumn("year", year(from_unixtime(unix_timestamp()))).\
                withColumn("month", substring(from_unixtime(unix_timestamp()), 6, 2)).select("*").write.mode("append").\
                partitionBy('year', 'month').format('parquet').save(
                self.dealerCodeOutput)

        self.sparkSession.stop()


if __name__ == "__main__":
    ATTDealerCodeRefine().loadRefined()
