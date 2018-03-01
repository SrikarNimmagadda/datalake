from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime


class DimStoreManagementHierDelivery(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.refinedBucketWorking = sys.argv[1]
        self.storeMgmtHierCurrentPath = sys.argv[2]

        self.refinedBucket = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[0]

        self.deliveryBucket = self.storeMgmtHierCurrentPath[self.storeMgmtHierCurrentPath.index('tb'):].split("/")[0]
        self.storeMgmtHierDeliveryName = self.storeMgmtHierCurrentPath[
                                         self.storeMgmtHierCurrentPath.index('tb'):].split("/")[1]
        self.currentName = self.storeMgmtHierCurrentPath[self.storeMgmtHierCurrentPath.index('tb'):].split("/")[2]

        self.prefixStoreRefinePath = self.refinedBucketWorking[self.refinedBucketWorking.index('tb'):].split("/")[1]
        self.prefixEmployeeRefinePath = "Employee"
        self.storeHierCurrentPath = "s3://" + self.deliveryBucket + '/WT_STORE_HIER/' + self.currentName

        self.storeMgmtHierPrevPath = "s3://" + self.deliveryBucket + '/' + self.storeMgmtHierDeliveryName + '/Previous'
        self.storeMgmtHierCurrentPath = "s3://" + self.deliveryBucket + '/' + self.storeMgmtHierDeliveryName + '/' + \
                                        self.currentName
        self.storeCSVPath = 's3://' + self.deliveryBucket + '/' + self.storeMgmtHierDeliveryName + '/' + 'csv/store'
        self.empCSVPath = 's3://' + self.deliveryBucket + '/' + self.storeMgmtHierDeliveryName + '/' + 'csv/emp'

        self.storeMgmtHierColumns = "hier_id,lvl_1_src_mgr_id,lvl_2_src_mgr_id,lvl_3_src_mgr_id,lvl_4_src_mgr_id," \
                                    "lvl_5_src_mgr_id,lvl_6_src_mgr_id"
        self.storeMgmtHierColumnsWithAlias = "a.hier_id,a.lvl_1_src_mgr_id,a.lvl_2_src_mgr_id,a.lvl_3_src_mgr_id," \
                                             "a.lvl_4_src_mgr_id,a.lvl_5_src_mgr_id,a.lvl_6_src_mgr_id"
        self.storeMgmtSelectQuery = "select concat(a.SpringMarket,a.SpringRegion,a.SpringDistrict) as hier_id," \
                                    "b.sourceemployeeid as lvl_1_src_mgr_id,c.sourceemployeeid as lvl_2_src_mgr_id," \
                                    "d.sourceemployeeid as lvl_3_src_mgr_id,'' as lvl_4_src_mgr_id,'' as " \
                                    "lvl_5_src_mgr_id,'' as lvl_6_src_mgr_id"
        self.storeMgmtJoinWithCondition = "join employee_curr b on a.SpringRegionVP = b.name join employee_curr c on " \
                                          "a.SpringMarketDirector = c.name join employee_curr d on " \
                                          "a.SpringDistrictManager = d.name"

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

        s3 = boto3.resource('s3')
        refinedBucketNode = s3.Bucket(name=self.refinedBucket)

        lastUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreRefinePath,
                                                         self.refinedBucket)
        lastUpdatedEmployeeFile = self.findLastModifiedFile(refinedBucketNode, self.prefixEmployeeRefinePath,
                                                            self.refinedBucket)

        lastPrevUpdatedStoreFile = self.findLastModifiedFile(refinedBucketNode, self.prefixStoreRefinePath,
                                                             self.refinedBucket, 0)
        self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            option("escape", '"'). \
            option("quote", "\""). \
            option("multiLine", "true"). \
            load(self.storeHierCurrentPath).registerTempTable("store_hier")

        self.sparkSession.read.parquet(lastUpdatedEmployeeFile).registerTempTable("employee_refine_curr")
        dfEmployeeRefined = self.sparkSession.sql(
            "select name,sourceemployeeid from employee_refine_curr where name != ''")
        # dfEmployeeRefined.coalesce(1).write.mode("overwrite").csv(self.empCSVPath, header=True)
        dfEmployeeRefined.registerTempTable("employee_curr")

        if lastPrevUpdatedStoreFile != '':

            self.sparkSession.read.parquet(lastUpdatedStoreFile).registerTempTable("store_refine_curr")
            self.sparkSession.sql("select SpringRegionVP,SpringMarketDirector,SpringDistrictManager,SpringMarket,"
                                  + "SpringRegion,SpringDistrict from store_refine_curr where SpringRegionVP != '' and"
                                  + " SpringMarketDirector != '' and SpringDistrictManager != ''").\
                registerTempTable("store_curr")

            dfStoreMgmtHierCurr = self.sparkSession.sql(self.storeMgmtSelectQuery + " from store_curr a " +
                                                        self.storeMgmtJoinWithCondition)

            self.sparkSession.read.parquet(lastPrevUpdatedStoreFile).registerTempTable("store_refine_prev")
            self.sparkSession.sql("select SpringRegionVP,SpringMarketDirector,SpringDistrictManager,SpringMarket,"
                                  + "SpringRegion,SpringDistrict from store_refine_prev where SpringRegionVP != '' and"
                                  + " SpringMarketDirector != '' and SpringDistrictManager != ''").\
                registerTempTable("store_prev")

            dfStoreMgmtHierPrev = self.sparkSession.sql(self.storeMgmtSelectQuery + " from store_prev a " +
                                                        self.storeMgmtJoinWithCondition)

            dfStoreMgmtHierCurr.subtract(dfStoreMgmtHierPrev).registerTempTable("store_mgmt_hier_delta")
            dfStoreMgmtHierPrev.registerTempTable("store_mgmt_hier_prev")
            dfStoreMgmtHierNew = self.sparkSession.sql(
                "select " + self.storeMgmtHierColumnsWithAlias + ",'I' as cdc_ind_cd from store_mgmt_hier_delta a "
                                                                 + "left join store_mgmt_hier_prev b on a.hier_id = "
                                                                 + "b.hier_id  where b.hier_id is null")
            dfStoreMgmtHierUpdated = self.sparkSession.sql(
                "select " + self.storeMgmtHierColumnsWithAlias + ",'C' as cdc_ind_cd from store_mgmt_hier_delta a "
                                                                 + "left join store_mgmt_hier_prev b on a.hier_id = "
                                                                 + "b.hier_id  where b.hier_id is not null")

            rowCountUpdateRecords = dfStoreMgmtHierUpdated.count()

            dfStoreMgmtHierUpdated.registerTempTable("store_mgmt_hier_updated_data")

            rowCountNewRecords = dfStoreMgmtHierNew.count()

            dfStoreMgmtHierNew.registerTempTable("store_mgmt_hier_new_data")

            if rowCountUpdateRecords > 0 or rowCountNewRecords > 0:
                self.log.info("Updated file has arrived..")
                dfStoreMgmtHierDelta = self.sparkSession.sql(
                        "select " + self.storeMgmtHierColumns +
                        ",cdc_ind_cd from store_mgmt_hier_updated_data union all select " + self.storeMgmtHierColumns +
                        ",cdc_ind_cd from store_mgmt_hier_new_data").drop_duplicates()
                dfStoreMgmtHierDelta.coalesce(1).write.mode("overwrite").csv(self.storeMgmtHierCurrentPath, header=True)
                dfStoreMgmtHierDelta.coalesce(1).write.mode("append").csv(self.storeMgmtHierPrevPath, header=True)
            else:
                self.log.info("The prev and current files are same. No delta file will be generated in refined bucket.")

        else:
            self.log.info(" This is the first transaformation call, So keeping the file in delivery bucket.")
            self.sparkSession.read.parquet(lastUpdatedStoreFile).registerTempTable("store_refine_curr")

            # self.sparkSession.sql("select * from store_refine_curr").coalesce(1).write.mode("overwrite").csv(
            #     self.storeCSVPath, header=True)

            self.sparkSession.sql("select SpringRegionVP,SpringMarketDirector,SpringDistrictManager,"
                                  "SpringMarket,SpringRegion,SpringDistrict from store_refine_curr "
                                  "where SpringRegionVP != '' and SpringMarketDirector != '' and "
                                  "SpringDistrictManager != ''").registerTempTable("store_curr")
            dfStoreManagementHierWithName = self.sparkSession.sql(self.storeMgmtSelectQuery + ",'I' as cdc_ind_cd from"
                                                                                              " store_curr a " +
                                                                  self.storeMgmtJoinWithCondition).drop_duplicates()

            dfStoreManagementHierWithName.coalesce(1).write.mode("overwrite").csv(self.storeMgmtHierCurrentPath,
                                                                                  header=True)
            dfStoreManagementHierWithName.coalesce(1).write.mode("append").csv(self.storeMgmtHierPrevPath, header=True)

        self.sparkSession.stop()


if __name__ == "__main__":
    DimStoreManagementHierDelivery().loadDelivery()
