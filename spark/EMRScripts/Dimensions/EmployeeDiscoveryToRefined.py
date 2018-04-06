from pyspark.sql import SparkSession
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import DateType


class EmployeeRefine(object):

    def __init__(self):

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.\
            appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.\
            sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)

        self.discoveryBucketWorking = sys.argv[1]
        self.discoveryBucket = self.\
            discoveryBucketWorking[self.
                                   discoveryBucketWorking.index(
                                            'tb'):].split("/")[0]
        self.refinedBucketWorking = sys.argv[2]
        self.refinedBucket = self.\
            refinedBucketWorking[self.
                                 refinedBucketWorking.index(
                                    'tb'):].split("/")[0]
        self.Employee = self.\
            refinedBucketWorking[self.
                                 refinedBucketWorking.index(
                                    'tb'):].split("/")[1]
        self.workingName = self.\
            refinedBucketWorking[self.
                                 refinedBucketWorking.index(
                                     'tb'):].split("/")[2]

        self.EmployeeWorkingPath = 's3://' + self.refinedBucket +\
            '/' + self.Employee + '/' + self.workingName
        self.EmployeePartitonPath = 's3://' + self.refinedBucket +\
            '/' + self.Employee
        self.prefixEmployeeDiscoveryPath = 'Employee'
        self.prefixStoreAssocPath = self.Employee

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
        revSortedFiles = sorted(req_values_dict,
                                key=req_values_dict.get, reverse=True)

        numFiles = len(revSortedFiles)
        self.log.info("Number of part files is : " + str(numFiles))
        lastUpdatedFilePath = ''

        if numFiles > 0:
            lastModifiedFileName = str(revSortedFiles[0])
            lastUpdatedFilePath = "s3n://" + bucket + "/" +\
                lastModifiedFileName
            self.log.info("Last Modified " + prefixType +
                          " file in s3 format is : " + lastUpdatedFilePath)
        return lastUpdatedFilePath

    def loadRefine(self):

        s3 = boto3.resource('s3')
        discoveryBucketNode = s3.Bucket(name=self.discoveryBucket)

        lastUpdatedEmployeeFile = self.\
            findLastModifiedFile(discoveryBucketNode,
                                 self.prefixEmployeeDiscoveryPath,
                                 self.discoveryBucket)
        dfEmployeeData = self.\
            sparkSession.read.parquet(lastUpdatedEmployeeFile)

        dfEmployeeData.registerTempTable("employee")

        dfEmployeeprevious = \
            self.sparkSession.sql("select " +
                                  "a.employeeid as sourceemployeeid, " +
                                  "4 as companycd, 'RQ4' as " +
                                  "sourcesystemname," +
                                  "a.specialidentifier as workdayid," +
                                  "a.employeename as name," +
                                  "a.gender, a.emailpersonal as workemail," +
                                  "CASE WHEN a.disabled IN ('TRUE','Yes',1) " +
                                  "THEN 0 ELSE 1 END " +
                                  "as statusindicator," +
                                  "CASE WHEN a.parttime IN('TRUE','Yes',1) " +
                                  "THEN 1 ELSE 0 END " +
                                  "as parttimeindicator," +
                                  "a.title, CASE WHEN a.language = " +
                                  "'en-us' THEN 'English' ELSE a.language " +
                                  "END as language, a.securityrole as " +
                                  "jobrole," +
                                  "a.city, a.stateprovince, a.country," +
                                  "a.usermanageremployeeid as " +
                                  "employeemanagerid," +
                                  " a.supervisor as employeemanagername," +
                                  " a.startdate," +
                                  "CASE WHEN a.terminationdate = 'TODAY' " +
                                  "THEN '' ELSE " +
                                  "a.terminationdate END as terminationdate," +
                                  "a.commissiongroup, a.compensationtype," +
                                  "a.username, a.phone as worknumber, " +
                                  "a.email as email," +
                                  "CASE WHEN a.rowevent = 'Updated' " +
                                  "THEN 'C' ELSE 'I' END " +
                                  "as CDC_IND_CD," +
                                  "a.rowinserted as datecreatedatsource, " +
                                  "a.rowupdated as employeelastmodifieddate," +
                                  "a.assignedlocations," +
                                  " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                                  "as year, SUBSTR(FROM_UNIXTIME " +
                                  "(UNIX_TIMESTAMP()),6,2) " +
                                  "as month " +
                                  "from employee a")
        # dfEmployeeprevious.show()
        dfEmployeeprevious.registerTempTable("employeehist")
        # test = self.sparkSession.sql("select count (*) from employeehist")
        # test.show()
        UpdatedEmployeeFile = self.discoveryBucketWorking
        dfUpdatedEmployeeData = self.sparkSession.\
            read.parquet(UpdatedEmployeeFile)
        dfUpdatedEmployeeData.registerTempTable("employee1")

        dfEmployeeUpdated = \
            self.sparkSession.sql("select " +
                                  "a.employeeid as sourceemployeeid, " +
                                  "4 as companycd, 'RQ4' as " +
                                  "sourcesystemname," +
                                  "a.specialidentifier as workdayid," +
                                  "a.employeename as name," +
                                  "a.gender, a.emailpersonal as workemail," +
                                  "CASE WHEN a.disabled IN ('TRUE','Yes',1) " +
                                  "THEN 0 ELSE 1 END " +
                                  "as statusindicator," +
                                  "CASE WHEN a.parttime IN('TRUE','Yes',1) " +
                                  "THEN 1 ELSE 0 END " +
                                  "as parttimeindicator," +
                                  "a.title, CASE WHEN a.language = " +
                                  "'en-us' THEN 'English' ELSE a.language " +
                                  "END as language, a.securityrole as " +
                                  "jobrole," +
                                  "a.city, a.stateprovince, a.country," +
                                  "a.usermanageremployeeid as " +
                                  "employeemanagerid," +
                                  " a.supervisor as employeemanagername," +
                                  " a.startdate," +
                                  "CASE WHEN a.terminationdate = 'TODAY' " +
                                  "THEN '' ELSE " +
                                  "a.terminationdate END as terminationdate," +
                                  "a.commissiongroup, a.compensationtype," +
                                  "a.username, a.phone as worknumber, " +
                                  "a.email as email," +
                                  "CASE WHEN a.rowevent = 'Updated' " +
                                  "THEN 'C' ELSE 'I' END " +
                                  "as CDC_IND_CD," +
                                  "a.rowinserted as datecreatedatsource, " +
                                  "a.rowupdated as employeelastmodifieddate," +
                                  "a.assignedlocations," +
                                  " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                                  "as year, SUBSTR(FROM_UNIXTIME " +
                                  "(UNIX_TIMESTAMP()),6,2) " +
                                  "as month " +
                                  "from employee1 a")
#       dfEmployeeUpdated.show()
        dfEmployeeUpdated.registerTempTable("employeecurrent")
        joined_DF = self.sparkSession.sql(
            "select * from employeehist union select * from employeecurrent")
        joined_DF.registerTempTable("employeeuninon")

        dfEmployee = \
            self.sparkSession.sql("select a.* from employeeuninon " +
                                  " a INNER JOIN (select " +
                                  "sourceemployeeid, " +
                                  "max(employeelastmodifieddate) as " +
                                  "value from employeeuninon " +
                                  "group by sourceemployeeid) as " +
                                  "b on a.sourceemployeeid= " +
                                  "b.sourceemployeeid and " +
                                  "a.employeelastmodifieddate=b.value")
        # dfEmployee.registerTempTable("emp")
        # dfEmployee1= self.sparkSession.sql( "select count(*) from emp")
        # dfEmployee1.show()

        dfEmployee = dfEmployee.\
            withColumn("startdate", dfEmployee["startdate"].cast(DateType()))
        dfEmployee = dfEmployee.withColumn(
            "terminationdate", dfEmployee["terminationdate"].cast(DateType()))
        dfEmployee = dfEmployee.where(col("sourceemployeeid").isNotNull())
        dfEmployee = dfEmployee.where(col("companycd").isNotNull())
        dfEmployee = dfEmployee.where(col("sourcesystemname").isNotNull())

        dfEmployee.coalesce(1).select("*"). \
            write.mode("overwrite").parquet(self.refinedBucketWorking)

        dfEmployee.coalesce(1).write.mode('append').partitionBy(
            'year', 'month').format('parquet').save(self.EmployeePartitonPath)

        self.sparkSession.stop()


if __name__ == "__main__":
    EmployeeRefine().loadRefine()
