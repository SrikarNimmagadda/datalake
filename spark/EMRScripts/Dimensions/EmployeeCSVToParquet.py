from pyspark.sql import SparkSession
import sys

EmployeeCust = sys.argv[1]
EmpOutputArg = sys.argv[2]

spark = SparkSession.builder.\
    appName("ParquetCustomeEmployeeFile").getOrCreate()


dfEmployee = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    option("quote", "\"").\
    option("multiLine", "true").\
    option("spark.read.simpleMode", "true").\
    option("useHeader", "true").\
    load(EmployeeCust)


dfEmployee = dfEmployee.\
    withColumnRenamed("UserManagerEmployeeID", "usermanageremployeeid").\
    withColumnRenamed("EmployeeID", "employeeid").\
    withColumnRenamed("EmployeeName", "employeename").\
    withColumnRenamed("Username", "username").\
    withColumnRenamed("SpecialIdentifier", "specialidentifier").\
    withColumnRenamed("Title", "title").\
    withColumnRenamed("Supervisor", "supervisor").\
    withColumnRenamed("Disabled", "disabled").\
    withColumnRenamed("Locked", "locked").\
    withColumnRenamed("Phone", "phone").\
    withColumnRenamed("Phone.Cellular", "phonecellular").\
    withColumnRenamed("Email.Personal", "emailpersonal").\
    withColumnRenamed("Email", "email").\
    withColumnRenamed("Email.Work.DisplayName", "emailworkdisplayname").\
    withColumnRenamed("Email.Work.SMTPServer", "emailworksmtpserver").\
    withColumnRenamed("Email.Work.Username", "emailworkusername").\
    withColumnRenamed("Address", "address").\
    withColumnRenamed("Address2", "address2").\
    withColumnRenamed("City", "city").\
    withColumnRenamed("StateProvince", "stateprovince").\
    withColumnRenamed("PostalCode", "postalcode").\
    withColumnRenamed("Country", "country").\
    withColumnRenamed("Photo", "photo").\
    withColumnRenamed("Fingerprint", "fingerprint").\
    withColumnRenamed("SecurityRole", "securityrole").\
    withColumnRenamed("PrimaryLocation", "primarylocation").\
    withColumnRenamed("Gender", "gender").\
    withColumnRenamed("AssignedLocations", "assignedlocations").\
    withColumnRenamed("AssignedGroups", "assignedgroups").\
    withColumnRenamed("LastHireDate", "lasthiredate").\
    withColumnRenamed("StartDate", "startdate").\
    withColumnRenamed("TerminationDate", "terminationdate").\
    withColumnRenamed("ScheduledTerminationDate", "scheduledterminationdate").\
    withColumnRenamed("TerminationNotes", "terminationnotes").\
    withColumnRenamed("Compensation Type", "compensationtype").\
    withColumnRenamed("Rate", "rate").\
    withColumnRenamed("Internal", "internal").\
    withColumnRenamed("RateEffective", "rateeffective").\
    withColumnRenamed("Commission Group", "commissiongroup").\
    withColumnRenamed("Frequency", "frequency").\
    withColumnRenamed("FrequencyEffective", "frequencyeffective").\
    withColumnRenamed("PartTime", "parttime").\
    withColumnRenamed("VacationDaysAvailable", "vacationdaysavailable").\
    withColumnRenamed("SickDaysAvailable", "sickdaysavailable").\
    withColumnRenamed("PersonalDaysAvailable", "personaldaysavailable").\
    withColumnRenamed("Language", "language").\
    withColumnRenamed("IntrgrationUsername", "intrgrationusername").\
    withColumnRenamed("RowThumbprint", "rowthumbprint").\
    withColumnRenamed("RowInserted", "rowinserted").\
    withColumnRenamed("RowUpdated", "rowupdated").\
    withColumnRenamed("RowEvent\r", "rowevent")

dfEmployee.registerTempTable("employee")
dfEmployee_Write = spark.sql("select *," +
                             "YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) " +
                             "as year," +
                             "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) " +
                             "as month from employee")

dfEmployee_Write.coalesce(1).select("*"). \
    write.option("header", "false").mode("overwrite").\
    parquet(EmpOutputArg + '/' + 'Working')

# Writing data to partitioned Directory

dfEmployee_Write.coalesce(1).write.mode('append').partitionBy(
    'year', 'month').format('parquet').save(EmpOutputArg)


spark.stop()
