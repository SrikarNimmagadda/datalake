from __future__ import print_function
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType, DoubleType
from pyspark.sql.functions import col, lit
import time
import sys
from pyspark.storagelevel import StorageLevel

SalesDetailInp = sys.argv[1]
ProductInp = sys.argv[2]
ProductCategoryInp = sys.argv[3]
StoreTransAdjustmentInp = sys.argv[4]
StoreInp = sys.argv[5]
ATTSalesActualInp = sys.argv[6]
StoreTrafficInp = sys.argv[7]
StoreRecruitingHeadcountInp = sys.argv[8]
EmployeeGoalInp = sys.argv[9]
EmployeeMasterInp = sys.argv[10]
EmpStoreAssociationInp = sys.argv[11]
SalesLeadInp = sys.argv[12]
StoreCustomerExpInp = sys.argv[13]
KPIList = sys.argv[14]
KPIOutput = sys.argv[15]

Config = SparkConf().setAppName("SalesKPI")
spark = SparkSession.builder.config(conf=Config).getOrCreate()
sqlContext = SQLContext(spark)

sqlContext.registerFunction("getDateS", lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
sqlContext.registerFunction("getDateH", lambda x: datetime.strptime(x, '%m-%d-%y'), DateType())
sqlContext.registerFunction("doubleToInt", lambda x: IntegerType(x), IntegerType())
sqlContext.registerFunction("getOnlyDate", lambda x: time.strftime("%m-%d-%Y", time.strptime(x[:19], "%Y-%m-%d %H:%M:%S")))
sqlContext.registerFunction("correctFormatDate", lambda x: time.strftime("%m/%d/%Y", time.strptime(x[:10], "%m/%d/%Y")))
sqlContext.registerFunction("getOnlyDate2", lambda x: time.strftime("%m/%d/%Y", time.strptime(x[:15], "%m/%d/%y %H:%M")))

dfSalesDetail = spark.read.parquet(SalesDetailInp)
dfProduct = spark.read.parquet(ProductInp)
dfProductCategory = spark.read.parquet(ProductCategoryInp)
dfStoreTransAdjustment = spark.read.parquet(StoreTransAdjustmentInp)
dfStore = spark.read.parquet(StoreInp)
dfATTSalesActual = spark.read.parquet(ATTSalesActualInp)
dfStoreTraffic = spark.read.parquet(StoreTrafficInp)
dfStoreRecruitingHeadcount = spark.read.parquet(StoreRecruitingHeadcountInp)
dfEmployeeGoal = spark.read.parquet(EmployeeGoalInp)
dfEmployeeMaster = spark.read.parquet(EmployeeMasterInp)
dfEmpStoreAssociation = spark.read.parquet(EmpStoreAssociationInp)
dfSalesLead = spark.read.parquet(SalesLeadInp)
dfStoreCustomerExp = spark.read.parquet(StoreCustomerExpInp)

dfKpilist = spark.read.format("com.crealytics.spark.excel").option("location", KPIList).option("sheetName", "KPIs for DL-Calculation").option("treatEmptyValuesAsNulls", "true").option("addColorColumns", "false").option("inferSchema", "true").option("spark.read.simpleMode", "true").option("useHeader", "true").load("com.databricks.spark.csv")

dfSalesDetail.registerTempTable("SalesDetails")
dfProduct.registerTempTable("Product")
dfProductCategory.registerTempTable("ProductCategories")
dfStoreTransAdjustment.registerTempTable("StoreTransactionAdjustments")
dfStore.registerTempTable("Store")
dfATTSalesActual.registerTempTable("ATTSalesActuals")
dfStoreTraffic.registerTempTable("StoreTraffic")
dfStoreRecruitingHeadcount.registerTempTable("StoreRecruitingHeadcount")
dfEmployeeMaster.registerTempTable("Employee")
dfEmpStoreAssociation.registerTempTable("EmployeeStoreAssociation")
dfSalesLead.registerTempTable("SalesLeads")
dfStoreCustomerExp.registerTempTable("StoreCustomerExperience")
dfEmployeeGoal.registerTempTable("EmployeeGoal")

Lev0KPIDict = {}
Lev1KPIDict = {}
Lev2KPIDict = {}

schema = StructType([StructField('storenumber', StringType(), True), StructField('sourceemployeeid', IntegerType(), True), StructField('kpiname', StringType(), False), StructField('kpivalue', DoubleType(), True), StructField('report_date', DateType(), False), StructField('companycd', IntegerType(), False)])

dfKPI = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
dfKpilistlev0 = dfKpilist.filter(col('CalculationLevel') == 0)
rowcount = dfKpilistlev0.count()

todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')
listvalue = dfKpilistlev0.rdd
list1 = listvalue.take(listvalue.count())
i = 0
for value in list1:
    i = i + 1
    doclist = []
    kpiheader = value.KPIName
    expression = value.Expression
    filtercondition = value.FilterCondition
    tablename = value.FromClause
    groupbycolumn = value.GroupByColumns
    companycd = value.Companycd
    words = tablename.split(' ')
    for word in words:
        doclist.append(word)

    firsttablename = doclist[0]
    sqlstring = ""
    if filtercondition:
        if groupbycolumn:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + "  " + filtercondition + " group by " + groupbycolumn
        else:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + "  " + filtercondition

    else:
        if groupbycolumn:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + " group by " + groupbycolumn
        else:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename

    df = spark.sql(sqlstring)
    df = df.withColumn('companycd', lit(companycd))
    df = df.withColumnRenamed("getOnlyDate(invoicedate)", "report_date")
    dfKPI.registerTempTable("DFKPI")
    dfKPI = spark.sql("select sourceemployeeid, storenumber, report_date, kpiname, kpivalue, companycd from DFKPI")
    dfKPI = dfKPI.union(df)
    Lev0KPIDict[kpiheader] = df

dfKpilistlev1 = dfKpilist.filter(col('CalculationLevel') == 1)
lev1rowcount = dfKpilistlev1.count()

listlv1value = dfKpilistlev1.rdd
listlv1 = listlv1value.take(listlv1value.count())

for value in listlv1:
    i = i + 1
    doclist = []
    headerlist = []
    kpiheader = value.KPIName
    expression = value.Expression
    filtercondition = value.FilterCondition
    tablename = value.FromClause
    groupbycolumn = value.GroupByColumns
    lev1headername = value.LveTableName
    wordsplit = lev1headername.split(',')
    j = 0
    for word in wordsplit:
        word1 = word.split('=')
        for word2 in word1:
            headerlist.append(word2)
        Lev0KPIDict[headerlist[j]].registerTempTable(headerlist[j + 1])
        j = j + 2
    words = tablename.split(' ')
    for word in words:
        doclist.append(word)

    firsttablename = doclist[0]
    sqlstring = ""
    if filtercondition:
        if groupbycolumn:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + "  " + filtercondition + " group by " + groupbycolumn
        else:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + "  " + filtercondition

    else:
        if groupbycolumn:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename + " group by " + groupbycolumn
        else:
            sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                + " as kpivalue from " \
                + tablename

    df = spark.sql(sqlstring)
    df = df.withColumn('companycd', lit(companycd))
    dfKPI = dfKPI.union(df)
    Lev1KPIDict[kpiheader] = df

dfKpilistlev2 = dfKpilist.filter(col('CalculationLevel') == 2)
lev1rowcount = dfKpilistlev2.count()

listlv2value = dfKpilistlev2.rdd
listlv2 = listlv2value.take(listlv2value.count())
for value in listlv2:
    i = i + 1
    doclist = []
    headerlist = []
    kpiheader = value.KPIName
    expression = value.Expression
    filtercondition = value.FilterCondition
    tablename = value.FromClause
    groupbycolumn = value.GroupByColumns
    lev1headername = value.LveTableName
    wordsplit = lev1headername.split(',')
    j = 0
    for word in wordsplit:
        word1 = word.split('=')
        for word2 in word1:
            headerlist.append(word2)

        if headerlist[j] in Lev1KPIDict:
            Lev1KPIDict[headerlist[j]].registerTempTable(headerlist[j + 1])
        else:
            Lev0KPIDict[headerlist[j]].registerTempTable(headerlist[j + 1])

        j = j + 2
    words = tablename.split(' ')
    for word in words:
        doclist.append(word)

    firsttablename = doclist[0]

    sqlstring = ""
    if filtercondition:
        sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                    + " as kpivalue from " \
                    + tablename + "  " + filtercondition + " group by " + groupbycolumn

    else:
        sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                    + " as kpivalue from " \
                    + tablename + " group by " + groupbycolumn

        df = spark.sql(sqlstring)
        df = df.withColumn('companycd', lit(companycd))
        dfKPI = dfKPI.union(df)
        Lev2KPIDict[kpiheader] = df

dfKPI = dfKPI.na.drop(subset=["kpivalue"])

dfKPI.registerTempTable("DFKPI2")
dfKPI = spark.sql("select sourceemployeeid,storenumber,report_date,kpiname,kpivalue,companycd from DFKPI2")

dfKPI.persist(StorageLevel.MEMORY_ONLY)
dfKPI.rdd.getNumPartitions()
dfKPI.coalesce(1).select("*").write.mode("overwrite").parquet(KPIOutput + '/' + 'Working')
dfKPI.coalesce(1).select("*").write.mode("append").parquet(KPIOutput + '/' + 'Previous')

spark.stop()
