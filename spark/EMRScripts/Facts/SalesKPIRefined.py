from __future__ import print_function
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType, DoubleType
from pyspark.sql.functions import col, lit
import time
import sys
from pyspark.storagelevel import StorageLevel


class SalesKPI(object):
    def __init__(self):
        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.logger = self.log4jLogger.LogManager.getLogger(self.appName)
        self.salesDetailInp = sys.argv[1]
        self.productInp = sys.argv[2]
        self.productCategoryInp = sys.argv[3]
        self.storeTransAdjustmentInp = sys.argv[4]
        self.storeInp = sys.argv[5]
        self.attSalesActualInp = sys.argv[6]
        self.storeTrafficInp = sys.argv[7]
        self.storeRecruitingHeadcountInp = sys.argv[8]
        self.employeeGoalInp = sys.argv[9]
        self.employeeMasterInp = sys.argv[10]
        self.empStoreAssociationInp = sys.argv[11]
        self.salesLeadInp = sys.argv[12]
        self.storeCustomerExpInp = sys.argv[13]
        self.kpiList = sys.argv[14]
        self.kpiOutput = sys.argv[15]
        sqlContext = SQLContext(self.sparkSession)
        sqlContext.registerFunction("getDateS", lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
        sqlContext.registerFunction("getDateH", lambda x: datetime.strptime(x, '%m-%d-%y'), DateType())
        sqlContext.registerFunction("doubleToInt", lambda x: IntegerType(x), IntegerType())
        sqlContext.registerFunction("getOnlyDate", lambda x: time.strftime("%m-%d-%Y", time.strptime(x[:19], "%Y-%m-%dT%H:%M:%S")))
        sqlContext.registerFunction("correctFormatDate", lambda x: time.strftime("%m/%d/%Y", time.strptime(x[:10], "%m/%d/%Y")))
        sqlContext.registerFunction("getOnlyDate2", lambda x: time.strftime("%m/%d/%Y", time.strptime(x[:15], "%m/%d/%y %H:%M")))

    def loadParquet(self):
        dfSalesDetail = self.sparkSession.read.parquet(self.salesDetailInp)
        dfProduct = self.sparkSession.read.parquet(self.productInp)
        dfProductCategory = self.sparkSession.read.parquet(self.productCategoryInp)
        dfStoreTransAdjustment = self.sparkSession.read.parquet(self.storeTransAdjustmentInp)
        dfStore = self.sparkSession.read.parquet(self.storeInp)
        dfATTSalesActual = self.sparkSession.read.parquet(self.attSalesActualInp)
        dfStoreTraffic = self.sparkSession.read.parquet(self.storeTrafficInp)
        dfStoreRecruitingHeadcount = self.sparkSession.read.parquet(self.storeRecruitingHeadcountInp)
        dfEmployeeGoal = self.sparkSession.read.parquet(self.employeeGoalInp)
        dfEmployeeMaster = self.sparkSession.read.parquet(self.employeeMasterInp)
        dfEmpStoreAssociation = self.sparkSession.read.parquet(self.empStoreAssociationInp)
        dfSalesLead = self.sparkSession.read.parquet(self.salesLeadInp)
        dfStoreCustomerExp = self.sparkSession.read.parquet(self.storeCustomerExpInp)

        dfKpilist = self.sparkSession.read.format("com.crealytics.spark.excel").option("location", self.kpiList).option("sheetName", "KPIs for DL-Calculation").option("treatEmptyValuesAsNulls", "true").option("addColorColumns", "false").option("inferSchema", "true").option("spark.read.simpleMode", "true").option("useHeader", "true").load("com.databricks.self.sparkSession.csv")

        #   Read the source files   #
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

        lvl0KpiDict = {}
        lvl1KpiDict = {}
        lvl2KpiDict = {}
        schema = StructType([StructField('storenumber', StringType(), True), StructField('sourceemployeeid', IntegerType(), True), StructField('kpiname', StringType(), False), StructField('kpivalue', DoubleType(), True), StructField('report_date', DateType(), False), StructField('companycd', IntegerType(), False)])

        dfKpi = self.sparkSession.createDataFrame(self.sparkSession.sparkContext.emptyRDD(), schema)
        dfKpilistlev0 = dfKpilist.filter(col('CalculationLevel') == 0)

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

            df = self.sparkSession.sql(sqlstring)
            df = df.withColumn('companycd', lit(companycd))
            df = df.withColumnRenamed("getOnlyDate(invoicedate)", "report_date")
            dfKpi.registerTempTable("dfKpi")
            dfKpi = self.sparkSession.sql("select sourceemployeeid, storenumber, report_date, kpiname, kpivalue, companycd from dfKpi")
            dfKpi = dfKpi.union(df)
            lvl0KpiDict[kpiheader] = df

            #   LEVEL 1 KPI code starts here   #

            dfKpilistlev1 = dfKpilist.filter(col('CalculationLevel') == 1)

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
                lvl0KpiDict[headerlist[j]].registerTempTable(headerlist[j + 1])
                j = j + 2
            words = tablename.split(' ')
            for word in words:
                doclist.append(word)

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

            df = self.sparkSession.sql(sqlstring)
            df = df.withColumn('companycd', lit(companycd))
            dfKpi = dfKpi.union(df)
            lvl1KpiDict[kpiheader] = df

        #         Logic starts for KPI level2         #
        dfKpilistlev2 = dfKpilist.filter(col('CalculationLevel') == 2)

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

                if headerlist[j] in lvl1KpiDict:
                    lvl1KpiDict[headerlist[j]].registerTempTable(headerlist[j + 1])
                else:
                        lvl0KpiDict[headerlist[j]].registerTempTable(headerlist[j + 1])
                j = j + 2
            words = tablename.split(' ')
            for word in words:
                doclist.append(word)

            sqlstring = ""
            if filtercondition:
                sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                            + "	 as kpivalue from " \
                            + tablename + "  " + filtercondition + " group by " + groupbycolumn
            else:
                sqlstring = "select " + groupbycolumn + ",'" + kpiheader + "' as kpiname, " + expression  \
                            + " as kpivalue from " \
                            + tablename + " group by " + groupbycolumn

                df = self.sparkSession.sql(sqlstring)
                df = df.withColumn('companycd', lit(companycd))
                dfKpi = dfKpi.union(df)
                lvl2KpiDict[kpiheader] = df

        dfKpi = dfKpi.na.drop(subset=["kpivalue"])

        dfKpi.registerTempTable("dfKpi2")
        dfKpi = self.sparkSession.sql("select sourceemployeeid, storenumber, report_date, kpiname, kpivalue, companycd from dfKpi2")
        dfKpi.persist(StorageLevel.MEMORY_ONLY)
        dfKpi.rdd.getNumPartitions()
        dfKpi.coalesce(1).select("*").write.mode("overwrite").parquet(self.kpiOutput + '/' + 'Working')
        dfKpi.coalesce(1).select("*").write.mode("append").parquet(self.kpiOutput + '/' + 'Previous')
        self.self.sparkSessionSession.stop()


if __name__ == "__main__":
    SalesKPI().loadParquet()
