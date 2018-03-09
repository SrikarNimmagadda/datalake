from pyspark.sql import SparkSession
import sys


class ProductCategoryCSVToParquet(object):

    def __init__(self):

        self.outputPath = sys.argv[1]
        self.prodCatInputPath = sys.argv[2]

        self.appName = self.__class__.__name__
        self.sparkSession = SparkSession.builder.appName(self.appName).getOrCreate()
        self.log4jLogger = self.sparkSession.sparkContext._jvm.org.apache.log4j
        self.log = self.log4jLogger.LogManager.getLogger(self.appName)
        self.fileHasDataFlag = 0

    def loadParquet(self):
        dfProductCatg = self.sparkSession.read.format("com.databricks.spark.csv"). \
            option("header", "true"). \
            option("treatEmptyValuesAsNulls", "true"). \
            option("inferSchema", "true"). \
            load(self.prodCatInputPath)

        dfProductCatg = dfProductCatg.withColumnRenamed("ID", "id"). \
            withColumnRenamed("Description", "description"). \
            withColumnRenamed("CategoryPath", "categorypath"). \
            withColumnRenamed("ParentID", "parentid"). \
            withColumnRenamed("Enabled", "enabled")

        if dfProductCatg.count() > 1:
            print("The product category file has data")
            fileHasDataFlag = 1
        else:
            print("The product category file does not have data")
            fileHasDataFlag = 0

        if fileHasDataFlag == 1:
            print("Csv file loaded into dataframe properly")

            dfProductCatg.dropDuplicates(['id']).registerTempTable("ProdCatTempTable")

            dfProductCatgFinal = self.sparkSession.sql("select cast(id as string),description,categorypath,parentid,enabled,"
                                                       " YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) as year,"
                                                       "SUBSTR(FROM_UNIXTIME(UNIX_TIMESTAMP()),6,2) as month from ProdCatTempTable  "
                                                       "where id is not null")

            dfProductCatgFinal.coalesce(1). \
                select('id', 'description', 'categorypath', 'parentid', 'enabled'). \
                write.mode("overwrite"). \
                format('parquet'). \
                save(self.outputPath + '/' + 'ProductCategory' + '/' + 'Working')

            dfProductCatgFinal.coalesce(1). \
                write.mode('append').partitionBy('year', 'month'). \
                format('parquet').\
                save(self.outputPath + '/' + 'ProductCategory')

        else:
            print("ERROR : Loading csv file into dataframe")

        self.sparkSession.stop()


if __name__ == "__main__":
    ProductCategoryCSVToParquet().loadParquet()
