from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
from datetime import datetime
import collections
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import Row, functions as F


DimStoreRefined = sys.argv[1]
ATTDealerCode = sys.argv[2]
DimTechBrandHierarchy = sys.argv[3]


# Create a SparkSession (Note, the config section is only for Windows!)

spark = SparkSession.builder.\
        appName("LocationStore").getOrCreate()
        
#########################################################################################################
#                                 Reading the source data files                                         #
#########################################################################################################

DimStoreRefined_DF = spark.read.parquet(DimStoreRefined).registerTempTable("DimStoreRefinedTT")
		
ATTDealerCode_DF = spark.read.parquet(ATTDealerCode).registerTempTable("ATTDealerCodeTT")
        


#########################################################################################################
#                                 Spark Transformation begins here                                      #
#########################################################################################################


joined_DF = spark.sql("select distinct 'ATT Hierrarchy' HierarchyName, ATTRegion Level_1_Code, ATTRegion Level_1_Name, ATTMktAbbrev Level_2_Code, "
                    + "ATTMarketName Level_2_Name, ' ' Level_3_Code, ' ' Level_3_Name, ' ' Level_4_Code, ' ' Level_4_Name, ' ' Level_5_Code, "
                    + "' ' Level_5_Name, ' ' Level_6_Code, ' ' Level_6_Name from ATTDealerCodeTT "
                    + "union "
                    + "select distinct 'Store Hierarchy' HierarchyName, Market Level_1_Code, Market Level_1_Name, Region Level_2_Code, Region Level_2_Name, "
                    + "District Level_3_Code, District Level_3_Name, ' ' Level_4_Code, ' ' Level_4_Name, ' ' Level_5_Code, ' ' Level_5_Name, "
                    + "' ' Level_6_Code, ' ' Level_6_Name from DimStoreRefinedTT")
    				
final_DF = joined_DF.select(F.row_number()
         .over(Window
              # .partitionBy("HierarchyName")
               .orderBy("HierarchyName")
              )
         .alias("Hierarchy_Id"), "*")                
  
final_DF.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(DimTechBrandHierarchy)

spark.stop()