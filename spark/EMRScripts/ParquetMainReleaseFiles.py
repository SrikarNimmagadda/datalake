from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf

SpringCommCust = sys.argv[1]
EmployeeCust = sys.argv[2]
CompanyOutputArg = sys.argv[3]
CompanyFileTime = sys.argv[4]

spark = SparkSession.builder.\
        appName("DealerCodeRefinary").getOrCreate()
        
'''
SpringCommCust_Schema = StructType() .\
                        add("CustomerID", IntegerType(), True). \
                        add("CustomerFirstName", StringType(), True). \
                        add("CustomerLastName", StringType(), True). \
                        add("CustomerCompanyName", StringType(), True). \
                        add("DateCreated", StringType(), True). \
                        add("ContactNumber", IntegerType(), True). \
                        add("Email", StringType(), True). \
                        add("EmployeeName", StringType(), True). \
                        add("EmployeeNameAssignedTo", StringType(), True). \
                        add("Address", StringType(), True). \
                        add("Address2", StringType(), True). \
                        add("City", StringType(), True). \
                        add("Province", StringType(), True). \
                        add("PostalCode", StringType(), True). \
                        add("Country", StringType(), True). \
                        add("VIPCustomer", StringType(), True). \
                        add("DeclineToProvideEmail", StringType(), True).\
                        add("TracPointMemberNumber", StringType(), True). \
                        add("TypeOfCustomer", StringType(), True). \
                        add("ContactTypeName", StringType(), True). \
                        add("IndustryTypeName", StringType(), True). \
                        add("PositionTypeName", StringType(), True). \
                        add("MultiLevelPriceID", IntegerType(), True). \
                        add("BillingAccountNumber", StringType(), True). \
                        add("NumberOfActivations", IntegerType(), True). \
                        add("LastDateModified", StringType(), True). \
                        add("DivisionID", StringType(), True). \
                        add("AccessedDate", StringType(), True)
'''                        

dfSpringCommCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        option("quote", "\"").\
                        option("multiLine", "true").\
                        option("spark.read.simpleMode","true").\
                        option("useHeader", "true").\
                        load(SpringCommCust) 
                        
#option("delimiter", ",").\
'''     
dfSpringCommCust = spark.read.format("com.crealytics.spark.excel").\
                option("location", SpringCommCust).\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(SpringCommCust_Schema). \
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")
'''                
###############Rename columns of customer#######################                
                
dfSpringCommCust = dfSpringCommCust.withColumnRenamed("CustomerID", "customeridentifier").\
                withColumnRenamed("CustomerFirstName", "firstname").\
                withColumnRenamed("CustomerLastName", "lastName").\
                withColumnRenamed("CustomerCompanyName", "companyname").\
                withColumnRenamed("ContactNumber", "phone").\
                withColumnRenamed("Email", "email").\
                withColumnRenamed("Address", "address1").\
                withColumnRenamed("Address2", "address2").\
                withColumnRenamed("City", "city").\
                withColumnRenamed("Province", "stateprovince").\
                withColumnRenamed("PostalCode", "zippostalcode").\
                withColumnRenamed("Country", "country").\
                withColumnRenamed("MultiLevelPriceID", "multilevelpriceidentifier").\
                withColumnRenamed("BillingAccountNumber", "billingaccountnumber").\
                withColumnRenamed("NumberOfActivations", "totalactivations").\
                withColumnRenamed("DateCreated", "datecreated").\
                withColumnRenamed("TypeOfCustomer", "customertype")
'''                
Product_Schema = StructType() .\
                        add("ProductSKU", StringType(), True). \
                        add("ProductName", StringType(), True). \
                        add("ProductLabel", StringType(), True). \
                        add("DefaultCost", StringType(), True). \
                        add("AverageCOS", StringType(), True). \
                        add("UnitCost", StringType(), True). \
                        add("MostRecentCost", StringType(), True). \
                        add("ProductLibraryName", StringType(), True). \
                        add("Manufacturer", StringType(), True). \
                        add("ManufacturerPartNumber", StringType(), True). \
                        add("CategoryName", StringType(), True). \
                        add("PricingType", StringType(), True). \
                        add("DefaultRetailPrice", StringType(), True). \
                        add("DefaultMargin", StringType(), True). \
                        add("FloorPrice", StringType(), True). \
                        add("PAWFloorPrice", StringType(), True). \
                        add("DefaultMinQty", StringType(), True).\
                        add("DefaultMaxQty", StringType(), True). \
                        add("LockMinMax", StringType(), True). \
                        add("NoSale", StringType(), True). \
                        add("RMADays", StringType(), True). \
                        add("InvoiceComments", StringType(), True). \
                        add("Serialized", StringType(), True). \
                        add("SerialNumberLength", StringType(), True). \
                        add("Discountable", StringType(), True). \
                        add("DefaultDiscontinuedDate", StringType(), True). \
                        add("DateCreated", StringType(), True). \
                        add("Enabled", StringType(), True).\
                        add("EcommerceItem", StringType(), True). \
                        add("WarehouseLocation", StringType(), True). \
                        add("DefaultVendorName", StringType(), True). \
                        add("PrimaryVendorSKU", StringType(), True). \
                        add("CostofGoodsSoldAccount", StringType(), True). \
                        add("SalesRevenueAccount", StringType(), True). \
                        add("InventoryAccount", StringType(), True). \
                        add("InventoryCorrectionsAccount", StringType(), True). \
                        add("WarrantyWebLink", StringType(), True). \
                        add("WarrantyDescription", StringType(), True). \
                        add("RMANumberRequired", StringType(), True). \
                        add("WarrantyLengthUnits", StringType(), True). \
                        add("WarrantyLengthValue", StringType(), True). \
                        add("CommissionDetailsLocked", StringType(), True). \
                        add("ShowOnInvoice", StringType(), True). \
                        add("Refundable", StringType(), True). \
                        add("RefundPeriodLength", StringType(), True).\
                        add("RefundToUsed", StringType(), True). \
                        add("TriggerServiceRequestOnSale", StringType(), True). \
                        add("ServiceRequestType", StringType(), True). \
                        add("MultiLevelPriceDetailsLocked", StringType(), True). \
                        add("BackOrderDate", StringType(), True). \
                        add("StoreInStoreSKU", StringType(), True). \
                        add("StoreInStorePrice", StringType(), True). \
                        add("DefaultDoNotOrder", StringType(), True). \
                        add("DefaultSpecialOrder", StringType(), True). \
                        add("DefaultDateEOL", StringType(), True). \
                        add("DefaultWriteOff", StringType(), True).\
                        add("NoAutoTaxes", StringType(), True). \
                        add("TaxApplicationType", StringType(), True). \
                        add("DivisionID", StringType(), True). \
                        add("AccessedDate", StringType(), True)
                
                
dfProductCust = spark.read.format("com.crealytics.spark.excel").\
                option("location", ProductCust).\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(Product_Schema).\
                option("spark.read.simpleMode","true").\
                option("useHeader", "true").\
                load("com.databricks.spark.csv")
'''
            
'''        
Employee_Schema = StructType() .\
                        add("EmployeeName", StringType(), True). \
                        add("Username", StringType(), True). \
                        add("ID#", StringType(), True). \
                        add("JobTitle", StringType(), True). \
                        add("Role", StringType(), True). \
                        add("Supervisor", StringType(), True). \
                        add("Disabled", StringType(), True). \
                        add("Locked", StringType(), True). \
                        add("ImageUploaded", StringType(), True). \
                        add("FingerprintEnabled", StringType(), True). \
                        add("ClearFingerprints", StringType(), True). \
                        add("SecurityRole", StringType(), True). \
                        add("PrimaryLocation", StringType(), True). \
                        add("Gender", StringType(), True). \
                        add("City", StringType(), True). \
                        add("State/Prov", StringType(), True). \
                        add("Country", StringType(), True).\
                        add("Work#", StringType(), True). \
                        add("PersonalEmail", StringType(), True). \
                        add("EmailAddress", StringType(), True). \
                        add("EmailServer", StringType(), True). \
                        add("EmailUserName", StringType(), True). \
                        add("EmailPassword", StringType(), True). \
                        add("EmailDisplayName", StringType(), True). \
                        add("AssignedLocations", StringType(), True).\
                        add("AssignedGroups", StringType(), True). \
                        add("StartDate", StringType(), True). \
                        add("TerminationDate", StringType(), True). \
                        add("TerminationNotes", StringType(), True). \
                        add("CompensationType", StringType(), True). \
                        add("Rate", StringType(), True). \
                        add("Internal", StringType(), True). \
                        add("RateEffective", StringType(), True). \
                        add("CommissionGroup", StringType(), True). \
                        add("Frequency", StringType(), True). \
                        add("FrequencyEffective", StringType(), True). \
                        add("PartTime", StringType(), True). \
                        add("VacationDaysAvailable", StringType(), True). \
                        add("SickDaysAvailable", StringType(), True). \
                        add("PersonalDaysAvailable", StringType(), True). \
                        add("Language", StringType(), True). \
                        add("EmployeeID", StringType(), True).\
                        add("IntrgrationUsername", StringType(), True). \
                        add("DivisionID", StringType(), True). \
                        add("AccessedDate", StringType(), True). \
                        add("DateCreated", StringType(), True)

'''
dfEmployeeCust = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        option("quote", "\"").\
                        option("multiLine", "true").\
                        option("spark.read.simpleMode","true").\
                        option("useHeader", "true").\
                        load(EmployeeCust)                          
'''
dfEmployeeCust = spark.read.format("com.crealytics.spark.excel").\
                option("location", EmployeeCust).\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(Employee_Schema). \
                option("spark.read.simpleMode","true").\
                option("useHeader", "true").\
                load("com.databricks.spark.csv")
                
'''
                
#dfSpringCommCust = dfSpringCommCust.withColumnRenamed("Employee Name", "EmployeeName")
           
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')

#dfProductCust.coalesce(1). \
#        write.format("com.databricks.spark.csv").\
#        option("header", "true").mode("overwrite").save(CompanyOutputArg)

   
dfEmployeeCust.coalesce(1). \
        write.format("com.databricks.spark.csv").\
        option("header", "true").mode("overwrite").save(CompanyOutputArg)


dfEmployeeCust.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'employee' + CompanyFileTime);
    
    
dfSpringCommCust.coalesce(1).select("*"). \
    write.parquet(CompanyOutputArg + '/' + todayyear + '/' + todaymonth + '/' + 'customer' + CompanyFileTime);


      
spark.stop()                        