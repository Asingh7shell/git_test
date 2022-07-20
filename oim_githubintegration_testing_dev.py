# Databricks notebook source
# html = """<h1 style="color:green;text-align:center;font-family:Courier">OIM Notebook</h1>"""
# displayHTML(html)

# COMMAND ----------

from pyspark.sql.functions import col, row_number, countDistinct,when,lit, dayofweek
from pyspark.sql.functions import concat_ws, max ,sum, desc,abs,unix_timestamp, from_unixtime
from datetime import date, timedelta , datetime
from pyspark.sql import functions as F
import pyspark.sql.functions as funcs
from pyspark.sql.window import Window
# from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#creating variable which contains the Time Duration of last 2 year from now as per Business Need.
year_2 = (date.today() - relativedelta(years=2)).year
# print(year_2)

# COMMAND ----------

# MAGIC %run /Shared/Phoenix/FI_REUSE_METHOD

# COMMAND ----------

# Widget inorder to get project path from SQL Table
dbutils.widgets.text("Get the Config table details", "", "")
Config_Table_name = dbutils.widgets.get("Get the Config table details")

# fetching path details from SQL table
df_proj_path = spark.read.jdbc(url=jdbcUrl, table=Config_Table_name)

# COMMAND ----------

#Reading Path of Currency_Document file from the DB Table and then creating Dataframe out of it.
Currency_Doc_Multiplier= \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "FIN_MAPPING") & (df_proj_path.OBJECT_NAME == "OIM")).select("EDAM_PATH").collect()[0][0]
df_CurrencyDocAmount = spark.read.format("csv").option("Header","true").load(Currency_Doc_Multiplier)

################################################################################################################
Path_AgentName_Proj = "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/AGENT_USERNAME_MAPPING.csv/"
df_AgentName_proj = spark.read.format("csv").option("header","true").load(Path_AgentName_Proj)
#Reading Dataframe for Fetching AGENT_NAME.
df_AgentName_req = df_AgentName_proj.withColumn("AGENT_NAME",concat_ws(" ","First Name","Surname"))
df_AgentName = df_AgentName_req.select(col("Email Alias").alias("AGENT_ID"),"First Name","Surname","Full Name","AGENT_NAME")


##########################################################################################################
#Reading path of Consolidated_OIM File.
DataFilter=df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "FIN_MAPPING") & (df_proj_path.OBJECT_NAME == "DataFilter_OIM")).select(
        "EDAM_PATH").collect()[0][0]

#Creating Schema for Consolidated_OIM  DataFrame 
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema=StructType([StructField("ERP_NAME",StringType(),True), \
    StructField("COM_CODE",StringType(),True), \
    StructField("GENERAL_LEDGER_ACCOUNT_NUMBER",StringType(),True), \
    StructField("VENDOR_ACCOUNT_GROUP_CODE", StringType(), True), \
    StructField("Doc_Type", StringType(), True), \
    StructField("Inclusion_Exclusion",StringType(),True), \
    StructField("Comments",StringType(),True)])

#Creating Dataframe from Consolidated_OIM file for reading Company_Code and Vendor_Account_Group for each ERPs 
df_datafilter = \
spark.read.format("csv").option("delimiter",",").option("multiline","true").option("Header","true").schema(schema).load(DataFilter)


# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_COMMON_CODE_FOR_ALL_ERPs

# COMMAND ----------

# MAGIC %md
# MAGIC #Start of SERP

# COMMAND ----------

#Defining Path for each Table of SERP.
'''Configuring project path'''
Path_BSEG_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "BSEG")).select("EDAM_PATH").collect()[0][0]

Path_BKPF_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "BKPF")).select("EDAM_PATH").collect()[0][0]

Path_VBSEGK_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 
   
Path_LFA1_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "LFA1")).select("EDAM_PATH").collect()[0][0]

Path_LFB1_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "LFB1")).select("EDAM_PATH").collect()[0][0]

Path_EKPO_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "EKPO")).select("EDAM_PATH").collect()[0][0]

Path_EKKO_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "EKKO")).select("EDAM_PATH").collect()[0][0]

Path_ESSR_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "ESSR")).select("EDAM_PATH").collect()[0][0]

Path_RSEG_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "RSEG")).select("EDAM_PATH").collect()[0][0]

Path_SWW_WI2OBJ_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIHEAD_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_SERP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select("EDAM_PATH").collect()[0][0]


# COMMAND ----------

# print(Path_BSEG_SERP)
# print(Path_BKPF_SERP)
# print(Path_VBSEGK_SERP)
# print(Path_VBKPF_SERP)
# print(Path_LFA1_SERP)
# print(Path_LFB1_SERP)
# print(Path_EKPO_SERP)
# print(Path_EKKO_SERP)
# print(Path_ESSR_SERP)
# print(Path_RSEG_SERP)
# print(Path_SWW_WI2OBJ_SERP)
# print(Path_SWWWIAGENT_SERP)
# print(Path_SWWWIHEAD_SERP)

# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_SERP_proj = spark.read.format("delta").load(Path_BSEG_SERP)
df_BKPF_SERP_proj = spark.read.format("delta").load(Path_BKPF_SERP)
df_VBSEGK_SERP_proj = spark.read.format("delta").load(Path_VBSEGK_SERP)
df_VBKPF_SERP_proj = spark.read.format("delta").load(Path_VBKPF_SERP)
df_LFA1_SERP_proj = spark.read.format("delta").load(Path_LFA1_SERP)
df_LFB1_SERP_proj = spark.read.format("delta").load(Path_LFB1_SERP)
df_EKPO_SERP_proj = spark.read.format("delta").load(Path_EKPO_SERP)
df_EKKO_SERP_proj = spark.read.format("delta").load(Path_EKKO_SERP)
df_ESSR_SERP_proj = spark.read.format("delta").load(Path_ESSR_SERP)
df_RSEG_SERP_proj = spark.read.format("delta").load(Path_RSEG_SERP)
df_SWW_WI2OBJ_SERP_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_SERP)
df_SWWIHEAD_SERP_proj = spark.read.format("delta").load(Path_SWWWIHEAD_SERP)
df_SWWWIAGENT_SERP_proj = spark.read.format("delta").load(Path_SWWWIAGENT_SERP)

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_SERP=list(df_datafilter.where(df_datafilter.ERP_NAME=='SERP').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_SERP=list(df_datafilter.where(df_datafilter.ERP_NAME=='SERP').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])

recon_account_SERP=list(df_datafilter.where(df_datafilter.ERP_NAME=='SERP').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])
 
############################### Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_SERP=df_BSEG_SERP_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
df_BSEG_req_SERP = df_BSEG_req_SERP.filter(col('COMPANY_CODE').isin(company_codes_SERP))
df_BSEG_req_SERP = df_BSEG_req_SERP.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_SERP = df_BSEG_req_SERP.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_SERP = df_BSEG_req_SERP.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_SERP = df_BSEG_req_SERP.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_SERP = df_BSEG_SERP.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_SERP))


df_VBSEGK_req_SERP=df_VBSEGK_SERP_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_SERP = df_VBSEGK_req_SERP.filter(col('COMPANY_CODE').isin(company_codes_SERP))
df_VBSEGK_req_SERP = df_VBSEGK_req_SERP.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_SERP = df_VBSEGK_req_SERP.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_SERP = df_VBSEGK_SERP.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_SERP))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_SERP_filtered = df_VBSEGK_SERP.join(df_BSEG_SERP, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK
df_BSEG_Merged_SERP = df_BSEG_SERP.unionByName(df_VBSEGK_SERP_filtered, allowMissingColumns = True)


# df_BSEG_Merged_SERP = df_BSEG_SERP.unionByName(df_VBSEGK_SERP, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################


df_BKPF_req_SERP=df_BKPF_SERP_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_SERP=df_BKPF_req_SERP.filter(col('COMPANY_CODE').isin(company_codes_SERP))
df_BKPF_SERP = df_BKPF_req_SERP.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_SERP=df_VBKPF_SERP_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_SERP = df_VBKPF_req_SERP.filter(col('COMPANY_CODE').isin(company_codes_SERP))
df_VBKPF_SERP = df_VBKPF_req_SERP.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################
#filtering records present in VBKPF only
df_VBKPF_SERP_filtered = df_VBKPF_SERP.join(df_BKPF_SERP, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF
df_BKPF_Merged_SERP = df_BKPF_SERP.unionByName(df_VBKPF_SERP_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_SERP = df_BKPF_SERP.unionByName(df_VBKPF_SERP,allowMissingColumns = True) 
######################################### END OF BKPF ########################################################################################
 


df_LFA1_req_SERP=df_LFA1_SERP_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_SERP=df_LFA1_req_SERP.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_SERP)) #Newly added

df_LFB1_SERP=df_LFB1_SERP_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_SERP=df_EKPO_SERP_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_SERP=df_EKKO_SERP_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_SERP = df_ESSR_SERP_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_SERP = df_RSEG_SERP_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_SERP=df_RSEG_req_SERP.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))

# COMMAND ----------

#Obtaining LIV records i.e. the documents which are Posted with Purchasing Document Number
# doc_type_liv_SERP =['RE','RM','RS']
doc_type_liv_SERP =['RE','RM','RS','IA','II','KI','KK','KN','MJ','UN']
df_BKPF_LIV_SERP = df_BKPF_Merged_SERP.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_SERP))
df_BKPF_LIV_SERP = df_BKPF_LIV_SERP.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Join BKPF and BSEG
df_BKPF_BSEG_SERP = df_BKPF_LIV_SERP.join(df_BSEG_Merged_SERP,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_SERP = df_BKPF_BSEG_SERP.join(df_RSEG_SERP,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_SERP = df_BKPF_BSEG_RSEG_SERP.join(df_ESSR_SERP,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_SERP = df_BKPF_BSEG_RSEG_ESSR_SERP.join(df_EKPO_SERP ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_SERP = df_BKPF_BSEG_RSEG_ESSR_EKPO_SERP.join(df_LFA1_SERP, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_SERP.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_SERP.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_SERP = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_SERP.join(df_LFB1_SERP,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with LFB1
df_LIV_SERP = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_SERP.join(df_EKKO_SERP, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_SERP = df_LIV_SERP.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
doc_type_fi_SERP=['RE','RM','RS','IA','II','KI','KK','KN','MJ','UN']

#Filtering out BKPF for FI document types
df_BKPF_FI_SERP=df_BKPF_Merged_SERP.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_SERP))

#Join BSEG and BKPF
df_BSEG_BKPF_FI_SERP = df_BSEG_Merged_SERP.join(df_BKPF_FI_SERP, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Joining with LFA1
df_BSEG_BKPF_LFA1_FI_SERP = df_BSEG_BKPF_FI_SERP.join(df_LFA1_SERP, df_BSEG_BKPF_FI_SERP.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_SERP.VENDOR_ID, how="left")

#Joining with LFB1
df_FI_SERP = df_BSEG_BKPF_LFA1_FI_SERP.join(df_LFB1_SERP, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_SERP=df_FI_SERP.join(df_LIV_SERP, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_SERP = df_FI_anti_SERP.withColumn('SPEND_TYPE',lit('FI'))



##############################################   Union Liv and FI Doctype Dataset  #################################
df_Final_LIVFI_SERP = df_LIV_SERP.unionByName(df_FI_anti_SERP, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_SERP = df_Final_LIVFI_SERP.withColumn("ERP_NAME",lit('SERP'))
df_Final_LIVFI_SERP = df_Final_LIVFI_SERP.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_SERP = df_Final_LIVFI_SERP.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_SERP = df_ERS_Cal_1_SERP.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_SERP = df_ERS_Cal_2_SERP.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))

#Joining Mapping Tables to the Final Dataframe
MASTER_DATA_MAPPING= \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
        "MASTER_DATA_PATH").collect()[0][0]
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_SERP =df_ERS_Cal_3_SERP.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')
df_ERS_Cal_Master_SERP.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_SERP = df_ERS_Cal_Master_SERP.withColumn("FISCAL_YEAR", df_ERS_Cal_3_SERP["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_SERP = df_Final1_SERP.withColumn("FISCAL_PERIOD", df_Final1_SERP["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_SERP = df_Final1_SERP.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_SERP = df_Final1_SERP.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_SERP = df_Final1_SERP.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_SERP = df_Final1_SERP.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_SERP = df_Final1_SERP.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_SERP = df_Final1_SERP.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_SERP = df_Final1_SERP.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_SERP = df_Final_DTypeC_SERP.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_SERP = df_lt18_SERP.withColumn("REFERENCE_KEY_ID_tw",df_lt18_SERP.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_SERP = df_Final_DTypeC_SERP.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_SERP = df_eq18_SERP.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_SERP = df_eq18_awkey_SERP.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_SERP = df_eq18_awkey_SERP.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_SERP = df_eq18_awkey_SERP.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_SERP = df_lt18_SERP.unionByName(df_eq18_awkey_SERP, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Table and Filtering the Column and the Records as per Business Need.
df_SWW_WI2OBJ_req_SERP = df_SWW_WI2OBJ_SERP_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_SERP = df_SWW_WI2OBJ_req_SERP.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_SERP = df_SWW_WI2OBJ_req_SERP.drop(col("TASK_ID"))


df_SWWIHEAD_req_SERP=df_SWWIHEAD_SERP_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_SERP = df_SWWIHEAD_req_SERP.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_SERP = df_SWWWIAGENT_SERP_proj.select("AGENT_ID","WORK_ITEM_ID")


################################ Joining the Finance Final Dataset with SWW_OBJ(WorkflowTable). ##############################
df_Finance_SWWOBJ_SERP = df_Final_Finance_SERP.join(df_SWW_WI2OBJ_SERP,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_SERP_isNull = df_Finance_SWWOBJ_SERP.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_SERP_NotNull = df_Finance_SWWOBJ_SERP.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_SERP_NotNull = df_Finance_SWWOBJ_SERP_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_SERP_NotNullfiltered = df_Finance_SWWOBJ_SERP_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_SERP = df_Finance_SWWOBJ_SERP_NotNullfiltered.join(df_SWWIHEAD_SERP,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_SERP = df_Finance_SWWOBJ_SWWHEAD_SERP.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_SERP = df_Finance_SWWOBJ_SWWHEAD_SERP.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_SERP_NotNull = df_Finance_SWWOBJ_SWWHEAD_SERP.join(df_SWWWIAGENT_SERP,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_SERP_NotNull = df_Finance_WORKFLOW_SERP.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_SERP = df_Finance_WORKFLOW_SERP_NotNull.unionByName(df_Finance_SWWOBJ_SERP_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_SERP1 = df_Finance_WORKFLOW_Final_SERP.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_SERP1 = df_Finance_WORKFLOW_Final_SERP1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_SERP1=df_Final_SERP1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_SERP1=df_Final_SERP1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_SERP)) #Newly Added

# Joining Final Dataset with Currency_DocAmountMultiplier dataframe. 
df_Final_Currency_SERP = df_Final_SERP1.join(df_CurrencyDocAmount, [df_Final_SERP1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_SERP1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

# Adding new column based on Document_Currency_Amount.
df_Final_Currency_SERP = df_Final_Currency_SERP.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul",         when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value')).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

#Dropping Unnecessary Columns.
columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_SERP = df_Final_Currency_SERP.drop(*columns_to_drop)
df_Final_DS_SERP = df_Final_Currency_SERP.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_SERP

# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING_OF_BG

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "BSEG")).select("EDAM_PATH").collect()[0][0]

Path_BKPF_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "BKPF")).select("EDAM_PATH").collect()[0][0]

Path_VBSEGK_BG = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_BG = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 

Path_LFA1_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "LFA1")).select("EDAM_PATH").collect()[0][0]
Path_LFB1_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "LFB1")).select("EDAM_PATH").collect()[0][0]
Path_EKPO_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "EKPO")).select("EDAM_PATH").collect()[0][0]
Path_EKKO_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "EKKO")).select("EDAM_PATH").collect()[0][0]

Path_ESSR_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "ESSR")).select("EDAM_PATH").collect()[0][0]
Path_RSEG_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "RSEG")).select("EDAM_PATH").collect()[0][0]

Path_SWW_WI2OBJ_BG = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select("EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_BG = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_BG = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BG") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select("EDAM_PATH").collect()[0][0]

# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_BG_proj = spark.read.format("delta").load(Path_BSEG_BG)
df_BKPF_BG_proj = spark.read.format("delta").load(Path_BKPF_BG)
df_VBSEGK_BG_proj = spark.read.format("delta").load(Path_VBSEGK_BG)
df_VBKPF_BG_proj = spark.read.format("delta").load(Path_VBKPF_BG)
df_LFA1_BG_proj = spark.read.format("delta").load(Path_LFA1_BG)
df_LFB1_BG_proj = spark.read.format("delta").load(Path_LFB1_BG)
df_EKPO_BG_proj = spark.read.format("delta").load(Path_EKPO_BG)
df_EKKO_BG_proj = spark.read.format("delta").load(Path_EKKO_BG)
df_EKKO_BG_proj = df_EKKO_BG_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_BG_proj = spark.read.format("delta").load(Path_ESSR_BG)
df_ESSR_BG_proj = df_ESSR_BG_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_BG_proj = spark.read.format("delta").load(Path_RSEG_BG)
df_SWW_WI2OBJ_BG_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_BG)
df_SWWIHEAD_BG_proj = spark.read.format("delta").load(Path_SWWWIHEAD_BG)
df_SWWWIAGENT_BG_proj = spark.read.format("delta").load(Path_SWWWIAGENT_BG)


# COMMAND ----------

#############--Selecting Columns from the Table as well as Filtering the Records, which are required as Per Business Need --##############

# Selecting Columns as Per Business requirement and Filtering the Records.
company_codes_BG=list(df_datafilter.where(df_datafilter.ERP_NAME=='BG').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_BG=list(df_datafilter.where(df_datafilter.ERP_NAME=='BG').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_BG=list(df_datafilter.where(df_datafilter.ERP_NAME=='BG').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])

############################### Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_BG=df_BSEG_BG_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_BG = df_BSEG_req_BG.filter(col('COMPANY_CODE').isin(company_codes_BG))
df_BSEG_req_BG = df_BSEG_req_BG.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_BG = df_BSEG_req_BG.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_BG = df_BSEG_req_BG.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_BG = df_BSEG_req_BG.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_BG = df_BSEG_BG.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_BG))


df_VBSEGK_req_BG=df_VBSEGK_BG_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_BG = df_VBSEGK_req_BG.filter(col('COMPANY_CODE').isin(company_codes_BG))
df_VBSEGK_req_BG = df_VBSEGK_req_BG.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_BG = df_VBSEGK_req_BG.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_BG = df_VBSEGK_BG.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_BG))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED#################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_BG_filtered = df_VBSEGK_BG.join(df_BSEG_BG, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_BG = df_BSEG_BG.unionByName(df_VBSEGK_BG_filtered, allowMissingColumns = True)


# df_BSEG_Merged_BG = df_BSEG_BG.unionByName(df_VBSEGK_BG, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################


df_BKPF_req_BG=df_BKPF_BG_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_BG=df_BKPF_req_BG.filter(col('COMPANY_CODE').isin(company_codes_BG))
df_BKPF_BG = df_BKPF_req_BG.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_BG=df_VBKPF_BG_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_BG = df_VBKPF_req_BG.filter(col('COMPANY_CODE').isin(company_codes_BG))
df_VBKPF_BG = df_VBKPF_req_BG.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED #######################################
#filtering records present in VBKPF only
df_VBKPF_BG_filtered = df_VBKPF_BG.join(df_BKPF_BG, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_BG = df_BKPF_BG.unionByName(df_VBKPF_BG_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_BG = df_BKPF_BG.unionByName(df_VBKPF_BG,allowMissingColumns = True)
######################################### END OF BKPF ########################################################################################
 


df_LFA1_req_BG=df_LFA1_BG_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_BG=df_LFA1_req_BG.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_BG)) #Newly added

df_LFB1_BG=df_LFB1_BG_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_BG=df_EKPO_BG_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_BG=df_EKKO_BG_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_BG = df_ESSR_BG_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_BG = df_RSEG_BG_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_BG=df_RSEG_req_BG.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_BG =['II','IJ','RE','RM','RD']
doc_type_liv_BG =['II','IJ','RE','RM','RD','KA','KG','KR']
df_BKPF_LIV_BG = df_BKPF_Merged_BG.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_BG))
df_BKPF_LIV_BG = df_BKPF_LIV_BG.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Joining BKPF and BSEG
df_BKPF_BSEG_BG = df_BKPF_LIV_BG.join(df_BSEG_Merged_BG,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_BG = df_BKPF_BSEG_BG.join(df_RSEG_BG,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_BG = df_BKPF_BSEG_RSEG_BG.join(df_ESSR_BG,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_BG = df_BKPF_BSEG_RSEG_ESSR_BG.join(df_EKPO_BG ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_BG = df_BKPF_BSEG_RSEG_ESSR_EKPO_BG.join(df_LFA1_BG, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_BG.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_BG.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_BG = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_BG.join(df_LFB1_BG,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_BG = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_BG.join(df_EKKO_BG, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_BG = df_LIV_BG.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_BG=['KR']

doc_type_fi_BG= ['II','IJ','RE','RM','RD','KA','KG','KR']

#Filtering out BKPF for FI document types
df_BKPF_FI_BG=df_BKPF_Merged_BG.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_BG))

#Join BSEG and BKPF
df_BSEG_BKPF_FI_BG = df_BSEG_Merged_BG.join(df_BKPF_FI_BG, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_BG = df_BSEG_BKPF_FI_BG.join(df_LFA1_BG, df_BSEG_BKPF_FI_BG.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_BG.VENDOR_ID, how="left")

#join with LFB1
df_FI_BG = df_BSEG_BKPF_LFA1_FI_BG.join(df_LFB1_BG, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_BG=df_FI_BG.join(df_LIV_BG, ['ACCOUNTING_DOCUMENT_NUMBER'], how='leftanti')
#Adding a new Column 
df_FI_anti_BG = df_FI_anti_BG.withColumn('SPEND_TYPE',lit('FI'))

##############################################   Union Liv and FI Doctype Dataset  #################################
df_Final_LIVFI_BG = df_LIV_BG.unionByName(df_FI_anti_BG, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_BG = df_Final_LIVFI_BG.withColumn("ERP_NAME",lit('BG'))
df_Final_LIVFI_BG = df_Final_LIVFI_BG.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_BG = df_Final_LIVFI_BG.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_BG = df_ERS_Cal_1_BG.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_BG = df_ERS_Cal_2_BG.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_BG =df_ERS_Cal_3_BG.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_BG.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_BG = df_ERS_Cal_Master_BG.withColumn("FISCAL_YEAR", df_ERS_Cal_3_BG["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_BG = df_Final1_BG.withColumn("FISCAL_PERIOD", df_Final1_BG["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_BG = df_Final1_BG.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BG = df_Final1_BG.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BG = df_Final1_BG.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BG = df_Final1_BG.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BG = df_Final1_BG.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BG = df_Final1_BG.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_BG = df_Final1_BG.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_BG = df_Final_DTypeC_BG.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_BG = df_lt18_BG.withColumn("REFERENCE_KEY_ID_tw",df_lt18_BG.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_BG = df_Final_DTypeC_BG.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_BG = df_eq18_BG.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_BG = df_eq18_awkey_BG.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_BG = df_eq18_awkey_BG.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_BG = df_eq18_awkey_BG.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_BG = df_lt18_BG.unionByName(df_eq18_awkey_BG, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_BG = df_SWW_WI2OBJ_BG_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_BG = df_SWW_WI2OBJ_req_BG.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_BG = df_SWW_WI2OBJ_req_BG.drop(col("TASK_ID"))


df_SWWIHEAD_req_BG=df_SWWIHEAD_BG_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_BG = df_SWWIHEAD_req_BG.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_BG = df_SWWWIAGENT_BG_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_BG = df_Final_Finance_BG.join(df_SWW_WI2OBJ_BG,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_BG_isNull = df_Finance_SWWOBJ_BG.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_BG_NotNull = df_Finance_SWWOBJ_BG.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_BG_NotNull = df_Finance_SWWOBJ_BG_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_BG_NotNullfiltered = df_Finance_SWWOBJ_BG_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_BG = df_Finance_SWWOBJ_BG_NotNullfiltered.join(df_SWWIHEAD_BG,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_BG = df_Finance_SWWOBJ_SWWHEAD_BG.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_BG = df_Finance_SWWOBJ_SWWHEAD_BG.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_BG_NotNull = df_Finance_SWWOBJ_SWWHEAD_BG.join(df_SWWWIAGENT_BG,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_BG_NotNull = df_Finance_WORKFLOW_BG.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_BG = df_Finance_WORKFLOW_BG_NotNull.unionByName(df_Finance_SWWOBJ_BG_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_BG1 = df_Finance_WORKFLOW_Final_BG.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_BG1 = df_Finance_WORKFLOW_Final_BG1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_BG1=df_Final_BG1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_BG1=df_Final_BG1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_BG)) #Newly Added

df_Final_Currency_BG = df_Final_BG1.join(df_CurrencyDocAmount, [df_Final_BG1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_BG1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_BG = df_Final_Currency_BG.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value')).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_BG = df_Final_Currency_BG.drop(*columns_to_drop)
df_Final_DS_BG = df_Final_Currency_BG.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_BG

# COMMAND ----------

#Union of Above dataset with BG
df_Final_DS_SERP_BG = df_Final_DS_SERP.unionByName(df_Final_DS_BG)

# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING_OF_CRITERION

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
        "EDAM_PATH").collect()[0][0]
Path_BKPF_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "BKPF")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBSEGK_CRITERION = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBKPF_CRITERION = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "VBKPF")).select(
        "EDAM_PATH").collect()[0][0] 
  
  
Path_LFA1_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
        "EDAM_PATH").collect()[0][0]
Path_LFB1_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "LFB1")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKPO_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "EKPO")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKKO_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "EKKO")).select(
        "EDAM_PATH").collect()[0][0]

Path_ESSR_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "ESSR")).select(
        "EDAM_PATH").collect()[0][0]
Path_RSEG_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "RSEG")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_CRITERION = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_CRITERION = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIAGENT_CRITERION = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRITERION") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select(
        "EDAM_PATH").collect()[0][0]

# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_CRITERION_proj = spark.read.format("delta").load(Path_BSEG_CRITERION)
df_BKPF_CRITERION_proj = spark.read.format("delta").load(Path_BKPF_CRITERION)
df_VBSEGK_CRITERION_proj = spark.read.format("delta").load(Path_VBSEGK_CRITERION)
df_VBKPF_CRITERION_proj = spark.read.format("delta").load(Path_VBKPF_CRITERION)
df_LFA1_CRITERION_proj = spark.read.format("delta").load(Path_LFA1_CRITERION)
df_LFB1_CRITERION_proj = spark.read.format("delta").load(Path_LFB1_CRITERION)
df_EKPO_CRITERION_proj = spark.read.format("delta").load(Path_EKPO_CRITERION)
df_EKKO_CRITERION_proj = spark.read.format("delta").load(Path_EKKO_CRITERION)
df_EKKO_CRITERION_proj = df_EKKO_CRITERION_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_CRITERION_proj = spark.read.format("delta").load(Path_ESSR_CRITERION)
df_ESSR_CRITERION_proj = df_ESSR_CRITERION_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_CRITERION_proj = spark.read.format("delta").load(Path_RSEG_CRITERION)
df_SWW_WI2OBJ_CRITERION_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_CRITERION)
df_SWWIHEAD_CRITERION_proj = spark.read.format("delta").load(Path_SWWWIHEAD_CRITERION)
df_SWWWIAGENT_CRITERION_proj = spark.read.format("delta").load(Path_SWWWIAGENT_CRITERION)


# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##############

## Selecting Columns as Per Business requirement and Filtering the Records.
company_codes_CRITERION=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRITERION').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger Account Number
recon_account_CRITERION=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRITERION').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_CRITERION=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRITERION').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])

############################### Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_CRITERION=df_BSEG_CRITERION_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_CRITERION = df_BSEG_req_CRITERION.filter(col('COMPANY_CODE').isin(company_codes_CRITERION))
df_BSEG_req_CRITERION = df_BSEG_req_CRITERION.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_CRITERION = df_BSEG_req_CRITERION.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_CRITERION = df_BSEG_req_CRITERION.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_CRITERION = df_BSEG_req_CRITERION.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_CRITERION = df_BSEG_CRITERION.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_CRITERION))

df_VBSEGK_req_CRITERION=df_VBSEGK_CRITERION_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_CRITERION = df_VBSEGK_req_CRITERION.filter(col('COMPANY_CODE').isin(company_codes_CRITERION))
df_VBSEGK_req_CRITERION = df_VBSEGK_req_CRITERION.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_CRITERION = df_VBSEGK_req_CRITERION.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_CRITERION = df_VBSEGK_CRITERION.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_CRITERION))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_CRITERION_filtered = df_VBSEGK_CRITERION.join(df_BSEG_CRITERION, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_CRITERION = df_BSEG_CRITERION.unionByName(df_VBSEGK_CRITERION_filtered, allowMissingColumns = True)


# df_BSEG_Merged_CRITERION = df_BSEG_CRITERION.unionByName(df_VBSEGK_CRITERION, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################


df_BKPF_req_CRITERION=df_BKPF_CRITERION_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_CRITERION=df_BKPF_req_CRITERION.filter(col('COMPANY_CODE').isin(company_codes_CRITERION))
df_BKPF_CRITERION = df_BKPF_req_CRITERION.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_CRITERION=df_VBKPF_CRITERION_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_CRITERION = df_VBKPF_req_CRITERION.filter(col('COMPANY_CODE').isin(company_codes_CRITERION))
df_VBKPF_CRITERION = df_VBKPF_req_CRITERION.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################
#filtering records present in VBKPF only
df_VBKPF_CRITERION_filtered = df_VBKPF_CRITERION.join(df_BKPF_CRITERION, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_CRITERION = df_BKPF_CRITERION.unionByName(df_VBKPF_CRITERION_filtered,allowMissingColumns = True) 
#df_BKPF_Merged_CRITERION = df_BKPF_CRITERION.unionByName(df_VBKPF_CRITERION,allowMissingColumns = True)

######################################### END OF BKPF ########################################################################################
 


df_LFA1_req_CRITERION=df_LFA1_CRITERION_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_CRITERION=df_LFA1_req_CRITERION.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_CRITERION)) #Newly added

df_LFB1_CRITERION=df_LFB1_CRITERION_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_CRITERION=df_EKPO_CRITERION_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_CRITERION=df_EKKO_CRITERION_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_CRITERION = df_ESSR_CRITERION_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_CRITERION = df_RSEG_CRITERION_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_CRITERION=df_RSEG_req_CRITERION.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_CRITERION =['RE','RM']
doc_type_liv_CRITERION =['RE','RM','KG','KR']
df_BKPF_LIV_CRITERION = df_BKPF_Merged_CRITERION.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_CRITERION))
df_BKPF_LIV_CRITERION = df_BKPF_LIV_CRITERION.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Join BKPF and BSEG
df_BKPF_BSEG_CRITERION = df_BKPF_LIV_CRITERION.join(df_BSEG_Merged_CRITERION,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_CRITERION = df_BKPF_BSEG_CRITERION.join(df_RSEG_CRITERION,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_CRITERION = df_BKPF_BSEG_RSEG_CRITERION.join(df_ESSR_CRITERION,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_CRITERION = df_BKPF_BSEG_RSEG_ESSR_CRITERION.join(df_EKPO_CRITERION ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_CRITERION = df_BKPF_BSEG_RSEG_ESSR_EKPO_CRITERION.join(df_LFA1_CRITERION, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_CRITERION.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_CRITERION.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_CRITERION = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_CRITERION.join(df_LFB1_CRITERION,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_CRITERION = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_CRITERION.join(df_EKKO_CRITERION, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_CRITERION = df_LIV_CRITERION.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_CRITERION=['KR','KG']
doc_type_fi_CRITERION=['KR','KG','RE','RM']

#Filtering out BKPF for FI document types
df_BKPF_FI_CRITERION=df_BKPF_Merged_CRITERION.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_CRITERION))

#Joining BSEG and BKPF
df_BSEG_BKPF_FI_CRITERION = df_BSEG_Merged_CRITERION.join(df_BKPF_FI_CRITERION, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_CRITERION = df_BSEG_BKPF_FI_CRITERION.join(df_LFA1_CRITERION, df_BSEG_BKPF_FI_CRITERION.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_CRITERION.VENDOR_ID, how="left")

#Join with LFB1
df_FI_CRITERION = df_BSEG_BKPF_LFA1_FI_CRITERION.join(df_LFB1_CRITERION, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_CRITERION=df_FI_CRITERION.join(df_LIV_CRITERION, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_CRITERION = df_FI_anti_CRITERION.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_CRITERION = df_LIV_CRITERION.unionByName(df_FI_anti_CRITERION, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_CRITERION = df_Final_LIVFI_CRITERION.withColumn("ERP_NAME",lit('CRITERION'))
df_Final_LIVFI_CRITERION = df_Final_LIVFI_CRITERION.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_CRITERION = df_Final_LIVFI_CRITERION.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_CRITERION = df_ERS_Cal_1_CRITERION.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_CRITERION = df_ERS_Cal_2_CRITERION.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_CRITERION =df_ERS_Cal_3_CRITERION.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_CRITERION.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_CRITERION = df_ERS_Cal_Master_CRITERION.withColumn("FISCAL_YEAR", df_ERS_Cal_3_CRITERION["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn("FISCAL_PERIOD", df_Final1_CRITERION["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRITERION = df_Final1_CRITERION.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_CRITERION = df_Final1_CRITERION.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_CRITERION = df_Final_DTypeC_CRITERION.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_CRITERION = df_lt18_CRITERION.withColumn("REFERENCE_KEY_ID_tw",df_lt18_CRITERION.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_CRITERION = df_Final_DTypeC_CRITERION.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_CRITERION = df_eq18_CRITERION.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_CRITERION = df_eq18_awkey_CRITERION.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_CRITERION = df_eq18_awkey_CRITERION.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_CRITERION = df_eq18_awkey_CRITERION.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_CRITERION = df_lt18_CRITERION.unionByName(df_eq18_awkey_CRITERION, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_CRITERION = df_SWW_WI2OBJ_CRITERION_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_CRITERION = df_SWW_WI2OBJ_req_CRITERION.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_CRITERION = df_SWW_WI2OBJ_req_CRITERION.drop(col("TASK_ID"))


df_SWWIHEAD_req_CRITERION=df_SWWIHEAD_CRITERION_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_CRITERION = df_SWWIHEAD_req_CRITERION.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_CRITERION = df_SWWWIAGENT_CRITERION_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_CRITERION = df_Final_Finance_CRITERION.join(df_SWW_WI2OBJ_CRITERION,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_CRITERION_isNull = df_Finance_SWWOBJ_CRITERION.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_CRITERION_NotNull = df_Finance_SWWOBJ_CRITERION.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_CRITERION_NotNull = df_Finance_SWWOBJ_CRITERION_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_CRITERION_NotNullfiltered = df_Finance_SWWOBJ_CRITERION_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_CRITERION = df_Finance_SWWOBJ_CRITERION_NotNullfiltered.join(df_SWWIHEAD_CRITERION,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_CRITERION = df_Finance_SWWOBJ_SWWHEAD_CRITERION.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_CRITERION = df_Finance_SWWOBJ_SWWHEAD_CRITERION.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_CRITERION_NotNull = df_Finance_SWWOBJ_SWWHEAD_CRITERION.join(df_SWWWIAGENT_CRITERION,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_CRITERION_NotNull = df_Finance_WORKFLOW_CRITERION.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_CRITERION = df_Finance_WORKFLOW_CRITERION_NotNull.unionByName(df_Finance_SWWOBJ_CRITERION_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_CRITERION1 = df_Finance_WORKFLOW_Final_CRITERION.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_CRITERION1 = df_Finance_WORKFLOW_Final_CRITERION1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_CRITERION1=df_Final_CRITERION1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_CRITERION1=df_Final_CRITERION1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_CRITERION)) #Newly Added

df_Final_Currency_CRITERION = df_Final_CRITERION1.join(df_CurrencyDocAmount, [df_Final_CRITERION1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_CRITERION1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_CRITERION = df_Final_Currency_CRITERION.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_CRITERION = df_Final_Currency_CRITERION.drop(*columns_to_drop)
df_Final_DS_CRITERION = df_Final_Currency_CRITERION.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_CRITERION#

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_WITH_CRITERION#

# COMMAND ----------

#Union with Criterion
df_Final_DS_SERP_BG_CRIETRION = df_Final_DS_SERP_BG.unionByName(df_Final_DS_CRITERION)

# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING_OF_GSAP#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
        "EDAM_PATH").collect()[0][0]
Path_BKPF_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "BKPF")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBSEGK_GSAP = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBKPF_GSAP = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "VBKPF")).select(
        "EDAM_PATH").collect()[0][0] 
  
  
Path_LFA1_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
        "EDAM_PATH").collect()[0][0]
Path_LFB1_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "LFB1")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKPO_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "EKPO")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKKO_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "EKKO")).select(
        "EDAM_PATH").collect()[0][0]

Path_ESSR_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "ESSR")).select(
        "EDAM_PATH").collect()[0][0]
Path_RSEG_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "RSEG")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_GSAP = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select(
        "EDAM_PATH").collect()[0][0]

Path_SWWWIHEAD_GSAP = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIAGENT_GSAP = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select(
        "EDAM_PATH").collect()[0][0]


# Currency_Doc_Multiplier= df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "FIN_MAPPING") & (df_proj_path.OBJECT_NAME == "OIM")).select(
#                   "EDAM_PATH").collect()[0][0]
# df_CurrencyDocAmount = spark.read.format("csv").option("Header","true").load(Currency_Doc_Multiplier)



# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_GSAP_proj = spark.read.format("delta").load(Path_BSEG_GSAP)
df_BKPF_GSAP_proj = spark.read.format("delta").load(Path_BKPF_GSAP)
df_VBSEGK_GSAP_proj = spark.read.format("delta").load(Path_VBSEGK_GSAP)
df_VBKPF_GSAP_proj = spark.read.format("delta").load(Path_VBKPF_GSAP)
df_LFA1_GSAP_proj = spark.read.format("delta").load(Path_LFA1_GSAP)
df_LFB1_GSAP_proj = spark.read.format("delta").load(Path_LFB1_GSAP)
df_EKPO_GSAP_proj = spark.read.format("delta").load(Path_EKPO_GSAP)
df_EKKO_GSAP_proj = spark.read.format("delta").load(Path_EKKO_GSAP)
df_EKKO_GSAP_proj = df_EKKO_GSAP_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_GSAP_proj = spark.read.format("delta").load(Path_ESSR_GSAP)
df_ESSR_GSAP_proj = df_ESSR_GSAP_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_GSAP_proj = spark.read.format("delta").load(Path_RSEG_GSAP)
df_SWW_WI2OBJ_GSAP_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_GSAP)
df_SWWIHEAD_GSAP_proj = spark.read.format("delta").load(Path_SWWWIHEAD_GSAP)
df_SWWWIAGENT_GSAP_proj = spark.read.format("delta").load(Path_SWWWIAGENT_GSAP)


# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_GSAP=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_GSAP=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_GSAP=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])


df_BSEG_req_GSAP=df_BSEG_GSAP_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_GSAP = df_BSEG_req_GSAP.filter(col('COMPANY_CODE').isin(company_codes_GSAP))
df_BSEG_req_GSAP = df_BSEG_req_GSAP.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_GSAP = df_BSEG_req_GSAP.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_GSAP = df_BSEG_req_GSAP.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_GSAP = df_BSEG_req_GSAP.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_GSAP = df_BSEG_GSAP.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_GSAP))


df_VBSEGK_req_GSAP=df_VBSEGK_GSAP_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_GSAP = df_VBSEGK_req_GSAP.filter(col('COMPANY_CODE').isin(company_codes_GSAP))
df_VBSEGK_req_GSAP = df_VBSEGK_req_GSAP.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_GSAP = df_VBSEGK_req_GSAP.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_GSAP = df_VBSEGK_GSAP.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_GSAP))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_GSAP_filtered = df_VBSEGK_GSAP.join(df_BSEG_GSAP, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_GSAP = df_BSEG_GSAP.unionByName(df_VBSEGK_GSAP_filtered, allowMissingColumns = True)


# df_BSEG_Merged_GSAP = df_BSEG_GSAP.unionByName(df_VBSEGK_GSAP, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################


df_BKPF_req_GSAP=df_BKPF_GSAP_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_GSAP=df_BKPF_req_GSAP.filter(col('COMPANY_CODE').isin(company_codes_GSAP))
df_BKPF_GSAP = df_BKPF_req_GSAP.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_GSAP=df_VBKPF_GSAP_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_GSAP = df_VBKPF_req_GSAP.filter(col('COMPANY_CODE').isin(company_codes_GSAP))
df_VBKPF_GSAP = df_VBKPF_req_GSAP.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################
#filtering records present in VBKPF only
df_VBKPF_GSAP_filtered = df_VBKPF_GSAP.join(df_BKPF_GSAP, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_GSAP = df_BKPF_GSAP.unionByName(df_VBKPF_GSAP_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_GSAP = df_BKPF_GSAP.unionByName(df_VBKPF_GSAP,allowMissingColumns = True) 
######################################### END OF BKPF ########################################################################################



df_LFA1_req_GSAP=df_LFA1_GSAP_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_GSAP=df_LFA1_req_GSAP.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_GSAP)) #Newly added

df_LFB1_GSAP=df_LFB1_GSAP_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_GSAP=df_EKPO_GSAP_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_GSAP=df_EKKO_GSAP_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_GSAP = df_ESSR_GSAP_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_GSAP = df_RSEG_GSAP_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_GSAP=df_RSEG_req_GSAP.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_GSAP =['KZ','RF','RI','RM','RN','RS','RZ','RC','RD','RR']
doc_type_liv_GSAP = ['KE','KI','KK','KN','KR','KS','KZ','RF','RI','RM','RN','RS','RZ','UC','UG','UI','UN','UP','UQ','UT','AB','CV','DC','HK',
                     'KA','KC','KF','KT','RC','RD','RR','RU','RX','SA','SI']

df_BKPF_LIV_GSAP = df_BKPF_Merged_GSAP.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_GSAP))
df_BKPF_LIV_GSAP = df_BKPF_LIV_GSAP.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Joining with BKPF and BSEG
df_BKPF_BSEG_GSAP = df_BKPF_LIV_GSAP.join(df_BSEG_Merged_GSAP,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_GSAP = df_BKPF_BSEG_GSAP.join(df_RSEG_GSAP,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_GSAP = df_BKPF_BSEG_RSEG_GSAP.join(df_ESSR_GSAP,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP = df_BKPF_BSEG_RSEG_ESSR_GSAP.join(df_EKPO_GSAP ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_GSAP = df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP.join(df_LFA1_GSAP, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_GSAP.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_GSAP = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_GSAP.join(df_LFB1_GSAP,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")
df_LIV_GSAP = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_GSAP.join(df_EKKO_GSAP, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_GSAP = df_LIV_GSAP.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_GSAP=['RM','KE','KI','KK','KN','KR','KS','UC','UG','UI','UN','UP','UQ','UT','CV','KC','KF','KT']
doc_type_fi_GSAP = ['KE','KI','KK','KN','KR','KS','KZ','RF','RI','RM','RN','RS','RZ','UC','UG','UI','UN','UP','UQ','UT','AB','CV','DC','HK',
                     'KA','KC','KF','KT','RC','RD','RR','RU','RX','SA','SI']


#Filtering out BKPF for FI document types
df_BKPF_FI_GSAP=df_BKPF_Merged_GSAP.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_GSAP))

#Join with BSEG and BKPF
df_BSEG_BKPF_FI_GSAP = df_BSEG_Merged_GSAP.join(df_BKPF_FI_GSAP, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_GSAP = df_BSEG_BKPF_FI_GSAP.join(df_LFA1_GSAP, df_BSEG_BKPF_FI_GSAP.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_GSAP.VENDOR_ID, how="left")

#Join with LFB1
df_FI_GSAP = df_BSEG_BKPF_LFA1_FI_GSAP.join(df_LFB1_GSAP, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_GSAP=df_FI_GSAP.join(df_LIV_GSAP, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_GSAP = df_FI_anti_GSAP.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_GSAP = df_LIV_GSAP.unionByName(df_FI_anti_GSAP, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_GSAP = df_Final_LIVFI_GSAP.withColumn("ERP_NAME",lit('GSAP'))
df_Final_LIVFI_GSAP = df_Final_LIVFI_GSAP.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_GSAP = df_Final_LIVFI_GSAP.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_GSAP = df_ERS_Cal_1_GSAP.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_GSAP = df_ERS_Cal_2_GSAP.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_GSAP =df_ERS_Cal_3_GSAP.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_GSAP.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_GSAP = df_ERS_Cal_Master_GSAP.withColumn("FISCAL_YEAR", df_ERS_Cal_3_GSAP["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_GSAP = df_Final1_GSAP.withColumn("FISCAL_PERIOD", df_Final1_GSAP["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_GSAP = df_Final1_GSAP.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP = df_Final1_GSAP.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP = df_Final1_GSAP.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP = df_Final1_GSAP.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP = df_Final1_GSAP.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP = df_Final1_GSAP.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_GSAP = df_Final1_GSAP.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_GSAP = df_Final_DTypeC_GSAP.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_GSAP = df_lt18_GSAP.withColumn("REFERENCE_KEY_ID_tw",df_lt18_GSAP.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_GSAP = df_Final_DTypeC_GSAP.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_GSAP = df_eq18_GSAP.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_GSAP = df_eq18_awkey_GSAP.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_GSAP = df_eq18_awkey_GSAP.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_GSAP = df_eq18_awkey_GSAP.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_GSAP = df_lt18_GSAP.unionByName(df_eq18_awkey_GSAP, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_GSAP = df_SWW_WI2OBJ_GSAP_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_GSAP = df_SWW_WI2OBJ_req_GSAP.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_GSAP = df_SWW_WI2OBJ_req_GSAP.drop(col("TASK_ID"))


df_SWWIHEAD_req_GSAP=df_SWWIHEAD_GSAP_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_GSAP = df_SWWIHEAD_req_GSAP.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_GSAP = df_SWWWIAGENT_GSAP_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_GSAP = df_Final_Finance_GSAP.join(df_SWW_WI2OBJ_GSAP,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_GSAP_isNull = df_Finance_SWWOBJ_GSAP.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_GSAP_NotNull = df_Finance_SWWOBJ_GSAP.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_GSAP_NotNull = df_Finance_SWWOBJ_GSAP_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_GSAP_NotNullfiltered = df_Finance_SWWOBJ_GSAP_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_GSAP = df_Finance_SWWOBJ_GSAP_NotNullfiltered.join(df_SWWIHEAD_GSAP,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_GSAP = df_Finance_SWWOBJ_SWWHEAD_GSAP.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_GSAP = df_Finance_SWWOBJ_SWWHEAD_GSAP.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_GSAP_NotNull = df_Finance_SWWOBJ_SWWHEAD_GSAP.join(df_SWWWIAGENT_GSAP,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_GSAP_NotNull = df_Finance_WORKFLOW_GSAP.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_GSAP = df_Finance_WORKFLOW_GSAP_NotNull.unionByName(df_Finance_SWWOBJ_GSAP_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_GSAP1 = df_Finance_WORKFLOW_Final_GSAP.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_GSAP1 = df_Finance_WORKFLOW_Final_GSAP1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_GSAP1=df_Final_GSAP1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_GSAP1=df_Final_GSAP1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_GSAP)) #Newly Added

df_Final_Currency_GSAP = df_Final_GSAP1.join(df_CurrencyDocAmount, [df_Final_GSAP1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_GSAP1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_GSAP = df_Final_Currency_GSAP.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_GSAP = df_Final_Currency_GSAP.drop(*columns_to_drop)
df_Final_DS_GSAP = df_Final_Currency_GSAP.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_GSAP##

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_WITH_GSAP#

# COMMAND ----------

df_Final_DS_SERP_BG_CRIETRION_GSAP = df_Final_DS_SERP_BG_CRIETRION.unionByName(df_Final_DS_GSAP)

# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING_OF_GSAP_CHEM#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
        "EDAM_PATH").collect()[0][0]
Path_BKPF_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "BKPF")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBSEGK_GSAP_CHEM = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select(
        "EDAM_PATH").collect()[0][0]

Path_VBKPF_GSAP_CHEM = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "VBKPF")).select(
        "EDAM_PATH").collect()[0][0] 
  
  
Path_LFA1_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
        "EDAM_PATH").collect()[0][0]
Path_LFB1_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "LFB1")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKPO_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "EKPO")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKKO_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "EKKO")).select(
        "EDAM_PATH").collect()[0][0]

Path_ESSR_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "ESSR")).select(
        "EDAM_PATH").collect()[0][0]
Path_RSEG_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "RSEG")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_GSAP_CHEM = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_GSAP_CHEM = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIAGENT_GSAP_CHEM = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "GSAP_CHEM") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select(
        "EDAM_PATH").collect()[0][0]



# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_GSAP_CHEM_proj = spark.read.format("delta").load(Path_BSEG_GSAP_CHEM)
df_BKPF_GSAP_CHEM_proj = spark.read.format("delta").load(Path_BKPF_GSAP_CHEM)
df_VBSEGK_GSAP_CHEM_proj = spark.read.format("delta").load(Path_VBSEGK_GSAP_CHEM)
df_VBKPF_GSAP_CHEM_proj = spark.read.format("delta").load(Path_VBKPF_GSAP_CHEM)
df_LFA1_GSAP_CHEM_proj = spark.read.format("delta").load(Path_LFA1_GSAP_CHEM)
df_LFB1_GSAP_CHEM_proj = spark.read.format("delta").load(Path_LFB1_GSAP_CHEM)
df_EKPO_GSAP_CHEM_proj = spark.read.format("delta").load(Path_EKPO_GSAP_CHEM)
df_EKKO_GSAP_CHEM_proj = spark.read.format("delta").load(Path_EKKO_GSAP_CHEM)
df_EKKO_GSAP_CHEM_proj = df_EKKO_GSAP_CHEM_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_GSAP_CHEM_proj = spark.read.format("delta").load(Path_ESSR_GSAP_CHEM)
df_ESSR_GSAP_CHEM_proj = df_ESSR_GSAP_CHEM_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_GSAP_CHEM_proj = spark.read.format("delta").load(Path_RSEG_GSAP_CHEM)
df_SWW_WI2OBJ_GSAP_CHEM_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_GSAP_CHEM)
df_SWWIHEAD_GSAP_CHEM_proj = spark.read.format("delta").load(Path_SWWWIHEAD_GSAP_CHEM)
df_SWWWIAGENT_GSAP_CHEM_proj = spark.read.format("delta").load(Path_SWWWIAGENT_GSAP_CHEM)

# COMMAND ----------

#Creating two set of Company Codes, one having no filter requied and in other Vendor_Account_Group Filter needs to be applied as per busniess

#Filter needs to be Applied 
company_codes_GSAP_CHEM_filter = ['AE01','GB01','GB02','GB03','NL01','NL02','NL03','TR01','ZA01']

#No Filter Required
company_codes_GSAP_CHEM_nofilter = ['CN01','KR01','KR02','NL06','PH01','SG01','SG02','SG03','SG04','SG05','CA01','US30','US32','US55','US64','US87','USF3','USJ2','USL2','USN1',
                                    'USN2','USP1','UST8']

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_GSAP_CHEM=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP CHEMS').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_GSAP_CHEM=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP CHEMS').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_GSAP_CHEM=list(df_datafilter.where(df_datafilter.ERP_NAME=='GSAP CHEMS').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])
 
############################# Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_GSAP_CHEM=df_BSEG_GSAP_CHEM_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'CLIENT_ID','NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY').orderBy('FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CLIENT_ID',col('LAST_DATETIME').desc()).drop_duplicates(["FISCAL_YEAR","ACCOUNTING_DOCUMENT_NUMBER","COMPANY_CODE","ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER","CLIENT_ID"])
                                     
df_BSEG_req_GSAP_CHEM = df_BSEG_req_GSAP_CHEM.filter(col('COMPANY_CODE').isin(company_codes_GSAP_CHEM))
df_BSEG_req_GSAP_CHEM = df_BSEG_req_GSAP_CHEM.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_GSAP_CHEM = df_BSEG_req_GSAP_CHEM.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_GSAP_CHEM = df_BSEG_req_GSAP_CHEM.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_GSAP_CHEM = df_BSEG_req_GSAP_CHEM.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_GSAP_CHEM = df_BSEG_GSAP_CHEM.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_GSAP_CHEM))


df_VBSEGK_req_GSAP_CHEM=df_VBSEGK_GSAP_CHEM_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_GSAP_CHEM = df_VBSEGK_req_GSAP_CHEM.filter(col('COMPANY_CODE').isin(company_codes_GSAP_CHEM))
df_VBSEGK_req_GSAP_CHEM = df_VBSEGK_req_GSAP_CHEM.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_GSAP_CHEM = df_VBSEGK_req_GSAP_CHEM.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_GSAP_CHEM = df_VBSEGK_GSAP_CHEM.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_GSAP_CHEM))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_GSAP_CHEM_filtered = df_VBSEGK_GSAP_CHEM.join(df_BSEG_GSAP_CHEM, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_GSAP_CHEM = df_BSEG_GSAP_CHEM.unionByName(df_VBSEGK_GSAP_CHEM_filtered, allowMissingColumns = True)


# df_BSEG_Merged_GSAP_CHEM = df_BSEG_GSAP_CHEM.unionByName(df_VBSEGK_GSAP_CHEM, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################




df_BKPF_req_GSAP_CHEM=df_BKPF_GSAP_CHEM_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_GSAP_CHEM=df_BKPF_req_GSAP_CHEM.filter(col('COMPANY_CODE').isin(company_codes_GSAP_CHEM))
df_BKPF_GSAP_CHEM = df_BKPF_req_GSAP_CHEM.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_GSAP_CHEM=df_VBKPF_GSAP_CHEM_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_GSAP_CHEM = df_VBKPF_req_GSAP_CHEM.filter(col('COMPANY_CODE').isin(company_codes_GSAP_CHEM))
df_VBKPF_GSAP_CHEM = df_VBKPF_req_GSAP_CHEM.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBKPF_GSAP_CHEM_filtered = df_VBKPF_GSAP_CHEM.join(df_BKPF_GSAP_CHEM, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BKPF_Merged_GSAP_CHEM = df_BKPF_GSAP_CHEM.unionByName(df_VBKPF_GSAP_CHEM_filtered, allowMissingColumns = True)


# df_BKPF_Merged_GSAP_CHEM = df_BKPF_GSAP_CHEM.unionByName(df_VBKPF_GSAP_CHEM,allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################


 


df_LFA1_req_GSAP_CHEM=df_LFA1_GSAP_CHEM_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_GSAP_CHEM=df_LFA1_req_GSAP_CHEM.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_GSAP_CHEM)) #Newly added

df_LFB1_GSAP_CHEM=df_LFB1_GSAP_CHEM_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_GSAP_CHEM=df_EKPO_GSAP_CHEM_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_GSAP_CHEM=df_EKKO_GSAP_CHEM_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_GSAP_CHEM = df_ESSR_GSAP_CHEM_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_GSAP_CHEM = df_RSEG_GSAP_CHEM_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_GSAP_CHEM=df_RSEG_req_GSAP_CHEM.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_GSAP_CHEM =['ED','RE','RI','RN','RZ','RC','RT','ZP']
doc_type_liv_GSAP_CHEM =['AA','AB','AC','AF','AO','CV','DA','DG','DR','DZ','ED','HA','HG','IT','KA','KC','KG','KM','KN','KP','KR','KU','KW','KX',
'KY','KZ','ML','PC','PR','RC','RE','RH','RI','RN','RR','RT','RV','RX','RY','RZ','SA','SB','SD','SE','SR','T2','TE','TG','TR','UG','UP','WA','WE','WI','WL','X1','Y8','Z5','Z6','Z7','Z8','ZA','ZB','ZC','ZG','ZK','ZM','ZP','ZR','ZT']


df_BKPF_LIV_GSAP_CHEM = df_BKPF_Merged_GSAP_CHEM.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_GSAP_CHEM))
df_BKPF_LIV_GSAP_CHEM = df_BKPF_LIV_GSAP_CHEM.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Joining BKPF and BSEG
df_BKPF_BSEG_GSAP_CHEM = df_BKPF_LIV_GSAP_CHEM.join(df_BSEG_Merged_GSAP_CHEM,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_GSAP_CHEM = df_BKPF_BSEG_GSAP_CHEM.join(df_RSEG_GSAP_CHEM,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_GSAP_CHEM = df_BKPF_BSEG_RSEG_GSAP_CHEM.join(df_ESSR_GSAP_CHEM,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP_CHEM = df_BKPF_BSEG_RSEG_ESSR_GSAP_CHEM.join(df_EKPO_GSAP_CHEM ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_GSAP_CHEM = df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP_CHEM.join(df_LFA1_GSAP_CHEM, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_GSAP_CHEM.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_GSAP_CHEM.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_GSAP_CHEM = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_GSAP_CHEM.join(df_LFB1_GSAP_CHEM,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join With EKKO
df_LIV_GSAP_CHEM = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_GSAP_CHEM.join(df_EKKO_GSAP_CHEM, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_GSAP_CHEM = df_LIV_GSAP_CHEM.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_GSAP_CHEM=['KA','KG','KN','KR','KW','KZ','TR','UG','UP','ZA','KC','AB','X1','ZP']
doc_type_fi_GSAP_CHEM =['AA','AB','AC','AF','AO','CV','DA','DG','DR','DZ','ED','HA','HG','IT','KA','KC','KG','KM','KN','KP','KR','KU','KW','KX',
'KY','KZ','ML','PC','PR','RC','RE','RH','RI','RN','RR','RT','RV','RX','RY','RZ','SA','SB','SD','SE','SR','T2','TE','TG','TR','UG','UP','WA','WE','WI','WL','X1','Y8','Z5','Z6','Z7','Z8','ZA','ZB','ZC','ZG','ZK','ZM','ZP','ZR','ZT']


#Filtering out BKPF for FI document types
df_BKPF_FI_GSAP_CHEM=df_BKPF_Merged_GSAP_CHEM.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_GSAP_CHEM))

#Join BSEG and BKPF
df_BSEG_BKPF_FI_GSAP_CHEM = df_BSEG_Merged_GSAP_CHEM.join(df_BKPF_FI_GSAP_CHEM, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_GSAP_CHEM = df_BSEG_BKPF_FI_GSAP_CHEM.join(df_LFA1_GSAP_CHEM, df_BSEG_BKPF_FI_GSAP_CHEM.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_GSAP_CHEM.VENDOR_ID, how="left")

#Join with LFB1
df_FI_GSAP_CHEM = df_BSEG_BKPF_LFA1_FI_GSAP_CHEM.join(df_LFB1_GSAP_CHEM, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_GSAP_CHEM=df_FI_GSAP_CHEM.join(df_LIV_GSAP_CHEM, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_GSAP_CHEM = df_FI_anti_GSAP_CHEM.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_GSAP_CHEM = df_LIV_GSAP_CHEM.unionByName(df_FI_anti_GSAP_CHEM, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_GSAP_CHEM = df_Final_LIVFI_GSAP_CHEM.withColumn("ERP_NAME",lit('GSAP_CHEM'))
df_Final_LIVFI_GSAP_CHEM = df_Final_LIVFI_GSAP_CHEM.withColumn("ERP",lit('P53'))

# COMMAND ----------

  #Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_GSAP_CHEM = df_Final_LIVFI_GSAP_CHEM.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_GSAP_CHEM = df_ERS_Cal_1_GSAP_CHEM.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_GSAP_CHEM = df_ERS_Cal_2_GSAP_CHEM.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1).otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe
MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_GSAP_CHEM =df_ERS_Cal_3_GSAP_CHEM.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_GSAP_CHEM.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_GSAP_CHEM = df_ERS_Cal_Master_GSAP_CHEM.withColumn("FISCAL_YEAR", df_ERS_Cal_3_GSAP_CHEM["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn("FISCAL_PERIOD", df_Final1_GSAP_CHEM["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_GSAP_CHEM = df_Final1_GSAP_CHEM.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_GSAP_CHEM = df_Final_DTypeC_GSAP_CHEM.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_GSAP_CHEM = df_lt18_GSAP_CHEM.withColumn("REFERENCE_KEY_ID_tw",df_lt18_GSAP_CHEM.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_GSAP_CHEM = df_Final_DTypeC_GSAP_CHEM.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_GSAP_CHEM = df_eq18_GSAP_CHEM.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_GSAP_CHEM = df_eq18_awkey_GSAP_CHEM.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_GSAP_CHEM = df_eq18_awkey_GSAP_CHEM.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_GSAP_CHEM = df_eq18_awkey_GSAP_CHEM.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_GSAP_CHEM = df_lt18_GSAP_CHEM.unionByName(df_eq18_awkey_GSAP_CHEM, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_GSAP_CHEM = df_SWW_WI2OBJ_GSAP_CHEM_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_GSAP_CHEM = df_SWW_WI2OBJ_req_GSAP_CHEM.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_GSAP_CHEM = df_SWW_WI2OBJ_req_GSAP_CHEM.drop(col("TASK_ID"))


df_SWWIHEAD_req_GSAP_CHEM=df_SWWIHEAD_GSAP_CHEM_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_GSAP_CHEM = df_SWWIHEAD_req_GSAP_CHEM.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_GSAP_CHEM = df_SWWWIAGENT_GSAP_CHEM_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_GSAP_CHEM = df_Final_Finance_GSAP_CHEM.join(df_SWW_WI2OBJ_GSAP_CHEM,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_GSAP_CHEM_isNull = df_Finance_SWWOBJ_GSAP_CHEM.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_GSAP_CHEM_NotNull = df_Finance_SWWOBJ_GSAP_CHEM.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_GSAP_CHEM_NotNull = df_Finance_SWWOBJ_GSAP_CHEM_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_GSAP_CHEM_NotNullfiltered = df_Finance_SWWOBJ_GSAP_CHEM_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM = df_Finance_SWWOBJ_GSAP_CHEM_NotNullfiltered.join(df_SWWIHEAD_GSAP_CHEM,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM = df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM = df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_GSAP_CHEM_NotNull = df_Finance_SWWOBJ_SWWHEAD_GSAP_CHEM.join(df_SWWWIAGENT_GSAP_CHEM,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_GSAP_CHEM_NotNull = df_Finance_WORKFLOW_GSAP_CHEM.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_GSAP_CHEM = df_Finance_WORKFLOW_GSAP_CHEM_NotNull.unionByName(df_Finance_SWWOBJ_GSAP_CHEM_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_GSAP_CHEM1 = df_Finance_WORKFLOW_Final_GSAP_CHEM.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_GSAP_CHEM1 = df_Finance_WORKFLOW_Final_GSAP_CHEM1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_GSAP_CHEM1=df_Final_GSAP_CHEM1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
# df_Final_GSAP_CHEM1=df_Final_GSAP_CHEM1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_GSAP_CHEM)) #Newly Added

df_Final_Currency_GSAP_CHEM = df_Final_GSAP_CHEM1.join(df_CurrencyDocAmount, [df_Final_GSAP_CHEM1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_GSAP_CHEM1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_GSAP_CHEM = df_Final_Currency_GSAP_CHEM.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_GSAP_CHEM = df_Final_Currency_GSAP_CHEM.drop(*columns_to_drop)
df_Final_DS_GSAP_CHEM = df_Final_Currency_GSAP_CHEM.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

###############Creating two DataFrame on the basis of COMPANY_CODE list created above.(This Scenario is Only for GSAP_CHEM and STN)#######

# Creating DataSet in having Company_Codes in which VENDOR_OR_CREDITOR_ACCOUNT_NUMBER Filter needs to be applied
df_Final_DS_GSAP_CHEM_filtered1 = df_Final_DS_GSAP_CHEM.filter(col("COMPANY_CODE").isin(company_codes_GSAP_CHEM_filter))

# Filtering VENDOR_OR_CREDITOR_ACCOUNT_NUMBER on the above Dataset as Per Busniess Need
df_Final_DS_GSAP_CHEM_filtered = df_Final_DS_GSAP_CHEM_filtered1.filter(col("VENDOR_OR_CREDITOR_ACCOUNT_NUMBER").between('0000800000','0000899999') | col("VENDOR_OR_CREDITOR_ACCOUNT_NUMBER").isin(['0007011993']))    

# Creating DataSet in having Company_Codes in which No Filter is Required
# company_codes_GSAP_CHEM_nofilter
df_Final_DS_GSAP_CHEM_nofilter = df_Final_DS_GSAP_CHEM.filter(col("COMPANY_CODE").isin(company_codes_GSAP_CHEM_filter))

#Performing UNION on the two DataSet Created Above and Creating Final DataSet for GSAP_CHEM
df_Final_DS_GSAP_CHEM_custom_filter = df_Final_DS_GSAP_CHEM_nofilter.unionByName(df_Final_DS_GSAP_CHEM_filtered,allowMissingColumns = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_GSAP_CHEM#

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_WITH_GSAP_CHEM

# COMMAND ----------

#Union with GSAP_CHEM
# df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM = df_Final_DS_SERP_BG_CRIETRION_GSAP.unionByName(df_Final_DS_GSAP_CHEM)
df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM = df_Final_DS_SERP_BG_CRIETRION_GSAP.unionByName(df_Final_DS_GSAP_CHEM_custom_filter)


# COMMAND ----------

# MAGIC %md
# MAGIC #STN(TRADING_FINANCE)#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
        "EDAM_PATH").collect()[0][0]
Path_BKPF_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "BKPF")).select("EDAM_PATH").collect()[0][0]

Path_VBSEGK_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 
  
Path_LFA1_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "LFA1")).select("EDAM_PATH").collect()[0][0]

Path_LFB1_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "LFB1")).select("EDAM_PATH").collect()[0][0]
Path_EKPO_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "EKPO")).select("EDAM_PATH").collect()[0][0]
Path_EKKO_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "EKKO")).select("EDAM_PATH").collect()[0][0]

Path_ESSR_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "ESSR")).select("EDAM_PATH").collect()[0][0]
Path_RSEG_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "RSEG")).select("EDAM_PATH").collect()[0][0]

Path_SWW_WI2OBJ_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select("EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select( "EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_STN = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "STN") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select("EDAM_PATH").collect()[0][0]


# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_STN_proj = spark.read.format("delta").load(Path_BSEG_STN)
df_BKPF_STN_proj = spark.read.format("delta").load(Path_BKPF_STN)
df_VBSEGK_STN_proj = spark.read.format("delta").load(Path_VBSEGK_STN)
df_VBKPF_STN_proj = spark.read.format("delta").load(Path_VBKPF_STN)
df_LFA1_STN_proj = spark.read.format("delta").load(Path_LFA1_STN)
df_LFB1_STN_proj = spark.read.format("delta").load(Path_LFB1_STN)
df_EKPO_STN_proj = spark.read.format("delta").load(Path_EKPO_STN)
df_EKKO_STN_proj = spark.read.format("delta").load(Path_EKKO_STN)
df_EKKO_STN_proj = df_EKKO_STN_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_STN_proj = spark.read.format("delta").load(Path_ESSR_STN)
df_ESSR_STN_proj = df_ESSR_STN_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_STN_proj = spark.read.format("delta").load(Path_RSEG_STN)
df_SWW_WI2OBJ_STN_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_STN)
df_SWWIHEAD_STN_proj = spark.read.format("delta").load(Path_SWWWIHEAD_STN)
df_SWWWIAGENT_STN_proj = spark.read.format("delta").load(Path_SWWWIAGENT_STN)

# COMMAND ----------

#Creating two set of Company Codes, one having no filter requied and in other Vendor_Account_Group Filter needs to be applied as per busniess

#Filter needs to be Applied 
company_codes_STN_range =['1001','1021','1022','1023','1025','1031','1037','1080','1084','1086','1903','3100','BGEG','BGEL','BGGC','BGGM','BGLS',
'BGMS','BGSA','BSL1','DE02','DE03','ES01','HLNG','IT01','RLNG','ROTT','SAT1','SAT2','SEBV','SEGL','SEIP','SITM','SJTL','SLNG''STAS','STBR','STRU','STSZ','STUG','STUK','SWST','TR01']

#No Filter Required
company_codes_STN_norange = ['BGGE','BGTS','CLNG','DLNG','ELNG','SAEP','SEAU','SECL','SECO','SEEP','SEID','SEMY','SEPH','SETI','SETL','SG02','SG39',
'SISS','STAP','STSG','STSL','TLNG']

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

## Selecting Columns as Per Business requirement and Filtering the Records.
company_codes_STN=list(df_datafilter.where(df_datafilter.ERP_NAME=='STN').select('COM_CODE').distinct().toPandas()['COM_CODE'])

# #Selecting General ledger account number
# recon_account_STN=list(df_datafilter.where(df_datafilter.ERP_NAME=='STN').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_STN=list(df_datafilter.where(df_datafilter.ERP_NAME=='STN').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])
 
############################## Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_STN=df_BSEG_STN_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_STN = df_BSEG_req_STN.filter(col('COMPANY_CODE').isin(company_codes_STN))
df_BSEG_req_STN = df_BSEG_req_STN.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_STN = df_BSEG_req_STN.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_STN = df_BSEG_req_STN.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_STN = df_BSEG_req_STN.filter(col('FISCAL_YEAR')>=year_2)
# df_BSEG_STN = df_BSEG_STN.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_STN))

df_VBSEGK_req_STN=df_VBSEGK_STN_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_STN = df_VBSEGK_req_STN.filter(col('COMPANY_CODE').isin(company_codes_STN))
df_VBSEGK_req_STN = df_VBSEGK_req_STN.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_STN = df_VBSEGK_req_STN.filter(col('FISCAL_YEAR')>=year_2)
# df_VBSEGK_STN = df_VBSEGK_STN.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_STN))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_STN_filtered = df_VBSEGK_STN.join(df_BSEG_STN, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_STN = df_BSEG_STN.unionByName(df_VBSEGK_STN_filtered, allowMissingColumns = True)


# df_BSEG_Merged_STN = df_BSEG_STN.unionByName(df_VBSEGK_STN, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################



######################################### START_OF_BKPF_and_VBKPF ##############################################################################
df_BKPF_req_STN=df_BKPF_STN_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_STN=df_BKPF_req_STN.filter(col('COMPANY_CODE').isin(company_codes_STN))
df_BKPF_STN = df_BKPF_req_STN.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_STN=df_VBKPF_STN_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_STN = df_VBKPF_req_STN.filter(col('COMPANY_CODE').isin(company_codes_STN))
df_VBKPF_STN = df_VBKPF_req_STN.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################
#filtering records present in VBKPF only
df_VBKPF_STN_filtered = df_VBKPF_STN.join(df_BKPF_STN, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_STN = df_BKPF_STN.unionByName(df_VBKPF_STN_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_STN = df_BKPF_STN.unionByName(df_VBKPF_STN,allowMissingColumns = True) 
######################################### END OF BKPF ########################################################################################



df_LFA1_req_STN=df_LFA1_STN_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_STN=df_LFA1_req_STN.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_STN)) #Newly added

df_LFB1_STN=df_LFB1_STN_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_STN=df_EKPO_STN_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_STN=df_EKKO_STN_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_STN = df_ESSR_STN_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_STN = df_RSEG_STN_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_STN=df_RSEG_req_STN.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_STN =['RE','RN','S1','S2','S4','S5','SU','ZP']
doc_type_liv_STN['A1','AA','AB','AC','AF','AT','DA','DG','DR','DZ','EM','FV','GA','GL','GR','IL','IM','KG','KN','KP','KR','MA','MI','QS','RA','RC','RE','RN','RV','S1','S2','S3','S4','S5','SI','SL','SQ','SU','WA''WE','XC','X1','X2','X3','X4','X5','X6','XF','XG','XH','XK','XL','Y0',
'Y1','Y2','Y3','Y4','Y5','Y6','Y7','Y8','Y9','YA','YB','YC','YD','YE','YF','YG','YH','YI','YL','YN','YO','YP','YQ','YR','YS','YT','YU','YV','YW','YX','YY','YZ','Z1','Z2','Z3','Z4','Z5','ZA','ZB','ZD','ZE','ZF','ZG','ZH','ZK','ZL','ZM','ZO','ZQ','ZR','ZS','ZT','ZV','ZX','ZY']

df_BKPF_LIV_STN = df_BKPF_Merged_STN.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_STN))
df_BKPF_LIV_STN = df_BKPF_LIV_STN.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Joining with BKPF and BSEG
df_BKPF_BSEG_STN = df_BKPF_LIV_STN.join(df_BSEG_Merged_STN,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Joining with RSEG
df_BKPF_BSEG_RSEG_STN = df_BKPF_BSEG_STN.join(df_RSEG_STN,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Joining with ESSR
df_BKPF_BSEG_RSEG_ESSR_STN = df_BKPF_BSEG_RSEG_STN.join(df_ESSR_STN,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Joining with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_STN = df_BKPF_BSEG_RSEG_ESSR_STN.join(df_EKPO_STN ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_STN = df_BKPF_BSEG_RSEG_ESSR_EKPO_STN.join(df_LFA1_STN, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_STN.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_STN.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_STN = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_STN.join(df_LFB1_STN,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_STN = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_STN.join(df_EKKO_STN, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_STN = df_LIV_STN.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_STN=['A1','KG','KN','KR','S3','Y9','YB','YG','YI','YS','AB','X1','ZP','XC','X1']
doc_type_fi_STN = ['A1','AA','AB','AC','AF','AT','DA','DG','DR','DZ','EM','FV','GA','GL','GR','IL','IM','KG','KN','KP','KR','MA','MI','QS','RA',
'RC','RE','RN','RV','S1','S2','S3','S4','S5','SI','SL','SQ','SU','WA''WE','XC','X1','X2','X3','X4','X5','X6','XF','XG','XH','XK','XL','Y0',
'Y1','Y2','Y3','Y4','Y5','Y6','Y7','Y8','Y9','YA','YB','YC','YD','YE','YF','YG','YH','YI','YL','YN','YO','YP','YQ','YR','YS','YT','YU','YV','YW','YX','YY','YZ','Z1','Z2','Z3','Z4','Z5','ZA','ZB','ZD','ZE','ZF','ZG','ZH','ZK','ZL','ZM','ZO','ZQ','ZR','ZS','ZT','ZV','ZX','ZY']

#Filtering out BKPF for FI document types
df_BKPF_FI_STN=df_BKPF_Merged_STN.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_STN))

#Joining BSEG and BKPF
df_BSEG_BKPF_FI_STN = df_BSEG_Merged_STN.join(df_BKPF_FI_STN, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_STN = df_BSEG_BKPF_FI_STN.join(df_LFA1_STN, df_BSEG_BKPF_FI_STN.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_STN.VENDOR_ID, how="left")

#Join with LFB1
df_FI_STN = df_BSEG_BKPF_LFA1_FI_STN.join(df_LFB1_STN, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_STN=df_FI_STN.join(df_LIV_STN, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_STN = df_FI_anti_STN.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_STN = df_LIV_STN.unionByName(df_FI_anti_STN, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_STN = df_Final_LIVFI_STN.withColumn("ERP_NAME",lit('STNT-Trading Finesse'))
df_Final_LIVFI_STN = df_Final_LIVFI_STN.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_STN = df_Final_LIVFI_STN.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_STN = df_ERS_Cal_1_STN.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_STN = df_ERS_Cal_2_STN.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe
MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"

df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')

df_ERS_Cal_Master_STN =df_ERS_Cal_3_STN.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_STN.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_STN = df_ERS_Cal_Master_STN.withColumn("FISCAL_YEAR", df_ERS_Cal_3_STN["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_STN = df_Final1_STN.withColumn("FISCAL_PERIOD", df_Final1_STN["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_STN = df_Final1_STN.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_STN = df_Final1_STN.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_STN = df_Final1_STN.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_STN = df_Final1_STN.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_STN = df_Final1_STN.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_STN = df_Final1_STN.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_STN = df_Final1_STN.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_STN = df_Final_DTypeC_STN.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_STN = df_lt18_STN.withColumn("REFERENCE_KEY_ID_tw",df_lt18_STN.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_STN = df_Final_DTypeC_STN.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_STN = df_eq18_STN.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_STN = df_eq18_awkey_STN.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_STN = df_eq18_awkey_STN.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_STN = df_eq18_awkey_STN.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_STN = df_lt18_STN.unionByName(df_eq18_awkey_STN, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_STN = df_SWW_WI2OBJ_STN_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_STN = df_SWW_WI2OBJ_req_STN.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_STN = df_SWW_WI2OBJ_req_STN.drop(col("TASK_ID"))


df_SWWIHEAD_req_STN=df_SWWIHEAD_STN_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_STN = df_SWWIHEAD_req_STN.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_STN = df_SWWWIAGENT_STN_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_STN = df_Final_Finance_STN.join(df_SWW_WI2OBJ_STN,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_STN_isNull = df_Finance_SWWOBJ_STN.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_STN_NotNull = df_Finance_SWWOBJ_STN.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_STN_NotNull = df_Finance_SWWOBJ_STN_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_STN_NotNullfiltered = df_Finance_SWWOBJ_STN_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_STN = df_Finance_SWWOBJ_STN_NotNullfiltered.join(df_SWWIHEAD_STN,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_STN = df_Finance_SWWOBJ_SWWHEAD_STN.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_STN = df_Finance_SWWOBJ_SWWHEAD_STN.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_STN_NotNull = df_Finance_SWWOBJ_SWWHEAD_STN.join(df_SWWWIAGENT_STN,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_STN_NotNull = df_Finance_WORKFLOW_STN.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_STN = df_Finance_WORKFLOW_STN_NotNull.unionByName(df_Finance_SWWOBJ_STN_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_STN1 = df_Finance_WORKFLOW_Final_STN.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_STN1 = df_Finance_WORKFLOW_Final_STN1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_STN1=df_Final_STN1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_STN1=df_Final_STN1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_STN)) #Newly Added

df_Final_Currency_STN = df_Final_STN1.join(df_CurrencyDocAmount, [df_Final_STN1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_STN1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_STN = df_Final_Currency_STN.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_STN = df_Final_Currency_STN.drop(*columns_to_drop)
df_Final_DS_STN = df_Final_Currency_STN.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

####### Creating two DataFrame on the basis of COMPANY_CODE list created above.(This Scenario is Only for GSAP_CHEM and STN) #################

# Creating DataSet in having Company_Codes in which VENDOR_OR_CREDITOR_ACCOUNT_NUMBER Filter needs to be applied
df_Final_DS_STN_filtered = df_Final_DS_STN.filter(col("COMPANY_CODE").isin(company_codes_STN_range) & col('VENDOR_OR_CREDITOR_ACCOUNT_NUMBER').between('0002000000','0002999999'))


# Creating DataSet in having Company_Codes in which No Filter is Required
# company_codes_GSAP_CHEM_nofilter
df_Final_DS_STN_nofilter = df_Final_DS_STN.filter(col("COMPANY_CODE").isin(company_codes_STN_norange))


#Performing UNION on the two DataSet Created Above and Creating Final DataSet for GSAP_CHEM
df_Final_DS_STN_final_filtered = df_Final_DS_STN_filtered.unionByName(df_Final_DS_STN_nofilter,allowMissingColumns = True) 

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_STN

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_OF_ERPs_WITH_STN

# COMMAND ----------

# df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN = df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM.unionByName(df_Final_DS_STN)
df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN = df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM.unionByName(df_Final_DS_STN_final_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC #CRYSTAL#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "BSEG")).select("EDAM_PATH").collect()[0][0]
Path_BKPF_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "BKPF")).select("EDAM_PATH").collect()[0][0]

Path_VBSEGK_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 
    
Path_LFA1_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "LFA1")).select("EDAM_PATH").collect()[0][0]
Path_LFB1_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "LFB1")).select("EDAM_PATH").collect()[0][0]
Path_EKPO_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "EKPO")).select("EDAM_PATH").collect()[0][0]
Path_EKKO_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "EKKO")).select("EDAM_PATH").collect()[0][0]

Path_ESSR_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "ESSR")).select("EDAM_PATH").collect()[0][0]
Path_RSEG_CRYSTAL = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "RSEG")).select("EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_CRYSTAL = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIHEAD_CRYSTAL = \
  df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_CRYSTAL = \
 df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "CRYSTAL") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select("EDAM_PATH").collect()[0][0]



# COMMAND ----------

# print(Path_BSEG_CRYSTAL)
# print(Path_BKPF_CRYSTAL)
# print(Path_VBSEGK_CRYSTAL)
# print(Path_VBKPF_CRYSTAL)
# print(Path_LFA1_CRYSTAL)
# print(Path_LFB1_CRYSTAL)
# print(Path_EKPO_CRYSTAL)
# print(Path_EKKO_CRYSTAL)
# print(Path_ESSR_CRYSTAL)
# print(Path_RSEG_CRYSTAL)
# print(Path_SWW_WI2OBJ_CRYSTAL)
# print(Path_SWWWIAGENT_CRYSTAL)
# print(Path_SWWWIHEAD_CRYSTAL)

# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_CRYSTAL_proj = spark.read.format("delta").load(Path_BSEG_CRYSTAL)
df_BKPF_CRYSTAL_proj = spark.read.format("delta").load(Path_BKPF_CRYSTAL)
df_VBSEGK_CRYSTAL_proj = spark.read.format("delta").load(Path_VBSEGK_CRYSTAL)
df_VBKPF_CRYSTAL_proj = spark.read.format("delta").load(Path_VBKPF_CRYSTAL)
df_LFA1_CRYSTAL_proj = spark.read.format("delta").load(Path_LFA1_CRYSTAL)
df_LFB1_CRYSTAL_proj = spark.read.format("delta").load(Path_LFB1_CRYSTAL)
df_EKPO_CRYSTAL_proj = spark.read.format("delta").load(Path_EKPO_CRYSTAL)
df_EKKO_CRYSTAL_proj = spark.read.format("delta").load(Path_EKKO_CRYSTAL)
df_EKKO_CRYSTAL_proj = df_EKKO_CRYSTAL_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_CRYSTAL_proj = spark.read.format("delta").load(Path_ESSR_CRYSTAL)
df_ESSR_CRYSTAL_proj = df_ESSR_CRYSTAL_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_CRYSTAL_proj = spark.read.format("delta").load(Path_RSEG_CRYSTAL)
df_SWW_WI2OBJ_CRYSTAL_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_CRYSTAL)
df_SWWIHEAD_CRYSTAL_proj = spark.read.format("delta").load(Path_SWWWIHEAD_CRYSTAL)
df_SWWWIAGENT_CRYSTAL_proj = spark.read.format("delta").load(Path_SWWWIAGENT_CRYSTAL)

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_CRYSTAL=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRYSTAL').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_CRYSTAL=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRYSTAL').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_CRYSTAL=list(df_datafilter.where(df_datafilter.ERP_NAME=='CRYSTAL').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])
 

############################### Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_CRYSTAL=df_BSEG_CRYSTAL_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_CRYSTAL = df_BSEG_req_CRYSTAL.filter(col('COMPANY_CODE').isin(company_codes_CRYSTAL))
df_BSEG_req_CRYSTAL = df_BSEG_req_CRYSTAL.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_CRYSTAL = df_BSEG_req_CRYSTAL.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_CRYSTAL = df_BSEG_req_CRYSTAL.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_CRYSTAL = df_BSEG_req_CRYSTAL.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_CRYSTAL = df_BSEG_CRYSTAL.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_CRYSTAL))

df_VBSEGK_req_CRYSTAL=df_VBSEGK_CRYSTAL_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_CRYSTAL = df_VBSEGK_req_CRYSTAL.filter(col('COMPANY_CODE').isin(company_codes_CRYSTAL))
df_VBSEGK_req_CRYSTAL = df_VBSEGK_req_CRYSTAL.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_CRYSTAL = df_VBSEGK_req_CRYSTAL.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_CRYSTAL = df_VBSEGK_CRYSTAL.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_CRYSTAL))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_CRYSTAL_filtered = df_VBSEGK_CRYSTAL.join(df_BSEG_CRYSTAL, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_CRYSTAL = df_BSEG_CRYSTAL.unionByName(df_VBSEGK_CRYSTAL_filtered, allowMissingColumns = True)


# df_BSEG_Merged_CRYSTAL = df_BSEG_CRYSTAL.unionByName(df_VBSEGK_CRYSTAL, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################




df_BKPF_req_CRYSTAL=df_BKPF_CRYSTAL_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_CRYSTAL=df_BKPF_req_CRYSTAL.filter(col('COMPANY_CODE').isin(company_codes_CRYSTAL))
df_BKPF_CRYSTAL = df_BKPF_req_CRYSTAL.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_CRYSTAL=df_VBKPF_CRYSTAL_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_CRYSTAL = df_VBKPF_req_CRYSTAL.filter(col('COMPANY_CODE').isin(company_codes_CRYSTAL))
df_VBKPF_CRYSTAL = df_VBKPF_req_CRYSTAL.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#filtering records present in VBKPF only
df_VBKPF_CRYSTAL_filtered = df_VBKPF_CRYSTAL.join(df_BKPF_CRYSTAL, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_CRYSTAL = df_BKPF_CRYSTAL.unionByName(df_VBKPF_CRYSTAL_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_CRYSTAL = df_BKPF_CRYSTAL.unionByName(df_VBKPF_CRYSTAL,allowMissingColumns = True)
######################################### END OF BKPF ########################################################################################

 


df_LFA1_req_CRYSTAL=df_LFA1_CRYSTAL_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_CRYSTAL=df_LFA1_req_CRYSTAL.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_CRYSTAL)) #Newly added

df_LFB1_CRYSTAL=df_LFB1_CRYSTAL_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_CRYSTAL=df_EKPO_CRYSTAL_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_CRYSTAL=df_EKKO_CRYSTAL_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_CRYSTAL = df_ESSR_CRYSTAL_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_CRYSTAL = df_RSEG_CRYSTAL_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_CRYSTAL=df_RSEG_req_CRYSTAL.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_CRYSTAL =['RC','RD','RM','RE']
doc_type_liv_CRYSTAL = ['FK','FG','KR','KG','RC','RD','RM','RE']
df_BKPF_LIV_CRYSTAL = df_BKPF_Merged_CRYSTAL.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_CRYSTAL))
df_BKPF_LIV_CRYSTAL = df_BKPF_LIV_CRYSTAL.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Join BKPF and BSEG
df_BKPF_BSEG_CRYSTAL = df_BKPF_LIV_CRYSTAL.join(df_BSEG_Merged_CRYSTAL,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_CRYSTAL = df_BKPF_BSEG_CRYSTAL.join(df_RSEG_CRYSTAL,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_CRYSTAL = df_BKPF_BSEG_RSEG_CRYSTAL.join(df_ESSR_CRYSTAL,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_CRYSTAL = df_BKPF_BSEG_RSEG_ESSR_CRYSTAL.join(df_EKPO_CRYSTAL ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_CRYSTAL = df_BKPF_BSEG_RSEG_ESSR_EKPO_CRYSTAL.join(df_LFA1_CRYSTAL, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_CRYSTAL.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_CRYSTAL.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_CRYSTAL = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_CRYSTAL.join(df_LFB1_CRYSTAL,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_CRYSTAL = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_CRYSTAL.join(df_EKKO_CRYSTAL, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_CRYSTAL = df_LIV_CRYSTAL.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_CRYSTAL=['FK','FG','KR','KG']
doc_type_fi_CRYSTAL = ['FK','FG','KR','KG','RC','RD','RM','RE']

#Filtering out BKPF for FI document types
df_BKPF_FI_CRYSTAL=df_BKPF_Merged_CRYSTAL.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_CRYSTAL))

#Joining BSEG and BKPF
df_BSEG_BKPF_FI_CRYSTAL = df_BSEG_Merged_CRYSTAL.join(df_BKPF_FI_CRYSTAL, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_CRYSTAL = df_BSEG_BKPF_FI_CRYSTAL.join(df_LFA1_CRYSTAL, df_BSEG_BKPF_FI_CRYSTAL.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_CRYSTAL.VENDOR_ID, how="left")

#Join with LFB1
df_FI_CRYSTAL = df_BSEG_BKPF_LFA1_FI_CRYSTAL.join(df_LFB1_CRYSTAL, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_CRYSTAL=df_FI_CRYSTAL.join(df_LIV_CRYSTAL, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_CRYSTAL = df_FI_anti_CRYSTAL.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_CRYSTAL = df_LIV_CRYSTAL.unionByName(df_FI_anti_CRYSTAL, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_CRYSTAL = df_Final_LIVFI_CRYSTAL.withColumn("ERP_NAME",lit('CRYSTAL'))
df_Final_LIVFI_CRYSTAL = df_Final_LIVFI_CRYSTAL.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_CRYSTAL = df_Final_LIVFI_CRYSTAL.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_CRYSTAL = df_ERS_Cal_1_CRYSTAL.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_CRYSTAL = df_ERS_Cal_2_CRYSTAL.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_CRYSTAL =df_ERS_Cal_3_CRYSTAL.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_CRYSTAL.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_CRYSTAL = df_ERS_Cal_Master_CRYSTAL.withColumn("FISCAL_YEAR", df_ERS_Cal_3_CRYSTAL["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn("FISCAL_PERIOD", df_Final1_CRYSTAL["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_CRYSTAL = df_Final1_CRYSTAL.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_CRYSTAL = df_Final1_CRYSTAL.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_CRYSTAL = df_Final_DTypeC_CRYSTAL.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_CRYSTAL = df_lt18_CRYSTAL.withColumn("REFERENCE_KEY_ID_tw",df_lt18_CRYSTAL.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_CRYSTAL = df_Final_DTypeC_CRYSTAL.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_CRYSTAL = df_eq18_CRYSTAL.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_CRYSTAL = df_eq18_awkey_CRYSTAL.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_CRYSTAL = df_eq18_awkey_CRYSTAL.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_CRYSTAL = df_eq18_awkey_CRYSTAL.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_CRYSTAL = df_lt18_CRYSTAL.unionByName(df_eq18_awkey_CRYSTAL, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_CRYSTAL = df_SWW_WI2OBJ_CRYSTAL_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_CRYSTAL = df_SWW_WI2OBJ_req_CRYSTAL.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_CRYSTAL = df_SWW_WI2OBJ_req_CRYSTAL.drop(col("TASK_ID"))


df_SWWIHEAD_req_CRYSTAL=df_SWWIHEAD_CRYSTAL_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_CRYSTAL = df_SWWIHEAD_req_CRYSTAL.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_CRYSTAL = df_SWWWIAGENT_CRYSTAL_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_CRYSTAL = df_Final_Finance_CRYSTAL.join(df_SWW_WI2OBJ_CRYSTAL,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_CRYSTAL_isNull = df_Finance_SWWOBJ_CRYSTAL.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_CRYSTAL_NotNull = df_Finance_SWWOBJ_CRYSTAL.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_CRYSTAL_NotNull = df_Finance_SWWOBJ_CRYSTAL_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_CRYSTAL_NotNullfiltered = df_Finance_SWWOBJ_CRYSTAL_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_CRYSTAL = df_Finance_SWWOBJ_CRYSTAL_NotNullfiltered.join(df_SWWIHEAD_CRYSTAL,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_CRYSTAL = df_Finance_SWWOBJ_SWWHEAD_CRYSTAL.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_CRYSTAL = df_Finance_SWWOBJ_SWWHEAD_CRYSTAL.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_CRYSTAL_NotNull = df_Finance_SWWOBJ_SWWHEAD_CRYSTAL.join(df_SWWWIAGENT_CRYSTAL,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_CRYSTAL_NotNull = df_Finance_WORKFLOW_CRYSTAL.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_CRYSTAL = df_Finance_WORKFLOW_CRYSTAL_NotNull.unionByName(df_Finance_SWWOBJ_CRYSTAL_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_CRYSTAL1 = df_Finance_WORKFLOW_Final_CRYSTAL.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_CRYSTAL1 = df_Finance_WORKFLOW_Final_CRYSTAL1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_CRYSTAL1=df_Final_CRYSTAL1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_CRYSTAL1=df_Final_CRYSTAL1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_CRYSTAL)) #Newly Added

df_Final_Currency_CRYSTAL = df_Final_CRYSTAL1.join(df_CurrencyDocAmount, [df_Final_CRYSTAL1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_CRYSTAL1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_CRYSTAL = df_Final_Currency_CRYSTAL.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_CRYSTAL = df_Final_Currency_CRYSTAL.drop(*columns_to_drop)
df_Final_DS_CRYSTAL = df_Final_Currency_CRYSTAL.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_CRSYTAL#

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_WITH_CRYSTAL

# COMMAND ----------

#Union the whole datset with Crystal Dataset
df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL = df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN.unionByName(df_Final_DS_CRYSTAL)


# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING_MAUI#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "BSEG")).select("EDAM_PATH").collect()[0][0]
Path_BKPF_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "BKPF")).select("EDAM_PATH").collect()[0][0]

Path_VBSEGK_MAUI = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_MAUI = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 
    
Path_LFA1_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "LFA1")).select("EDAM_PATH").collect()[0][0]
Path_LFB1_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "LFB1")).select("EDAM_PATH").collect()[0][0]
Path_EKPO_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "EKPO")).select("EDAM_PATH").collect()[0][0]
Path_EKKO_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "EKKO")).select("EDAM_PATH").collect()[0][0]

Path_ESSR_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "ESSR")).select("EDAM_PATH").collect()[0][0]
Path_RSEG_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "RSEG")).select("EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_MAUI = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select("EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_MAUI = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select("EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_MAUI = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "MAUI") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select("EDAM_PATH").collect()[0][0]


# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_MAUI_proj = spark.read.format("delta").load(Path_BSEG_MAUI)
df_BKPF_MAUI_proj = spark.read.format("delta").load(Path_BKPF_MAUI)
df_VBSEGK_MAUI_proj = spark.read.format("delta").load(Path_VBSEGK_MAUI)
df_VBKPF_MAUI_proj = spark.read.format("delta").load(Path_VBKPF_MAUI)
df_LFA1_MAUI_proj = spark.read.format("delta").load(Path_LFA1_MAUI)
df_LFB1_MAUI_proj = spark.read.format("delta").load(Path_LFB1_MAUI)
df_EKPO_MAUI_proj = spark.read.format("delta").load(Path_EKPO_MAUI)
df_EKKO_MAUI_proj = spark.read.format("delta").load(Path_EKKO_MAUI)
df_EKKO_MAUI_proj = df_EKKO_MAUI_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_MAUI_proj = spark.read.format("delta").load(Path_ESSR_MAUI)
df_ESSR_MAUI_proj = df_ESSR_MAUI_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_MAUI_proj = spark.read.format("delta").load(Path_RSEG_MAUI)
df_SWW_WI2OBJ_MAUI_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_MAUI)
df_SWWIHEAD_MAUI_proj = spark.read.format("delta").load(Path_SWWWIHEAD_MAUI)
df_SWWWIAGENT_MAUI_proj = spark.read.format("delta").load(Path_SWWWIAGENT_MAUI)

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_MAUI=list(df_datafilter.where(df_datafilter.ERP_NAME=='S4 Hana').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_MAUI=list(df_datafilter.where(df_datafilter.ERP_NAME=='S4 Hana').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_MAUI=list(df_datafilter.where(df_datafilter.ERP_NAME=='S4 Hana').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])
 

############################### Creating Dataframe for each Table and selecting required columns as per business. ###########################.
df_BSEG_req_MAUI=df_BSEG_MAUI_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_MAUI = df_BSEG_req_MAUI.filter(col('COMPANY_CODE').isin(company_codes_MAUI))
df_BSEG_req_MAUI = df_BSEG_req_MAUI.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_MAUI = df_BSEG_req_MAUI.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_MAUI = df_BSEG_req_MAUI.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_MAUI = df_BSEG_req_MAUI.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_MAUI = df_BSEG_MAUI.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_MAUI))

df_VBSEGK_req_MAUI=df_VBSEGK_MAUI_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')

df_VBSEGK_req_MAUI = df_VBSEGK_req_MAUI.filter(col('COMPANY_CODE').isin(company_codes_MAUI))
df_VBSEGK_req_MAUI = df_VBSEGK_req_MAUI.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_MAUI = df_VBSEGK_req_MAUI.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_MAUI = df_VBSEGK_MAUI.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_MAUI))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_MAUI_filtered = df_VBSEGK_MAUI.join(df_BSEG_MAUI, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_MAUI = df_BSEG_MAUI.unionByName(df_VBSEGK_MAUI_filtered, allowMissingColumns = True)


# df_BSEG_Merged_MAUI = df_BSEG_MAUI.unionByName(df_VBSEGK_MAUI, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################




df_BKPF_req_MAUI=df_BKPF_MAUI_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_MAUI=df_BKPF_req_MAUI.filter(col('COMPANY_CODE').isin(company_codes_MAUI))
df_BKPF_MAUI = df_BKPF_req_MAUI.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_MAUI=df_VBKPF_MAUI_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE',col('DOCUMENT_HEADER_INTERNAL_REFERENCE_KEY_ID').alias('DOCUMENT_POSTING_DATE'),'DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_MAUI = df_VBKPF_req_MAUI.filter(col('COMPANY_CODE').isin(company_codes_MAUI))
df_VBKPF_MAUI = df_VBKPF_req_MAUI.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#filtering records present in VBKPF only
df_VBKPF_MAUI_filtered = df_VBKPF_MAUI.join(df_BKPF_MAUI, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_MAUI = df_BKPF_MAUI.unionByName(df_VBKPF_MAUI_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_MAUI = df_BKPF_MAUI.unionByName(df_VBKPF_MAUI,allowMissingColumns = True) 
######################################### END OF BKPF ########################################################################################
 


df_LFA1_req_MAUI=df_LFA1_MAUI_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_MAUI=df_LFA1_req_MAUI.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_MAUI)) #Newly added

df_LFB1_MAUI=df_LFB1_MAUI_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_MAUI=df_EKPO_MAUI_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_MAUI=df_EKKO_MAUI_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_MAUI = df_ESSR_MAUI_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_MAUI = df_RSEG_MAUI_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_MAUI=df_RSEG_req_MAUI.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))



# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_MAUI =['RI','RM','RN','RS','RC','RD']
doc_type_liv_MAUI =['IA','KI','KN','KQ','RI','RM','RN','RS','UI','UP','RC','RD','KC']
df_BKPF_LIV_MAUI = df_BKPF_Merged_MAUI.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_MAUI))
df_BKPF_LIV_MAUI = df_BKPF_LIV_MAUI.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Joining BKPF and BSEG
df_BKPF_BSEG_MAUI = df_BKPF_LIV_MAUI.join(df_BSEG_Merged_MAUI,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#join with RSEG
df_BKPF_BSEG_RSEG_MAUI = df_BKPF_BSEG_MAUI.join(df_RSEG_MAUI,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_MAUI = df_BKPF_BSEG_RSEG_MAUI.join(df_ESSR_MAUI,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_MAUI = df_BKPF_BSEG_RSEG_ESSR_MAUI.join(df_EKPO_MAUI ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_MAUI = df_BKPF_BSEG_RSEG_ESSR_EKPO_MAUI.join(df_LFA1_MAUI, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_MAUI.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_MAUI.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_MAUI = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_MAUI.join(df_LFB1_MAUI,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_MAUI = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_MAUI.join(df_EKKO_MAUI, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_MAUI = df_LIV_MAUI.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_MAUI=['IA','KI','KN','UI','UP','KC']
doc_type_fi_MAUI =['IA','KI','KN','KQ','RI','RM','RN','RS','UI','UP','RC','RD','KC']

#Filtering out BKPF for FI document types
df_BKPF_FI_MAUI=df_BKPF_Merged_MAUI.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_MAUI))

#Joining BSEG and BKPF
df_BSEG_BKPF_FI_MAUI = df_BSEG_Merged_MAUI.join(df_BKPF_FI_MAUI, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_MAUI = df_BSEG_BKPF_FI_MAUI.join(df_LFA1_MAUI, df_BSEG_BKPF_FI_MAUI.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_MAUI.VENDOR_ID, how="left")

#Join with LFB1
df_FI_MAUI = df_BSEG_BKPF_LFA1_FI_MAUI.join(df_LFB1_MAUI, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_MAUI=df_FI_MAUI.join(df_LIV_MAUI, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_MAUI = df_FI_anti_MAUI.withColumn('SPEND_TYPE',lit('FI'))


#Union Liv and FI Doctype Dataset
df_Final_LIVFI_MAUI = df_LIV_MAUI.unionByName(df_FI_anti_MAUI, allowMissingColumns = True)
# df_Final_LIVFI = df_LIV.unionByName(df_FI, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_MAUI = df_Final_LIVFI_MAUI.withColumn("ERP_NAME",lit('MAUI'))
df_Final_LIVFI_MAUI = df_Final_LIVFI_MAUI.withColumn("ERP",lit('P53'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_MAUI = df_Final_LIVFI_MAUI.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_MAUI = df_ERS_Cal_1_MAUI.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_MAUI = df_ERS_Cal_2_MAUI.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_MAUI =df_ERS_Cal_3_MAUI.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_MAUI.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_MAUI = df_ERS_Cal_Master_MAUI.withColumn("FISCAL_YEAR", df_ERS_Cal_3_MAUI["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_MAUI = df_Final1_MAUI.withColumn("FISCAL_PERIOD", df_Final1_MAUI["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_MAUI = df_Final1_MAUI.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_MAUI = df_Final1_MAUI.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_MAUI = df_Final1_MAUI.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_MAUI = df_Final1_MAUI.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_MAUI = df_Final1_MAUI.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_MAUI = df_Final1_MAUI.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_MAUI = df_Final1_MAUI.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_MAUI = df_Final_DTypeC_MAUI.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_MAUI = df_lt18_MAUI.withColumn("REFERENCE_KEY_ID_tw",df_lt18_MAUI.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_MAUI = df_Final_DTypeC_MAUI.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_MAUI = df_eq18_MAUI.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_MAUI = df_eq18_awkey_MAUI.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_MAUI = df_eq18_awkey_MAUI.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_MAUI = df_eq18_awkey_MAUI.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_MAUI = df_lt18_MAUI.unionByName(df_eq18_awkey_MAUI, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_MAUI = df_SWW_WI2OBJ_MAUI_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_MAUI = df_SWW_WI2OBJ_req_MAUI.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_MAUI = df_SWW_WI2OBJ_req_MAUI.drop(col("TASK_ID"))


df_SWWIHEAD_req_MAUI=df_SWWIHEAD_MAUI_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_MAUI = df_SWWIHEAD_req_MAUI.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_MAUI = df_SWWWIAGENT_MAUI_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_MAUI = df_Final_Finance_MAUI.join(df_SWW_WI2OBJ_MAUI,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_MAUI_isNull = df_Finance_SWWOBJ_MAUI.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_MAUI_NotNull = df_Finance_SWWOBJ_MAUI.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_MAUI_NotNull = df_Finance_SWWOBJ_MAUI_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_MAUI_NotNullfiltered = df_Finance_SWWOBJ_MAUI_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_MAUI = df_Finance_SWWOBJ_MAUI_NotNullfiltered.join(df_SWWIHEAD_MAUI,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_MAUI = df_Finance_SWWOBJ_SWWHEAD_MAUI.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_MAUI = df_Finance_SWWOBJ_SWWHEAD_MAUI.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_MAUI_NotNull = df_Finance_SWWOBJ_SWWHEAD_MAUI.join(df_SWWWIAGENT_MAUI,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_MAUI_NotNull = df_Finance_WORKFLOW_MAUI.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_MAUI = df_Finance_WORKFLOW_MAUI_NotNull.unionByName(df_Finance_SWWOBJ_MAUI_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_MAUI1 = df_Finance_WORKFLOW_Final_MAUI.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_MAUI1 = df_Finance_WORKFLOW_Final_MAUI1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_MAUI1=df_Final_MAUI1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
# df_Final_MAUI1=df_Final_MAUI1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_MAUI)) #Newly Added

df_Final_Currency_MAUI = df_Final_MAUI1.join(df_CurrencyDocAmount, [df_Final_MAUI1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_MAUI1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_MAUI = df_Final_Currency_MAUI.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_MAUI = df_Final_Currency_MAUI.drop(*columns_to_drop)
df_Final_DS_MAUI = df_Final_Currency_MAUI.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_MAUI#

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION_WITH_MAUI

# COMMAND ----------

#UNION of Whole Dataset with MAUI                    
df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL_MAUI = df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL.unionByName(df_Final_DS_MAUI)

# COMMAND ----------

# MAGIC %md
# MAGIC #START_OF_BLUEPRINT#

# COMMAND ----------

#Defining Path for each Table 
'''Configuring project path'''
Path_BSEG_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
        "EDAM_PATH").collect()[0][0]
Path_BKPF_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "BKPF")).select(
    "EDAM_PATH").collect()[0][0]

Path_VBSEGK_BLUEPRINT = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "VBSEGK")).select("EDAM_PATH").collect()[0][0]

Path_VBKPF_BLUEPRINT = \
   df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "VBKPF")).select("EDAM_PATH").collect()[0][0] 
  
  
Path_LFA1_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
        "EDAM_PATH").collect()[0][0]
Path_LFB1_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "LFB1")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKPO_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "EKPO")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKKO_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "EKKO")).select(
        "EDAM_PATH").collect()[0][0]

Path_ESSR_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "ESSR")).select(
        "EDAM_PATH").collect()[0][0]
Path_RSEG_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "RSEG")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWW_WI2OBJ_BLUEPRINT = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "SWW_WI2OBJ")).select(
        "EDAM_PATH").collect()[0][0]


Path_SWWWIHEAD_BLUEPRINT = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "SWWWIHEAD")).select(
        "EDAM_PATH").collect()[0][0]

Path_SWWWIAGENT_BLUEPRINT = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "BLUEPRINT") & (df_proj_path.OBJECT_NAME == "SWWWIAGENT")).select(
        "EDAM_PATH").collect()[0][0]


# COMMAND ----------

####Reading Data from Delta Table and Creating DataFrame for each Table.
df_BSEG_BLUEPRINT_proj = spark.read.format("delta").load(Path_BSEG_BLUEPRINT)
df_BKPF_BLUEPRINT_proj = spark.read.format("delta").load(Path_BKPF_BLUEPRINT)
df_VBSEGK_BLUEPRINT_proj = spark.read.format("delta").load(Path_VBSEGK_BLUEPRINT)
df_VBKPF_BLUEPRINT_proj = spark.read.format("delta").load(Path_VBKPF_BLUEPRINT)
df_LFA1_BLUEPRINT_proj = spark.read.format("delta").load(Path_LFA1_BLUEPRINT)
df_LFB1_BLUEPRINT_proj = spark.read.format("delta").load(Path_LFB1_BLUEPRINT)
df_EKPO_BLUEPRINT_proj = spark.read.format("delta").load(Path_EKPO_BLUEPRINT)
df_EKKO_BLUEPRINT_proj = spark.read.format("delta").load(Path_EKKO_BLUEPRINT)
df_EKKO_BLUEPRINT_proj = df_EKKO_BLUEPRINT_proj.withColumn("PO_RESPONSIBLE_FUNCTION_TEXT",lit('PORFT'))
df_ESSR_BLUEPRINT_proj = spark.read.format("delta").load(Path_ESSR_BLUEPRINT)
df_ESSR_BLUEPRINT_proj = df_ESSR_BLUEPRINT_proj.withColumn("SERVICE_ENTRY_APPROVER_ID",lit('USVME4'))
df_RSEG_BLUEPRINT_proj = spark.read.format("delta").load(Path_RSEG_BLUEPRINT)
df_SWW_WI2OBJ_BLUEPRINT_proj = spark.read.format("delta").load(Path_SWW_WI2OBJ_BLUEPRINT)
df_SWWIHEAD_BLUEPRINT_proj = spark.read.format("delta").load(Path_SWWWIHEAD_BLUEPRINT)
df_SWWWIAGENT_BLUEPRINT_proj = spark.read.format("delta").load(Path_SWWWIAGENT_BLUEPRINT)

# COMMAND ----------

#############--Selecting Columns from the Tables as well as Filtering the Records, which are required as Per Business Need --##################

#selecting List of COMPANY_CODE out of OIM_Requirement_file
company_codes_BLUEPRINT=list(df_datafilter.where(df_datafilter.ERP_NAME=='BLUEPRINT').select('COM_CODE').distinct().toPandas()['COM_CODE'])

#Selecting General ledger account number
recon_account_BLUEPRINT=list(df_datafilter.where(df_datafilter.ERP_NAME=='BLUEPRINT').select('GENERAL_LEDGER_ACCOUNT_NUMBER').distinct().toPandas()['GENERAL_LEDGER_ACCOUNT_NUMBER'])

#selecting vendor_account_group data from OIM DataFrame.
vendor_account_group_BLUEPRINT=list(df_datafilter.where(df_datafilter.ERP_NAME=='BLUEPRINT').select('VENDOR_ACCOUNT_GROUP_CODE').distinct().toPandas()['VENDOR_ACCOUNT_GROUP_CODE'])
 

df_BSEG_req_BLUEPRINT=df_BSEG_BLUEPRINT_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE',col('ACCOUNTING_DOCUMENT_LINE_ITEM').alias('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'),'BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','REGION_CODE','PRICE_UNIT_VALUE','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY')
                                     
df_BSEG_req_BLUEPRINT = df_BSEG_req_BLUEPRINT.filter(col('COMPANY_CODE').isin(company_codes_BLUEPRINT))
df_BSEG_req_BLUEPRINT = df_BSEG_req_BLUEPRINT.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG_req_BLUEPRINT = df_BSEG_req_BLUEPRINT.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG_req_BLUEPRINT = df_BSEG_req_BLUEPRINT.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG_BLUEPRINT = df_BSEG_req_BLUEPRINT.filter(col('FISCAL_YEAR')>=year_2)
df_BSEG_BLUEPRINT = df_BSEG_BLUEPRINT.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_BLUEPRINT))


df_VBSEGK_req_BLUEPRINT=df_VBSEGK_BLUEPRINT_proj.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','CALCULATED_LOCAL_CURRENCY_AMOUNT','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD','BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER')


df_VBSEGK_req_BLUEPRINT = df_VBSEGK_req_BLUEPRINT.filter(col('COMPANY_CODE').isin(company_codes_BLUEPRINT))
df_VBSEGK_req_BLUEPRINT = df_VBSEGK_req_BLUEPRINT.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_VBSEGK_BLUEPRINT = df_VBSEGK_req_BLUEPRINT.filter(col('FISCAL_YEAR')>=year_2)
df_VBSEGK_BLUEPRINT = df_VBSEGK_BLUEPRINT.filter(col('GENERAL_LEDGER_ACCOUNT_NUMBER').isin(recon_account_BLUEPRINT))

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################

#Filtering records from VBSEGK which are not in BSEG.
df_VBSEGK_BLUEPRINT_filtered = df_VBSEGK_BLUEPRINT.join(df_BSEG_BLUEPRINT, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR'],how = 'leftanti')

#Union of BSEGK and VSEGK( present in this Table only)
df_BSEG_Merged_BLUEPRINT = df_BSEG_BLUEPRINT.unionByName(df_VBSEGK_BLUEPRINT_filtered, allowMissingColumns = True)


# df_BSEG_Merged_BLUEPRINT = df_BSEG_BLUEPRINT.unionByName(df_VBSEGK_BLUEPRINT, allowMissingColumns = True)
######################################### END OF BSEG AND VSEGK ###############################################################################




df_BKPF_req_BLUEPRINT=df_BKPF_BLUEPRINT_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF_req_BLUEPRINT=df_BKPF_req_BLUEPRINT.filter(col('COMPANY_CODE').isin(company_codes_BLUEPRINT))
df_BKPF_BLUEPRINT = df_BKPF_req_BLUEPRINT.filter(col('FISCAL_YEAR')>=year_2)

df_VBKPF_req_BLUEPRINT=df_VBKPF_BLUEPRINT_proj.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_VBKPF_req_BLUEPRINT = df_VBKPF_req_BLUEPRINT.filter(col('COMPANY_CODE').isin(company_codes_BLUEPRINT))
df_VBKPF_BLUEPRINT = df_VBKPF_req_BLUEPRINT.filter(col('FISCAL_YEAR')>=2)

################ Logic for Removing those records which are present in Both PARKED as well as POSTED##########################################
#filtering records present in VBKPF only
df_VBKPF_BLUEPRINT_filtered = df_VBKPF_BLUEPRINT.join(df_BKPF_BLUEPRINT, ['ACCOUNTING_DOCUMENT_NUMBER',"COMPANY_CODE",'FISCAL_YEAR'],how = 'leftanti')

#Union of BKPF and VBKPF(records present in this table only)
df_BKPF_Merged_BLUEPRINT = df_BKPF_BLUEPRINT.unionByName(df_VBKPF_BLUEPRINT_filtered,allowMissingColumns = True) 


# df_BKPF_Merged_BLUEPRINT = df_BKPF_BLUEPRINT.unionByName(df_VBKPF_BLUEPRINT,allowMissingColumns = True)
######################################### END OF BKPF ########################################################################################
 


df_LFA1_req_BLUEPRINT=df_LFA1_BLUEPRINT_proj.select("COUNTRY_CODE",'VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1_BLUEPRINT=df_LFA1_req_BLUEPRINT.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_BLUEPRINT)) #Newly added

df_LFB1_BLUEPRINT=df_LFB1_BLUEPRINT_proj.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO_BLUEPRINT=df_EKPO_BLUEPRINT_proj.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','NET_PRICE_AMOUNT','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER',
                      'PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME')

df_EKKO_BLUEPRINT=df_EKKO_BLUEPRINT_proj.select("PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE",'COMPANY_CODE', 'PURCHASING_DOCUMENT_NUMBER')

df_ESSR_BLUEPRINT = df_ESSR_BLUEPRINT_proj.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER',"OBJECT_CREATED_BY", 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG_req_BLUEPRINT = df_RSEG_BLUEPRINT_proj.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG_BLUEPRINT=df_RSEG_req_BLUEPRINT.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))


# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
# doc_type_liv_BLUEPRINT =['RE','RM','RS','YN','ZI','ZJ','ZW','RD']
doc_type_liv_BLUEPRINT= ['CN','KA','KG','KH','KR','KZ','RE','RM','RS','SA','YN','ZE','ZG','ZH','ZI','ZJ','X1','X2','X3','X4','ZW','ZX','X5','RD']
df_BKPF_LIV_BLUEPRINT = df_BKPF_Merged_BLUEPRINT.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv_BLUEPRINT))
df_BKPF_LIV_BLUEPRINT = df_BKPF_LIV_BLUEPRINT.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

#Join BKPF and BSEG
df_BKPF_BSEG_BLUEPRINT = df_BKPF_LIV_BLUEPRINT.join(df_BSEG_Merged_BLUEPRINT,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with RSEG
df_BKPF_BSEG_RSEG_BLUEPRINT = df_BKPF_BSEG_BLUEPRINT.join(df_RSEG_BLUEPRINT,[ 'COMPANY_CODE', 'Reference_Indicator','FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="left") 

#Join with ESSR
df_BKPF_BSEG_RSEG_ESSR_BLUEPRINT = df_BKPF_BSEG_RSEG_BLUEPRINT.join(df_ESSR_BLUEPRINT,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")

#Join with EKPO
df_BKPF_BSEG_RSEG_ESSR_EKPO_BLUEPRINT = df_BKPF_BSEG_RSEG_ESSR_BLUEPRINT.join(df_EKPO_BLUEPRINT ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

#Join with LFA1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_BLUEPRINT = df_BKPF_BSEG_RSEG_ESSR_EKPO_BLUEPRINT.join(df_LFA1_BLUEPRINT, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO_BLUEPRINT.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1_BLUEPRINT.VENDOR_ID,how="left")

#Join with LFB1
df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_BLUEPRINT = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_BLUEPRINT.join(df_LFB1_BLUEPRINT,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Join with EKKO
df_LIV_BLUEPRINT = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1_BLUEPRINT.join(df_EKKO_BLUEPRINT, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")
df_LIV_BLUEPRINT = df_LIV_BLUEPRINT.withColumn('SPEND_TYPE',lit('LIV'))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
# doc_type_fi_BLUEPRINT=['KA','KG','KH','KR','KZ']

doc_type_fi_BLUEPRINT= ['CN','KA','KG','KH','KR','KZ','RE','RM','RS','SA','YN','ZE','ZG','ZH','ZI','ZJ','X1','X2','X3','X4','ZW','ZX','X5','RD']

#Filtering out BKPF for FI document types
df_BKPF_FI_BLUEPRINT=df_BKPF_Merged_BLUEPRINT.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi_BLUEPRINT))

#Joining BKPF and BSEG
df_BSEG_BKPF_FI_BLUEPRINT = df_BSEG_Merged_BLUEPRINT.join(df_BKPF_FI_BLUEPRINT, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

#Join with LFA1
df_BSEG_BKPF_LFA1_FI_BLUEPRINT = df_BSEG_BKPF_FI_BLUEPRINT.join(df_LFA1_BLUEPRINT, df_BSEG_BKPF_FI_BLUEPRINT.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1_BLUEPRINT.VENDOR_ID, how="left")

#Join with LFB1
df_FI_BLUEPRINT = df_BSEG_BKPF_LFA1_FI_BLUEPRINT.join(df_LFB1_BLUEPRINT, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")

#Doing Anti-Join of  "LIV" and "FI"  type to create a new Dataframe.
df_FI_anti_BLUEPRINT=df_FI_BLUEPRINT.join(df_LIV_BLUEPRINT, ['ACCOUNTING_DOCUMENT_NUMBER','COMPANY_CODE'], how='leftanti')
#Adding a new Column 
df_FI_anti_BLUEPRINT = df_FI_anti_BLUEPRINT.withColumn('SPEND_TYPE',lit('FI'))



#Union Liv and FI Doctype Dataset
df_Final_LIVFI_BLUEPRINT = df_LIV_BLUEPRINT.unionByName(df_FI_anti_BLUEPRINT, allowMissingColumns = True)
# df_Final_LIVFI_BLUEPRINT = df_LIV_BLUEPRINT.unionByName(df_FI_BLUEPRINT, allowMissingColumns = True)


#Adding columns with Constant Value
df_Final_LIVFI_BLUEPRINT = df_Final_LIVFI_BLUEPRINT.withColumn("ERP_NAME",lit('BLUEPRINT'))
df_Final_LIVFI_BLUEPRINT = df_Final_LIVFI_BLUEPRINT.withColumn("ERP",lit('P16'))

# COMMAND ----------

#Applying DEBIT_CREDIT_INDICATOR column condition to all the amount columns namely the below:
  #CALCULATED_DOCUMENT_CURRENCY_AMOUNT, SECOND_LOCAL_CURRENCY_AMOUNT, CALCULATED_LOCAL_CURRENCY_AMOUNT
  
df_ERS_Cal_1_BLUEPRINT = df_Final_LIVFI_BLUEPRINT.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))


df_ERS_Cal_2_BLUEPRINT = df_ERS_Cal_1_BLUEPRINT.withColumn("SECOND_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('SECOND_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('SECOND_LOCAL_CURRENCY_AMOUNT')))


df_ERS_Cal_3_BLUEPRINT = df_ERS_Cal_2_BLUEPRINT.withColumn("CALCULATED_LOCAL_CURRENCY_AMOUNT_F", when(col('DEBIT_CREDIT_INDICATOR')=='H', col('CALCULATED_LOCAL_CURRENCY_AMOUNT') * -1)
                                        .otherwise(col('CALCULATED_LOCAL_CURRENCY_AMOUNT')))



#Joining Mapping Tables to the Final Dataframe

MASTER_DATA_MAPPING= "dbfs:/mnt/PRJ_COACH/PROJECT/P00021-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/FIN_MAPPING/FIN_DATA_HUB_Phoenix/PHOENIX_MASTER_DATA_MAPPING_edit.csv"
df_master_map = spark.read.format("csv").option("Header","true").load(MASTER_DATA_MAPPING)
df_master_map = df_master_map.select(col("COM_CODE").alias("COMPANY_CODE"), 'Primary_Business', 'Primary_Sub_Business', 'ERP_NAME','Region')
df_ERS_Cal_Master_BLUEPRINT =df_ERS_Cal_3_BLUEPRINT.join(df_master_map , on=['COMPANY_CODE','ERP_NAME'] ,how='inner')

df_ERS_Cal_Master_BLUEPRINT.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))


#Changing the data types for the required columns to "date" and "Integer" as per Power BI 
df_Final1_BLUEPRINT = df_ERS_Cal_Master_BLUEPRINT.withColumn("FISCAL_YEAR", df_ERS_Cal_3_BLUEPRINT["FISCAL_YEAR"].cast(IntegerType()))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn("FISCAL_PERIOD", df_Final1_BLUEPRINT["FISCAL_PERIOD"].cast(IntegerType()))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('CLEARING_DATE_D', 
                   to_date(unix_timestamp(col('CLEARING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('DUE_BASELINE_DATE_D', 
                   to_date(unix_timestamp(col('DUE_BASELINE_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('SERVICE_ENTRY_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('SERVICE_ENTRY_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('DOCUMENT_POSTING_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_POSTING_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('RECORD_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('RECORD_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final1_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('DOCUMENT_CREATED_DATE_D', 
                   to_date(unix_timestamp(col('DOCUMENT_CREATED_DATE'), 'yyyyMMdd').cast("timestamp")))

df_Final_DTypeC_BLUEPRINT = df_Final1_BLUEPRINT.withColumn('ACCOUNTING_DOCUMENT_ENTRY_DATE_D', 
                   to_date(unix_timestamp(col('ACCOUNTING_DOCUMENT_ENTRY_DATE'), 'yyyyMMdd').cast("timestamp")))


# COMMAND ----------

#Reference key Id is required to Join with SWW_OBJ. But the lenght is variable.
#so we are  Creating two Dataset based upon the lenght of "REFERENCE_KEY_ID".

#Finance Dataset having reference_key_id lenght less than 18.
df_lt18_BLUEPRINT = df_Final_DTypeC_BLUEPRINT.filter(length(col("REFERENCE_KEY_ID")) != 18)
df_lt18_BLUEPRINT = df_lt18_BLUEPRINT.withColumn("REFERENCE_KEY_ID_tw",df_lt18_BLUEPRINT.REFERENCE_KEY_ID)

#Finance Dataset having reference_key_id lenght equal to 18.
df_eq18_BLUEPRINT = df_Final_DTypeC_BLUEPRINT.filter(length(col("REFERENCE_KEY_ID")) == 18)
df_eq18_awkey_BLUEPRINT = df_eq18_BLUEPRINT.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_eq18_awkey_BLUEPRINT = df_eq18_awkey_BLUEPRINT.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_eq18_awkey_BLUEPRINT = df_eq18_awkey_BLUEPRINT.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
#Rearranging the Reference key ID. Adding new Column "REFERENCE_KEY_ID_tw" needed for Joining with SWW_OBJ  Table 
df_eq18_awkey_BLUEPRINT = df_eq18_awkey_BLUEPRINT.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))


#Union of two Dataset created above and this the Final FINANCE DATASET having reference_key_id proper format to be joined with SWW_OBJ.
df_Final_Finance_BLUEPRINT = df_lt18_BLUEPRINT.unionByName(df_eq18_awkey_BLUEPRINT, allowMissingColumns = True)


# COMMAND ----------

##Reading Workflow Data and Filtering them as per Business Need
df_SWW_WI2OBJ_req_BLUEPRINT = df_SWW_WI2OBJ_BLUEPRINT_proj.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ_req_BLUEPRINT = df_SWW_WI2OBJ_req_BLUEPRINT.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
df_SWW_WI2OBJ_BLUEPRINT = df_SWW_WI2OBJ_req_BLUEPRINT.drop(col("TASK_ID"))


df_SWWIHEAD_req_BLUEPRINT=df_SWWIHEAD_BLUEPRINT_proj.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID',
              'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','TASK_SHORT_TEXT',
                'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','WORK_ITEM_ID','WORK_ITEM_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID',
                              'WORK_ITEM_PROCESSING_STATUS_CODE')
df_SWWIHEAD_BLUEPRINT = df_SWWIHEAD_req_BLUEPRINT.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'F')))

df_SWWWIAGENT_BLUEPRINT = df_SWWWIAGENT_BLUEPRINT_proj.select("AGENT_ID","WORK_ITEM_ID")


###################################################################################################################################################
#Joining the Finance Final Dataset with SWW_OBJ(Workflow Table)
df_Finance_SWWOBJ_BLUEPRINT = df_Final_Finance_BLUEPRINT.join(df_SWW_WI2OBJ_BLUEPRINT,['REFERENCE_KEY_ID_tw'],how="left")

#Filtering the above Fin-SWW_OBJ  Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NULL
df_Finance_SWWOBJ_BLUEPRINT_isNull = df_Finance_SWWOBJ_BLUEPRINT.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNull())

#Filtering the above Fin-SWW_OBJ Dataset having "TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID" is NotNULL
df_Finance_SWWOBJ_BLUEPRINT_NotNull = df_Finance_SWWOBJ_BLUEPRINT.filter(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").isNotNull())


#Partitioning the above Dataset based on "REFERENCE_KEY_ID_tw" followed by Ordering the WorkFlowUniqueID in Descending.
windowDept = Window.partitionBy("REFERENCE_KEY_ID_tw").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df_Finance_SWWOBJ_BLUEPRINT_NotNull = df_Finance_SWWOBJ_BLUEPRINT_NotNull.withColumn("row",row_number().over(windowDept))

#Filtering the Dataset, keeping Latest WorkFlowUniqueID.
df_Finance_SWWOBJ_BLUEPRINT_NotNullfiltered = df_Finance_SWWOBJ_BLUEPRINT_NotNull.filter(col("row")==1)

#Joining the above Dataset with SWWHEAD Table
df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT = df_Finance_SWWOBJ_BLUEPRINT_NotNullfiltered.join(df_SWWIHEAD_BLUEPRINT,['TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID'],how="left")

#Partioning the Dataset based on COMPANYCODE and ACCOUNTING_DOCUMENT_NUMBER Followed by Ordering on WorkItemID in Descending Order
windowSpec  = Window.partitionBy(["COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER"]).orderBy(col("WORK_ITEM_ID").desc())
df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT = df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT.withColumn("WORK_ITEM_ID_order",row_number().over(windowSpec))

#Filtering the Dataset, Keeping only latest WorkItemID
df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT = df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT.filter(col("WORK_ITEM_ID_order")==1)


#Joining the above Dataset with SWWWIAGENT
df_Finance_WORKFLOW_BLUEPRINT_NotNull = df_Finance_SWWOBJ_SWWHEAD_BLUEPRINT.join(df_SWWWIAGENT_BLUEPRINT,["WORK_ITEM_ID"],how="left")

# #dropping extra column
# df_Finance_WORKFLOW_BLUEPRINT_NotNull = df_Finance_WORKFLOW_BLUEPRINT.drop("WORK_ITEM_TEXT")

df_Finance_WORKFLOW_Final_BLUEPRINT = df_Finance_WORKFLOW_BLUEPRINT_NotNull.unionByName(df_Finance_SWWOBJ_BLUEPRINT_isNull, allowMissingColumns = True)

#Joining the Above dataset with AGENT NAME Dataframe to fetch "AGENT_NAME" Details
df_Finance_WORKFLOW_Final_BLUEPRINT1 = df_Finance_WORKFLOW_Final_BLUEPRINT.join(df_AgentName,on=['AGENT_ID'],how='left')

# COMMAND ----------

#taking only required columns to be pushed to SQL
# df_Final1=df_Final1.withColumn("BASELINE_DATE" ,col("DUE_BASELINE_DATE"))
df_Final_BLUEPRINT1 = df_Finance_WORKFLOW_Final_BLUEPRINT1.select('COMPANY_CODE','ERP','ERP_NAME','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER','ENTRY_SHEET_NUMBER','FISCAL_YEAR','FISCAL_PERIOD','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CLEARING_DOCUMENT_NUMBER','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','ACCOUNT_TYPE_CODE','BALANCE_SHEET_ACCOUNT_INDICATOR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR','PROFIT_CENTER_CODE','DEBIT_CREDIT_INDICATOR','INVOICE_DOCUMENT_NUMBER','CASH_DISCOUNT_PERIOD','CASH_DISCOUNT_PERIOD_2','NET_PAYMENT_TERMS_PERIOD','SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','INVOICE_REFERENCE_DOCUMENT_NUMBER','OBJECT_CREATED_BY','SERVICE_ENTRY_APPROVER_ID','GOODS_RECEIPT_INDICATOR','VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1','RECONCILIATION_ACCOUNT_ID',
col("CLEARING_DATE_D").alias("CLEARING_DATE"),col('DOCUMENT_CREATED_DATE_D').alias('DOCUMENT_CREATED_DATE'), col("DUE_BASELINE_DATE_D").alias("DUE_BASELINE_DATE"),col("SERVICE_ENTRY_POSTING_DATE_D").alias("SERVICE_ENTRY_POSTING_DATE"),col("DOCUMENT_POSTING_DATE_D").alias("DOCUMENT_POSTING_DATE"),col("RECORD_CREATED_DATE_D").alias("RECORD_CREATED_DATE"),col("ACCOUNTING_DOCUMENT_ENTRY_DATE_D").alias("ACCOUNTING_DOCUMENT_ENTRY_DATE"),col("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_F").alias("CALCULATED_DOCUMENT_CURRENCY_AMOUNT"),col("SECOND_LOCAL_CURRENCY_AMOUNT_F").alias("SECOND_LOCAL_CURRENCY_AMOUNT"),col("CALCULATED_LOCAL_CURRENCY_AMOUNT_F").alias("CALCULATED_LOCAL_CURRENCY_AMOUNT"),
 "PO_RESPONSIBLE_FUNCTION_TEXT","PURCHASING_GROUP_CODE","PURCHASING_ORGANIZATION_CODE","SPEND_TYPE",
   'NET_PRICE_AMOUNT','PLANT_FACILITY_ID','PURCHASE_REQUISITION_NUMBER','REQUISITIONER_OR_REQUESTER_NAME','REGION_CODE','PRICE_UNIT_VALUE',
   'BUSINESS_PARTNER_REFERENCE_KEY2_NUMBER','QUANTITY','COUNTRY_CODE',"Region",
  "AGENT_ID","WORK_ITEM_ACTUAL_AGENT_ID","WORK_ITEM_ID","WORK_ITEM_CREATOR_ID","WORK_ITEM_CREATED_BY_USER_ID",
 "WORK_ITEM_CREATION_DATE","WORK_ITEM_TEXT","TASK_SHORT_TEXT","AGENT_NAME",'TASK_ID',
  'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','Primary_Business', 'Primary_Sub_Business')

# df_Final_BLUEPRINT1=df_Final_BLUEPRINT1.filter(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') <= 0) #Newly Added
df_Final_BLUEPRINT1=df_Final_BLUEPRINT1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group_BLUEPRINT)) #Newly Added

df_Final_Currency_BLUEPRINT = df_Final_BLUEPRINT1.join(df_CurrencyDocAmount, [df_Final_BLUEPRINT1.CURRENCY_KEY_CODE==df_CurrencyDocAmount.Currency_Type,df_Final_BLUEPRINT1.ERP_NAME==df_CurrencyDocAmount.ERP_Doc], how="left")

df_Final_Currency_BLUEPRINT = df_Final_Currency_BLUEPRINT.withColumn("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul", when((col('CURRENCY_KEY_CODE')==col('Currency_Type')) & (col('ERP_NAME')==col('ERP_Doc')), col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT') * col('Multiplier_Value'))
                                        .otherwise(col('CALCULATED_DOCUMENT_CURRENCY_AMOUNT')))

columns_to_drop = ['CALCULATED_DOCUMENT_CURRENCY_AMOUNT', 'Currency_Type', 'Multiplier_Value', 'ERP_Doc']
df_Final_Currency_BLUEPRINT = df_Final_Currency_BLUEPRINT.drop(*columns_to_drop)
df_Final_DS_BLUEPRINT = df_Final_Currency_BLUEPRINT.withColumnRenamed("CALCULATED_DOCUMENT_CURRENCY_AMOUNT_Mul","CALCULATED_DOCUMENT_CURRENCY_AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC #END_OF_BLUEPRINT#

# COMMAND ----------

# MAGIC %md
# MAGIC #WHOLE_ERP_DATASET#

# COMMAND ----------

#UNION of Whole Dataset with MAUI                    
df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL_MAUI_BLUEPRINT = df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL_MAUI.unionByName(df_Final_DS_BLUEPRINT)

# COMMAND ----------

# df_Final_DS_SERP_BG_CRITERION_GSAP_GSAPCHEM_STN_CRYSTAL_MAUI_BLUEPRINT.write.format("com.microsoft.sqlserver.jdbc.spark").mode("overwrite").option("url",jdbcUrl).option("mssqlIsolationLevel","READ_UNCOMMITTED").option("reliabilityLevel","BEST_EFFORT").option("tableLock","true").option("dbtable","OIM").option("user",db_username).option("password", db_password).save()

# COMMAND ----------



# COMMAND ----------


