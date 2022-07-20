# Databricks notebook source
from pyspark.sql.functions import col , when, abs , lit, dayofweek, unix_timestamp, from_unixtime
from pyspark.sql.functions import *
import pyspark.sql.functions as funcs
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta , datetime
from pyspark.sql.types import IntegerType
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import concat_ws

from pyspark.sql.functions import desc, sum
from pyspark.sql.functions import max
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import countDistinct

# COMMAND ----------

# MAGIC %run /Shared/Phoenix/FI_REUSE_METHOD

# COMMAND ----------

# Widget inorder to get project path from SQL Table
dbutils.widgets.text("Get the Config table details", "", "")
Config_Table_name = dbutils.widgets.get("Get the Config table details")

# fetching path details from SQL table
df_proj_path = spark.read.jdbc(url=jdbcUrl, table=Config_Table_name)

# COMMAND ----------

'''Configuring project path'''
Path_BSEG_Proj = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "BSEG")).select(
                  "EDAM_PATH").collect()[0][0]
Path_BKPF_Proj = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "BKPF")).select(
                  "EDAM_PATH").collect()[0][0]

Path_VBSEGK_Proj = "dbfs:/mnt/PRJ_FIN/PROJECT/P00026-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/SERP/VBSEGK/"
Path_VBKPF_Proj = "dbfs:/mnt/PRJ_FIN/PROJECT/P00026-Finance-Azure-Data-Hub/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/SERP/VBKPF/"

Path_LFA1_Proj = df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "LFA1")).select(
                  "EDAM_PATH").collect()[0][0]
Path_LFB1_Proj = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "LFB1")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKPO_Proj = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "EKPO")).select(
        "EDAM_PATH").collect()[0][0]
Path_EKKO_Proj = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "EKKO")).select(
        "EDAM_PATH").collect()[0][0]
Path_ESSR_Proj = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "ESSR")).select(
        "EDAM_PATH").collect()[0][0]
Path_RSEG_Proj = \
    df_proj_path.where((df_proj_path.SOURCE_SYSTEM == "SERP") & (df_proj_path.OBJECT_NAME == "RSEG")).select(
        "EDAM_PATH").collect()[0][0]

Path_SWW_WI2OBJ_Proj = "dbfs:/mnt/PRJ_CP/PROJECT/P00016-CP_DATA_HUB/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/SERP/SWW_WI2OBJ"
Path_SWWWIAGENT_Proj ="dbfs:/mnt/PRJ_CP/PROJECT/P00016-CP_DATA_HUB/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/SERP/SWWWIAGENT/"
Path_SWWWIHEAD_Proj ="dbfs:/mnt/PRJ_CP/PROJECT/P00016-CP_DATA_HUB/ENRICHED/UN-HARMONIZED/Non-Sensitive/1st Party/SERP/SWWWIHEAD/"
# display(Path_BSEG_Proj)


# COMMAND ----------

print(Path_BSEG_Proj)
print(Path_BKPF_Proj)
print(Path_VBSEGK_Proj)
print(Path_VBKPF_Proj)
print(Path_LFA1_Proj)
print(Path_LFB1_Proj)
print(Path_EKPO_Proj)
print(Path_EKKO_Proj)
print(Path_ESSR_Proj)
print(Path_RSEG_Proj)
print(Path_SWW_WI2OBJ_Proj)
print(Path_SWWWIAGENT_Proj)
print(Path_SWWWIHEAD_Proj)

# COMMAND ----------

df_BSEG = spark.read.format("delta").load(Path_BSEG_Proj)
df_BKPF = spark.read.format("delta").load(Path_BKPF_Proj)
# df_VBSEGK = spark.read.format("delta").load(Path_VBSEGK_Proj)
# df_VBKPF = spark.read.format("delta").load(Path_VBKPF_Proj)
df_LFA1 = spark.read.format("delta").load(Path_LFA1_Proj)
df_LFB1 = spark.read.format("delta").load(Path_LFB1_Proj)
df_EKPO = spark.read.format("delta").load(Path_EKPO_Proj)
df_EKKO = spark.read.format("delta").load(Path_EKKO_Proj)
df_ESSR = spark.read.format("delta").load(Path_ESSR_Proj)
df_RSEG = spark.read.format("delta").load(Path_RSEG_Proj)
df_SWW_WI2OBJ = spark.read.format("delta").load(Path_SWW_WI2OBJ_Proj)
df_SWWIHEAD = spark.read.format("delta").load(Path_SWWWIHEAD_Proj)
df_SWWWIAGENT = spark.read.format("delta").load(Path_SWWWIAGENT_Proj)

# COMMAND ----------

company_codes=['BM11','BM18','BM34','CA19','DE32','GB03','GB10','GB32','GB33','GB50','GB68','GBB0','GBB1','GBB2','GBB3','GBB5','GBC8','GBE6','GBE9','GBF3','GBJ7','GBM8','GBU0','GBV4','IN02','JP10','MY03','MY04','MY30','NL27','NL31','NL47','NL56','NL59','NL61','NL63','NL78','NL80','NL82','NL87','NL98','NL99','NLA0','NLB9','NLF1','NLG5','NLJ2','NLP6','NLP7','PH07','PL04','US01','US46','US47','US50','US51','US57','US58','US60','US61','US62','US63','US65','US66','US67','US68','US69','US70','US84','USC2','USE6','USQ8','UST3','USG5','NL40','IN19']

vendor_account_group = ['VFIO','YNPE','YOTV','YVND']

df_BSEG=df_BSEG.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','ACCOUNTING_DOCUMENT_NUMBER','BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','COMPANY_CODE','FISCAL_YEAR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD')

df_BSEG = df_BSEG.filter(col('COMPANY_CODE').isin(company_codes))
df_BSEG = df_BSEG.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
df_BSEG = df_BSEG.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
df_BSEG = df_BSEG.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
df_BSEG = df_BSEG.filter(col('CLEARING_DOCUMENT_NUMBER').like('2%'))
df_BSEG = df_BSEG.filter(col('FISCAL_YEAR')>=2)
# df_BSEG.count()
# df_VBSEGK=df_VBSEGK.select('COMPANY_CODE','ACCOUNTING_DOCUMENT_NUMBER','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','ASSIGNMENT_NUMBER','CALCULATED_DOCUMENT_CURRENCY_AMOUNT','CLEARING_DATE','CLEARING_DOCUMENT_NUMBER','DUE_BASELINE_DATE','ITEM_TEXT','PAYMENT_BLOCK_CODE','PAYMENT_KEY_TERM_CODE','PAYMENT_METHOD_CODE','POSTING_KEY_CODE','SECOND_LOCAL_CURRENCY_AMOUNT','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER','ACCOUNT_TYPE_CODE','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER','ACCOUNTING_DOCUMENT_NUMBER','BALANCE_SHEET_ACCOUNT_INDICATOR','CALCULATED_LOCAL_CURRENCY_AMOUNT','COMPANY_CODE','FISCAL_YEAR','GENERAL_LEDGER_ACCOUNT_NUMBER','LINE_ITEM_REFERENCE_KEY_ID','SPECIAL_GENERAL_LEDGER_INDICATOR', 'PROFIT_CENTER_CODE', 'DEBIT_CREDIT_INDICATOR', 'INVOICE_DOCUMENT_NUMBER', 'CASH_DISCOUNT_PERIOD', 'CASH_DISCOUNT_PERIOD_2', 'NET_PAYMENT_TERMS_PERIOD')

# df_VBSEGK = df_VBSEGK.filter(col('COMPANY_CODE').isin(company_codes))
# df_VBSEGK = df_VBSEGK.filter(col('ACCOUNT_TYPE_CODE').isin('K'))
# df_VBSEGK = df_VBSEGK.filter(col('SPECIAL_GENERAL_LEDGER_INDICATOR').isin(''))
# df_VBSEGK = df_VBSEGK.filter(col('BALANCE_SHEET_ACCOUNT_INDICATOR')=='X')
# df_VBSEGK = df_VBSEGK.filter(col('CLEARING_DOCUMENT_NUMBER').like('2%'))
# df_VBSEGK = df_VBSEGK.filter(col('FISCAL_YEAR')>=2)

# df_BSEG_Merged = df_BSEG.unionByName(df_VBSEGK)

df_BKPF=df_BKPF.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
df_BKPF=df_BKPF.filter(col('COMPANY_CODE').isin(company_codes))
df_BKPF = df_BKPF.filter(col('FISCAL_YEAR')>=2)

# df_VBKPF=df_VBKPF.select('SECOND_LOCAL_CURRENCY_KEY_CODE','LOCAL_CURRENCY_KEY_CODE','DOCUMENT_PARKED_BY_NAME','DOCUMENT_POSTING_DATE','DOCUMENT_STATUS_CODE','DOCUMENT_HEADER_TEXT','DOCUMENT_TYPE_CODE','DOCUMENT_CREATED_DATE','ACCOUNTING_DOCUMENT_ENTRY_DATE','USER_ID','CURRENCY_KEY_CODE','REFERENCE_DOCUMENT_NUMBER','COMPANY_CODE','FISCAL_YEAR','ACCOUNTING_DOCUMENT_NUMBER','REFERENCE_KEY_ID','FISCAL_PERIOD')
# df_VBKPF = df_VBKPF.filter(col('COMPANY_CODE').isin(company_codes))
# df_VBKPF = df_VBKPF.filter(col('FISCAL_YEAR')>=2)

# df_BKPF_Merged = df_BKPF.unionByName(df_VBKPF) 


df_LFA1=df_LFA1.select('VENDOR_ACCOUNT_GROUP_CODE','VENDOR_ID','VENDOR_NAME_1')
df_LFA1=df_LFA1.filter(col('VENDOR_ACCOUNT_GROUP_CODE').isin(vendor_account_group)) #Newly added

df_LFB1=df_LFB1.select('COMPANY_CODE','RECONCILIATION_ACCOUNT_ID','VENDOR_OR_CREDITOR_ACCOUNT_NUMBER')

df_EKPO=df_EKPO.select('COMPANY_CODE','GOODS_RECEIPT_INDICATOR','PURCHASING_DOCUMENT_ITEM_NUMBER','PURCHASING_DOCUMENT_NUMBER')

df_EKKO=df_EKKO.select(col("OBJECT_CREATED_BY").alias("PO_Creator"),'PO_RESPONSIBLE_FUNCTION_TEXT','PURCHASING_GROUP_CODE','PURCHASING_ORGANIZATION_CODE','COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER')

df_ESSR = df_ESSR.select(col("DOCUMENT_POSTING_DATE").alias("SERVICE_ENTRY_POSTING_DATE"), 'ENTRY_SHEET_NUMBER', col("OBJECT_CREATED_BY").alias("Service_Entry_Creator"), 'RECORD_CREATED_DATE', 'SERVICE_ENTRY_APPROVER_ID', 'PURCHASING_DOCUMENT_NUMBER')

df_RSEG = df_RSEG.select(col("INVOICE_REFERENCE_DOCUMENT_NUMBER").alias("ENTRY_SHEET_NUMBER"), col("ACCOUNTING_DOCUMENT_NUMBER").alias("Reference_Indicator"), 'FISCAL_YEAR','COMPANY_CODE','INVOICE_REFERENCE_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_NUMBER', 'PURCHASING_DOCUMENT_ITEM_NUMBER',  'ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER')
df_RSEG=df_RSEG.withColumn("ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER", substring('ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER',4,6))

# COMMAND ----------

#Obtaining LIV records i.e. the documents which are posted with Purchasing Document Number
doc_type_liv=['RE','RM','RS']
df_BKPF_LIV = df_BKPF.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_liv))
df_BKPF_LIV = df_BKPF_LIV.withColumn("Reference_Indicator",substring("REFERENCE_KEY_ID",1,10))

df_BKPF_BSEG = df_BKPF_LIV.join(df_BSEG,['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")
df_BKPF_BSEG_RSEG = df_BKPF_BSEG.join(df_RSEG,['Reference_Indicator', 'COMPANY_CODE', 'FISCAL_YEAR','ACCOUNTING_DOCUMENT_LINE_ITEM_NUMBER'], how="inner") 

df_BKPF_BSEG_RSEG_ESSR = df_BKPF_BSEG_RSEG.join(df_ESSR,['ENTRY_SHEET_NUMBER', 'PURCHASING_DOCUMENT_NUMBER'], how="left")
df_BKPF_BSEG_RSEG_ESSR_EKPO = df_BKPF_BSEG_RSEG_ESSR.join(df_EKPO ,      ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER','PURCHASING_DOCUMENT_ITEM_NUMBER'] ,how="left")

df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1 = df_BKPF_BSEG_RSEG_ESSR_EKPO.join(df_LFA1, 
                               df_BKPF_BSEG_RSEG_ESSR_EKPO.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER == df_LFA1.VENDOR_ID,how="left")

df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1 = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1.join(df_LFB1,['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")
df_LIV = df_BKPF_BSEG_RSEG_ESSR_EKPO_LFA1_LFB1.join(df_EKKO, ['COMPANY_CODE','PURCHASING_DOCUMENT_NUMBER'], how="left")

# COMMAND ----------

# display(df_BKPF_LIV.select("REFERENCE_KEY_ID","Reference_Indicator"))

# COMMAND ----------

#Obtaining FI records i.e. the documents which are posted without Purchasing Document Number
doc_type_fi=['IA','II','KI','KK','KN','MJ','UN']

#Filtering out BKPF for FI document types
df_BKPF_FI=df_BKPF.filter(col('DOCUMENT_TYPE_CODE').isin(doc_type_fi))

#Applying the required joins
df_BSEG_BKPF_FI = df_BSEG.join(df_BKPF_FI, ['ACCOUNTING_DOCUMENT_NUMBER', 'COMPANY_CODE', 'FISCAL_YEAR'], how="inner")

df_BSEG_BKPF_LFA1_FI = df_BSEG_BKPF_FI.join(df_LFA1, df_BSEG_BKPF_FI.VENDOR_OR_CREDITOR_ACCOUNT_NUMBER==df_LFA1.VENDOR_ID, how="left")

df_FI = df_BSEG_BKPF_LFA1_FI.join(df_LFB1, ['COMPANY_CODE', 'VENDOR_OR_CREDITOR_ACCOUNT_NUMBER'], how="left")
# df_Final_ERS = df_LIV.unionByName(df_FI, allowMissingColumns = True)

# df_Final_ERS = df_Final_ERS.withColumn("ERP_NAME",lit('SERP'))

# COMMAND ----------

df_Final_ERS = df_LIV.unionByName(df_FI, allowMissingColumns = True)
# display(df_Final_ERS)

# COMMAND ----------

# display(df_Final_ERS.filter(col("ACCOUNTING_DOCUMENT_NUMBER") == '5500084841'))
display(df_Final_ERS.where(df_Final_ERS.ACCOUNTING_DOCUMENT_NUMBER=='5500084841'))


# COMMAND ----------

df_NL31 = df_Final_ERS.filter(col("COMPANY_CODE") == "NL31")
# display(df_NL31.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID"))
rec_NL31 = df_NL31.count()
print(rec_NL31)

# COMMAND ----------

# display(df_Final_ERS.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID"))

# COMMAND ----------

df_NL31_less18 = df_NL31.filter(length(col("REFERENCE_KEY_ID")) < 18)
# df_NL31_less18 = df_NL31_less18.withColumnRenamed("REFERENCE_KEY_ID","REFERENCE_KEY_ID_tw")
df_NL31_less18 = df_NL31_less18.withColumn("REFERENCE_KEY_ID_tw",df_NL31_less18.REFERENCE_KEY_ID)
# display(df_NL31_less18.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID_tw","REFERENCE_KEY_ID"))
# display(df_NL31_less18)
rec_less18 = df_NL31_less18.count()
print(rec_less18)

# COMMAND ----------

df_NL31_18 = df_NL31.filter(length(col("REFERENCE_KEY_ID")) == 18)
display(df_NL31_18.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID"))
rec_18 = df_NL31_18.count()
print(rec_18)

# COMMAND ----------

# var22 = df_NL31_18.filter(length(col("REFERENCE_KEY_ID_tw")))
# print(var22)

# COMMAND ----------

df_NL31_18_awkey = df_NL31_18.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
df_NL31_18_awkey = df_NL31_18_awkey.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
df_NL31_18_awkey = df_NL31_18_awkey.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
df_NL31_18_awkey=df_NL31_18_awkey.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))
# display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey'))
# display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey"))
display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey",'post_awkey',"REFERENCE_KEY_ID_tw"))

# COMMAND ----------

# display(df_NL31_18_awkey.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID","BOR_COMPATIBLE_INSTANCE_ID"))

# COMMAND ----------

df_NL31_final = df_NL31_less18.unionByName(df_NL31_18_awkey, allowMissingColumns = True)
# display(df_NL31_final)
# display(df_NL31_final.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey",'mid_awkey',"REFERENCE_KEY_ID_tw"))

# COMMAND ----------

display(df_NL31_final.filter(col("ACCOUNTING_DOCUMENT_NUMBER") == 5500084841))

# COMMAND ----------

# display(df_NL31_final.filter(length(col("REFERENCE_KEY_ID_tw")) == 18))
print(df_NL31_final.count())

# COMMAND ----------



# COMMAND ----------

display(df_NL31_final.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID","REFERENCE_KEY_ID_tw"))

# COMMAND ----------

# df_Fin = df_Final_ERS.withColumn("BOR_COMPATIBLE_INSTANCE_ID",concat_ws("","COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR"))
# display(df_Fin.select("BOR_COMPATIBLE_INSTANCE_ID"))
# display(df_Fin)

# COMMAND ----------

# display(df_Final_ERS.select("REFERENCE_KEY_ID"))

# COMMAND ----------

# display(df_Final_ERS)
df_FIN_awkey = df_Final_ERS.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
# df_FIN_awkey = df_FIN_awkey.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,14))
# df_FIN_awkey = df_FIN_awkey.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,18))
# # df_FIN = df_FIN_awkey.withColumn("BOR_COMPATIBLE_INSTANCE_ID",concat_ws("","mid_awkey","pre_awkey","post_awkey"))
# display(df_FIN_awkey.select("REFERENCE_KEY_ID",'pre_awkey'))
# display(df_FIN_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey"))
# display(df_FIN_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey",'mid_awkey'))

# COMMAND ----------



# COMMAND ----------

df_SWW_WI2OBJ = spark.read.format("delta").load(Path_SWW_WI2OBJ_Proj)

df_SWWWIAGENT = spark.read.format("delta").load(Path_SWWWIAGENT_Proj)
# df_SWW_WI2OBJ = df_SWW_WI2OBJ.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"),'WORKFLOW_WORK_ITEM_TYPE_CODE','TASK_ID','TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID')
df_SWW_WI2OBJ =  df_SWW_WI2OBJ.select(col('BOR_COMPATIBLE_INSTANCE_ID').alias("REFERENCE_KEY_ID_tw"))

df_SWW_WI2OBJ = df_SWW_WI2OBJ.filter((col('WORKFLOW_WORK_ITEM_TYPE_CODE')==99) & (col('TASK_ID').startswith("WS")))
# display(df_SWW_WI2OBJ)

# COMMAND ----------

sww_obj_cnt = df_SWW_WI2OBJ.count()
print(sww_obj_cnt)

# COMMAND ----------

df_NL31_WI2OBJ = df_NL31_18_awkey.join(df_SWW_WI2OBJ,['REFERENCE_KEY_ID_tw'],how="left")
print(df_NL31_WI2OBJ.count())

display(df_NL31_WI2OBJ.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID","REFERENCE_KEY_ID_tw"))

# COMMAND ----------

display(df_NL31_WI2OBJ.filter(col("ACCOUNTING_DOCUMENT_NUMBER") == 5500084841))

# COMMAND ----------

display(df_Fin_WI2OBJ.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID_tw","DOCUMENT_TYPE_CODE"))

# COMMAND ----------

df_SWWIHEAD = spark.read.format("delta").load(Path_SWWWIHEAD_Proj)
df_SWWIHEAD=df_SWWIHEAD.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID','WORKFLOW_CREATION_TIMESTAMP',
                 'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','WORK_ITEM_TEXT','WORKFLOW_EXECUTION_TIME_SECONDS',
                   'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','TASK_SHORT_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID')


# df_SWWIHEAD = df_SWWIHEAD.filter(col('TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID') == val1)
# df_SWWIHEAD = df_SWWIHEAD.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'K')))
display(df_SWWIHEAD)
# df_SWWIHEAD.count()

# df_SWWWIAGENT = df_SWWWIAGENT.select('AGENT_ID')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


# display(df_Fin_WI2OBJ.select(countDistinct("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID")))

# COMMAND ----------

# df_Fin_WI2OBJ1=df_Fin_WI2OBJ.withColumn("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID",df_Fin_WI2OBJ.TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID.cast('int'))

# COMMAND ----------

df_rank = df_Fin_WI2OBJ1.withColumn("col3", dense_rank().over(Window.partitionBy("BOR_COMPATIBLE_INSTANCE_ID").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())))
df_rank.where(df_rank.col3==1).count()

# COMMAND ----------

windowDept = Window.partitionBy("BOR_COMPATIBLE_INSTANCE_ID").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
df=df_Fin_WI2OBJ.withColumn("row",row_number().over(windowDept))
df1 = df.select("BOR_COMPATIBLE_INSTANCE_ID","TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID","row")
# display(df1.orderBy("BOR_COMPATIBLE_INSTANCE_ID"))
df1.where(df1.row==1).count()

# COMMAND ----------

df_Fin_WI2OBJ1=df_Fin_WI2OBJ.withColumn("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID",df_Fin_WI2OBJ.TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID.cast('int'))
df_Fin_WI2OBJ1.groupBy("BOR_COMPATIBLE_INSTANCE_ID").max("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID")
# display(df_Fin_WI2OBJ1)
# windowDept = Window.partitionBy("BOR_COMPATIBLE_INSTANCE_ID").orderBy(col("TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID").desc())
# display(df_Fin_WI2OBJ.withColumn("row",row_number().over(windowDept)))

# COMMAND ----------

df_SWWIHEAD = spark.read.format("delta").load(Path_SWWWIHEAD_Proj)
df_SWWIHEAD=df_SWWIHEAD.select('WORK_ITEM_ACTUAL_AGENT_ID','WORK_ITEM_LAST_FORWARDER_ID','WORK_ITEM_CREATOR_ID','WORKFLOW_CREATION_TIMESTAMP',
                 'WORK_ITEM_CREATED_BY_USER_ID','WORK_ITEM_CREATION_DATE','WORK_ITEM_TEXT','WORKFLOW_EXECUTION_TIME_SECONDS',
                   'TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID','TASK_SHORT_TEXT','TASK_ID','WORK_ITEM_TYPE_CODE','WORK_ITEM_ID')


# df_SWWIHEAD = df_SWWIHEAD.filter(col('TOP_LEVEL_INSTANCE_WORKFLOW_UNIQUE_ID') == val1)
# df_SWWIHEAD = df_SWWIHEAD.filter((col('WORK_ITEM_TYPE_CODE') == 'W') | ((col('WORK_ITEM_TYPE_CODE') == 'K')))
display(df_SWWIHEAD)
# df_SWWIHEAD.count()

# df_SWWWIAGENT = df_SWWWIAGENT.select('AGENT_ID')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# df_NL31 = df_Final_ERS.filter(col("COMPANY_CODE") == "NL31")
# # display(df_NL31.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID"))
# rec_NL31 = df_NL31.count()
# print(rec_NL31)



# df_NL31_less18 = df_NL31.filter(length(col("REFERENCE_KEY_ID")) < 18)
# df_NL31_less18 = df_NL31_less18.withColumnRenamed("REFERENCE_KEY_ID","REFERENCE_KEY_ID_tw")
# # df_NL31_less18 = df_NL31_less18.withColumn("REFERENCE_KEY_ID_tw",df_NL31_less18.REFERENCE_KEY_ID)
# # display(df_NL31_less18.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID_tw","REFERENCE_KEY_ID"))
# display(df_NL31_less18)
# rec_less18 = df_NL31_less18.count()
# print(rec_less18)


# df_NL31_18 = df_NL31.filter(length(col("REFERENCE_KEY_ID")) == 18)
# # display(df_NL31_18.select("COMPANY_CODE","ACCOUNTING_DOCUMENT_NUMBER","FISCAL_YEAR","REFERENCE_KEY_ID"))
# rec_18 = df_NL31_18.count()
# print(rec_18)


# df_NL31_18_awkey = df_NL31_18.withColumn("pre_awkey",substring('REFERENCE_KEY_ID',1,10))
# df_NL31_18_awkey = df_NL31_18_awkey.withColumn("mid_awkey",substring('REFERENCE_KEY_ID',11,4))
# df_NL31_18_awkey = df_NL31_18_awkey.withColumn("post_awkey",substring('REFERENCE_KEY_ID',15,4))
# df_NL31_18_awkey=df_NL31_18_awkey.withColumn("REFERENCE_KEY_ID_tw",concat_ws("","mid_awkey","pre_awkey","post_awkey"))
# # display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey'))
# # display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey"))
# display(df_NL31_18_awkey.select("REFERENCE_KEY_ID",'pre_awkey',"mid_awkey",'post_awkey',"REFERENCE_KEY_ID_tw"))
