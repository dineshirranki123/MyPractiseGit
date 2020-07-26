# Databricks notebook source
columns = ["cost_center_ref_id",         
"function_name",              
"profit_center_2_id",         
"cost_center_name",           
"batch_id",                   
"rec_effectiveend_datetime",  
"gsf_product_code",           
"rec_end_datetime",           
"is_cost_center_active",      
"profit_center_1_id",         
"cost_center_create_date",    
"company_code_id",            
"profit_center_count",        
"rec_effectivestart_datetime",
"rec_start_datetime",         
"profit_center_2_name",       
"function_code",              
"cost_center_id",             
"gsf_product_code_name",      
"cost_center_country_code",   
"company_code_name",          
"rec_active_status",          
"rec_last_update_datetime",   
"profit_center_1_name",       
"pk_costcent_key"]  

def scdexpr(columns,primaryKey,batch_id,start_time):
  dict={}
  compositeKey = ''
  for col in primaryKey:
    compositeKey += "t."+col+","
  expr2 = "concat("+compositeKey+")"
  compositeKeyExpr = ''.join(expr2.rsplit(',', 1))
  
  condition = compositeKeyExpr + "=s.MergeKey"
  print(condition)
  for col in columns:
    dict[col] = "s." + col
    dict["batch_id"] = batch_id
    dict["rec_start_datetime"]= start_time
    dict["rec_end_datetime"] = "9999-12-31"
    dict["rec_active_status"] = "1"
  return (dict,condition)
    

  

# COMMAND ----------

  #temporary arrangement
#   dict["batch_id"] = "1"
#   dict["rec_effectiveend_datetime"] = "9999-12-31   "
#   dict["rec_end_datetime"] = "9999-12-31            "
#   dict["rec_effectivestart_datetime"] = "s.ingest_ts "
#   dict["rec_start_datetime"] = "s.ingest_ts "
#   dict["rec_active_status"] = "1"
#   dict["rec_last_update_datetime"] = "s.ingest_ts"
#   dict["pk_costcent_key"] = "1"  
  #~temporary arrangement
  # (insertValues, condition )= scdexpr(columns, "cost_center_ref_id")
# print(insertValues)

# COMMAND ----------

columnsDict = {"cost_center_ref_id":" integer ","cost_center_id":" double ","cost_center_name":" string ","cost_center_country_code":" string ","is_cost_center_active":" string ","company_code_name":" string ","company_code_id":" string ","function_code":" string ","function_name":" string ","gsf_product_code":" string ","gsf_product_code_name":" string ","cost_center_create_date":" string ","profit_center_1_name":" string ","profit_center_1_id":" string ","profit_center_2_name":" string ","profit_center_2_id":" string ","profit_center_count":" string ","ingest_ts":" string "}

def ConvertColumnTypes(columnsDict):
  typeCastedArr = []
  for col, dataType in columnsDict.items():
    val = dataType.strip().upper()
    if val != "STRING":
      expr = "CAST(" + col + " AS " + val +")"
      typeCastedArr.append(expr)
    else:
      typeCastedArr.append(col)
  return typeCastedArr

# ConvertColumnTypes(columnsDict)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from  pyspark.sql.types import *

rdd = sc.parallelize([Row("James","34","2006-01-01","True","M","3000.60"),
    Row("Michael","33","1980-01-10","True","F","3300.80"),
    Row("Robert","37","06-01-1992","False","M","5000.50")])

# First - Updated records (update)
# Second - Same as Existing Records 
# Third - Exist in Target but not in incoming records (delte)
# Fourth - Inactive records of current active records (History)
# Fifth - New records (insert)
targetRdd = sc.parallelize([
  Row("James",34,"2006-01-01",True,"M",2500.60,12,True,"2010-01-01","2099-01-01"),
  Row("Michael",33,"1980-01-10",True,"F",3300.80,13,True,"2010-01-01","2099-01-01"),
  Row("Madan",34,"1980-01-10",True,"F",3200.80,12,True,"2010-01-01","2099-01-01"),
  Row("James",34,"2006-01-01",True,"M",3500.60,11,False,"2009-01-01","2010-01-01")
]
)
source_Schema  =  StructType([
    StructField("firstName",StringType(),True),
    StructField("age",StringType(),True),
    StructField("jobStartDate",StringType(),True),
    StructField("isGraduated", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
  ])
target_Schema  =  StructType([
    StructField("firstName",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("jobStartDate",StringType(),True),
    StructField("isGraduated", BooleanType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("batchId", IntegerType(), True),
    StructField("isActive", BooleanType(), True),
    StructField("record_Start_Date", StringType(), True),
    StructField("record_End_Date", StringType(), True)
  ])


# COMMAND ----------

def getColumnsMap(colsType, rdd):
  return rdd.filter(lambda x : x[0] == colsType).map(lambda x : (x[1],x[2])).collectAsMap()

etl_Columns = ["batchId","isActive","record_Start_Date","record_End_Date"]

def GenerateScdType2Exressions(naturalCols,scdCols):
  joinClause = compositeKey = notEqualClause = equalClause =''
  
  for col in naturalCols:
    joinClause+= "a." + col + "=" + "b." + col + " AND "
    compositeKey += "a."+col+","
  for col in scdCols:
    notEqualClause += "a." + col + "<>" + "b." +col+ " AND "   
    equalClause += "a." + col + "=" + "b." +col+ " AND "
    
  whereClause = " b.isActive = True"  
  updateExpr = joinClause + notEqualClause + whereClause
#   updateExpr = ''.join(expr.rsplit('AND ', 1))
  
  similarExpr = joinClause + equalClause + whereClause
#   similarExpr = ''.join(expr1.rsplit('AND ', 1))
  deleteExpr = "a.isActive = True"
  expr2 = "concat("+compositeKey+") as MergeKey"
  compositeKeyExpr = ''.join(expr2.rsplit(',', 1))
#   print(compositeKeyExpr)
  
  return (updateExpr,similarExpr,deleteExpr,compositeKeyExpr)

def updateRecords(sourceTable,target,expr,naturalCols,joinType,combined):
  return sourceTable.alias('a').join(target.alias('b'),naturalCols,joinType).where(expr)

# COMMAND ----------

from ast import literal_eval
configRDD = sc.textFile("adl://us6datahubadlsprod001.azuredatalakestore.net/data/foundation/hr-csi/common/global/workday/f_hr_cdp_workday/config/configFile.txt")
myRDD =  configRDD.map(lambda lines : lines.split(':='))
metaDataMap = getColumnsMap("MetaData",myRDD)
scdTypeMap = getColumnsMap("scdType",myRDD)

naturalColumns = literal_eval(scdTypeMap["NaturalColumns"])
scdColumns = literal_eval(scdTypeMap["scdColumns"])
(updateExpr,similarExpr,deleteExpr,comExpr) = GenerateScdType2Exressions(naturalColumns,scdColumns)
print(deleteExpr)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from  pyspark.sql.types import *
from delta.tables import *

source = spark.createDataFrame(rdd, source_Schema)
#Change the source datatypes
sourceSelExpr  = ConvertColumnTypes(metaDataMap)
finalSourceDF  = source.selectExpr(sourceSelExpr)
finalTargetDF  = spark.createDataFrame(targetRdd,target_Schema)
similarRecords = updateRecords(finalSourceDF,finalTargetDF,similarExpr,naturalColumns,'inner',comExpr).select("a.*")
deltaRecords   = finalSourceDF.subtract(similarRecords)
updatedRecords = updateRecords(deltaRecords,finalTargetDF,updateExpr,naturalColumns,'inner',comExpr).selectExpr(comExpr, "a.*")
deltedRecords  = updateRecords(finalTargetDF,deltaRecords,deleteExpr,naturalColumns,'left_anti',comExpr).selectExpr(comExpr, "a.*").drop(*etl_Columns)
incRecords     = deltaRecords.alias("a").selectExpr("null as MergeKey", "a.*")
sourceRecords  = incRecords.union(updatedRecords).union(deltedRecords)
finalTargetDF.write.format("delta").mode("overwrite").save("/mnt/delta/finalTargetDf")
targetDelta = DeltaTable.forPath(spark,"/mnt/delta/finalTargetDf")
(columns,condition,setTargetColumns) =  scdexpr(finalTargetDF.columns,naturalColumns,"1",'jobStartDate' )

targetDelta.alias("t").merge(sourceRecords.alias("s"), condition).whenMatchedUpdate(
set = setTargetColumns
  ).whenNotMatchedInsert(
  values = columns
  ).execute()

# if(scdTypeMap["Type"] == "Type2"):
  
# elif(scdTypeMap["Type"] == "Type1"):

# else:
  


# COMMAND ----------

targetDelta.toDF().show()

# COMMAND ----------

def scdexpr(columns,primaryKey,batch_id,start_time):
  dict={}
  compositeKey = ''
  for col in primaryKey:
    compositeKey += "t."+col+","
  expr2 = "concat("+compositeKey+")"
  compositeKeyExpr = ''.join(expr2.rsplit(',', 1))  
  condition = compositeKeyExpr + "=s.MergeKey"
  for col in columns:
    dict[col] = "s." + col
  dict["batchId"] = batch_id
  dict["record_Start_Date"]= 's.'+ start_time
  dict["record_End_Date"] = '2099-01-01'
  dict["isActive"] = "True"
  setTargetColumns={
    "record_End_Date":'s.'+ start_time,
    "isActive":"False"
    }
  return (dict,condition,setTargetColumns)

# COMMAND ----------

import json

def generateSchemaFromCols(columnsDict):
  json_type = {"fields": list(), "type": "struct"}
  for col,type in columnsDict.items():
    json_type['fields'].append({
      "metadata":{},
      "name":col,
      "nullable":True,
      "type":type.strip()
      })
  schemajson = json.dumps(json_type)
  return StructType.fromJson(json.loads(schemajson))
  

# COMMAND ----------

cols = {"firstName":"string",
"age":"integer",
"jobStartDate":"string",
"isGraduated":"boolean",
"gender":"string",
"salary":"double"}

# schema = generateSchemaFromCols(cols)
expr   = ConvertColumnTypes(cols)
df.selectExpr("concat(firstName,gender)").show()