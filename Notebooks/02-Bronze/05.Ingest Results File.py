# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting Results Files

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

sourcefile = f'{landingfolderpath}/{v_batch_id}/results'
tablename = f'{catalogname}.{bronzeschema}.RECP_RESULTS'

# COMMAND ----------

from pyspark.sql.types import *

ResultSchema = StructType([
    StructField("date", DateType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', FloatType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType())
])

# COMMAND ----------

ResultsDF = spark.read.json(sourcefile, schema=ResultSchema, mode='FAILFAST')

ResultFinalDF = AddIngestionmetadata(ResultsDF)

#ResultFinalDF.write.mode('overwrite').format('delta').saveAsTable(tablename)

write_to_bronze(ResultFinalDF, tablename, v_batch_id)

# COMMAND ----------

'''
from pyspark.sql import functions as F

maxdate = ResultFinalDF.agg(F.max('date')).collect()[0][0]

print(maxdate)
'''

# COMMAND ----------

#display(spark.table(tablename))

# COMMAND ----------

