# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Sprints Files with multi line JSON

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

sourcefile = f'{landingfolderpath}/{v_batch_id}/sprints'
tablename = f'{catalogname}.{bronzeschema}.RECP_SPRINTS'

# COMMAND ----------

from pyspark.sql.types import *

SprintsSchema = StructType([
    StructField('date', DateType()),
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

SprintsDF = spark.read.json(sourcefile, schema=SprintsSchema, multiLine=True, mode='FAILFAST')

SprintsFinalDF = AddIngestionmetadata(SprintsDF)

#SprintsFinalDF.write.mode('overwrite').saveAsTable(tablename)

write_to_bronze(SprintsFinalDF, tablename, v_batch_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select season, batch_id, count(1) from formula1_incr.bronze.recp_sprints group by season, batch_id order by season;

# COMMAND ----------

