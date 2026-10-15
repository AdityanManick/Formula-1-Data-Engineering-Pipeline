# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting the Drivers.json file

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

sourcefile = f'{landingfolderpath}/{v_batch_id}/drivers.json'
tablename = f'{catalogname}.{bronzeschema}.RECP_DRIVERS'

# COMMAND ----------

from pyspark.sql.types import *

NameSchema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

DriversSchema = StructType([
    StructField('driverId', StringType()),
    StructField('name', NameSchema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

DriversDF = spark.read.json(sourcefile, schema=DriversSchema, mode = 'FAILFAST')

DriversFinalDF = AddIngestionmetadata(DriversDF)

#DriversFinalDF.write.mode('overwrite').format('delta').saveAsTable(tablename)

write_to_bronze(DriversFinalDF, tablename, v_batch_id)

# COMMAND ----------

#display(spark.table(tablename))

# COMMAND ----------

