# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Drivers Data

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_DRIVERS'
silvertable = f'{catalogname}.{silverschema}.DIM_DRIVERS'

# COMMAND ----------

driversDF = spark.read.table(bronzetable).filter(f"batch_id = '{v_batch_id}'")

# COMMAND ----------

from pyspark.sql import functions as F

driversdroppedDF = driversDF.drop('url')

driversrenamedDF = driversdroppedDF.withColumnsRenamed(
    {'driverid': 'driver_id', 'dateOfBirth':'date_of_birth'}
)

driversconcatDF = driversrenamedDF.withColumn('driver_name', F.initcap(F.concat_ws(' ', F.col('name.givenname'), F.col('name.familyname')))).drop('name')

driversdistinctDF = driversconcatDF.dropDuplicates(['driver_id'])
driverfinalDF = driversdistinctDF.withColumn('nationality', F.initcap(F.col('nationality')))

# COMMAND ----------

write_to_silver(
    driverfinalDF,
    silvertable,
    't.driver_id = s.driver_id',
    [
        'date_of_birth',
        'driver_name',
        'nationality',
        'Timestamp',
        'Sourcefile',
        'batch_id'
    ]
)

# COMMAND ----------

#driverfinalDF.write.mode('overwrite').saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table(silvertable))

# COMMAND ----------

