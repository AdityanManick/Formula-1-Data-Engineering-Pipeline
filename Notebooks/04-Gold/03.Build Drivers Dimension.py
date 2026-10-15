# Databricks notebook source
# MAGIC %md
# MAGIC #Build Drivers Dimension table

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

targettable = f'{catalogname}.{goldschema}.dim_drivers'

driversDF = spark.table(f'{catalogname}.{silverschema}.dim_drivers').filter(F.col('batch_id') == v_batch_id)
regionDF = spark.table(f'{catalogname}.{goldschema}.ref_nationality_region')

# COMMAND ----------

dimdriversDF = (
    driversDF.join(regionDF, on="nationality", how="left").select(driversDF.driver_id, driversDF.driver_name, driversDF.date_of_birth, driversDF.nationality, regionDF.region.alias("nationality_region"))
)
#display(dimdriversDF)

# COMMAND ----------

write_to_gold(
    dimdriversDF,
    targettable,
    't.driver_id = s.driver_id',
    [
        'driver_name',
        'date_of_birth',
        'nationality',
        'nationality_region'
    ]
)

# COMMAND ----------

#dimdriversDF.write.mode("overwrite").saveAsTable(targettable)

# COMMAND ----------

#display(spark.table(targettable))

# COMMAND ----------

