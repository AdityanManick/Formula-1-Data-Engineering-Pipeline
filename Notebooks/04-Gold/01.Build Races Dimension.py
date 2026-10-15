# Databricks notebook source
# MAGIC %md
# MAGIC # Building Races dimension from the silver races

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

targettable = f"{catalogname}.{goldschema}.dim_races"

# COMMAND ----------

circuitsDF = spark.table(f"{catalogname}.{silverschema}.fct_circuits").filter("batch_id = '{0}'".format(v_batch_id))
racesDF = spark.table(f"{catalogname}.{silverschema}.fct_races").filter("batch_id = '{0}'".format(v_batch_id))

# COMMAND ----------

dimracesDF = racesDF.join(circuitsDF, on = "circuit_id", how = 'inner').select(racesDF.season, racesDF.round, racesDF.race_name, racesDF.race_date,circuitsDF.circuit_name, circuitsDF.locality, circuitsDF.country)

# COMMAND ----------

write_to_gold(
    dimracesDF,
    targettable,
    't.season = s.season and t.round = s.round',
    [
        'race_name',
        'race_date',
        'circuit_name',
        'locality',
        'country'
    ]
)

# COMMAND ----------

#dimracesDF.write.format("delta").mode("overwrite").saveAsTable(targettable)

# COMMAND ----------

#display(spark.table(targettable))

# COMMAND ----------

