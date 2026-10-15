# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Races Data

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_RACES'
silvertable = f'{catalogname}.{silverschema}.FCT_RACES'

# COMMAND ----------

from pyspark.sql import functions as F
racesdf = spark.read.table(bronzetable).filter(F.col('batch_id') == v_batch_id)

# COMMAND ----------

racesselecteddf = racesdf.select('season', 'round', 'raceName', 'date', 'circuitId', 'Timestamp', 'Sourcefile', "batch_id")

racesrenameddf = racesselecteddf.withColumnsRenamed(
    {'raceName': 'race_name', 'circuitId': 'circuit_id', 'date':'race_date'}
)

racesdistinctdf = racesrenameddf.dropDuplicates(['season', 'round'])

racesfinalDF = racesdistinctdf.withColumn("race_name",F.initcap(F.col("race_name")))

# COMMAND ----------

write_to_silver(
    racesfinalDF,
    silvertable,
    't.season = s.season and t.round = s.round',
    ['season',
     'round',
     'race_name',
     'race_date',
     'circuit_id',
     'Timestamp',
     'Sourcefile',
     'batch_id']
    )

# COMMAND ----------

#racesfinalDF.write.format('delta').mode('overwrite').saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table(silvertable))

# COMMAND ----------

