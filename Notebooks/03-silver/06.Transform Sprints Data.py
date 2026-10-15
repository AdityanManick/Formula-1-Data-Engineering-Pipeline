# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Sprints Data

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_SPRINTS'
silvertable = f'{catalogname}.{silverschema}.FCT_SPRINTS'

# COMMAND ----------

sprintsDF = spark.table(bronzetable).filter(f"batch_id='{v_batch_id}'").drop('url').withColumnsRenamed(
            {'constructorId':'constructor_id',
            'driverId':'driver_id',
            'raceName':'race_name',
            'positiontext':'final_position_text',
            'date':'race_date',
            'grid':'grid_position',
            'laps':'completed_laps',
            'number': 'car_number',
            'position': 'final_position'
            }
)

# COMMAND ----------

from pyspark.sql import functions as F
sprintsvalidDF = sprintsDF.filter(F.col('season').isNotNull()&F.col('round').isNotNull()&F.col('constructor_id').isNotNull()&F.col('driver_id').isNotNull()).dropDuplicates(['season','round','constructor_id','driver_id'])

sprintsfinalDF = sprintsvalidDF.withColumn('race_name',F.initcap(F.col('race_name')))

# COMMAND ----------

write_to_silver(
    sprintsfinalDF,
    silvertable,
    't.season = s.season and t.round = s.round and t.constructor_id = s.constructor_id and t.driver_id = s.driver_id',
    [
        'race_date',
        'race_name',
        'grid_position',
        'completed_laps',
        'car_number',
        'points',
        'final_position',
        'final_position_text',
        'status',
        'Timestamp',
        'Sourcefile',
        'batch_id'
    ]
)

# COMMAND ----------

#sprintsfinalDF.write.mode('overwrite').saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table(silvertable))

# COMMAND ----------

