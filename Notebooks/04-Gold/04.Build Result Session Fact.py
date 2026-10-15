# Databricks notebook source
# MAGIC %md
# MAGIC #Create Fact table for results and sprints

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

targettable = f'{catalogname}.{goldschema}.fct_session_result'

resultsDF = spark.table(f'{catalogname}.{silverschema}.fct_results').filter(F.col('batch_id')==v_batch_id).withColumn('session_type', F.lit('RACE')).drop('race_name','race_date','timestamp','sourcefile','batch_id','created_timestamp','updated_timestamp')
sprintsDF = spark.table(f'{catalogname}.{silverschema}.fct_sprints').filter(F.col('batch_id')==v_batch_id).withColumn('session_type', F.lit('SPRINT')).drop('race_name','race_date','timestamp','sourcefile','batch_id','created_timestamp','updated_timestamp')

# COMMAND ----------

resultssprintsDF = resultsDF.unionByName(sprintsDF)

fctsessionresultDF = resultssprintsDF.withColumn('is_win', F.col("final_position") == 1).withColumn('is_podium', F.col('final_position').between(1,3)).withColumn('has_points', F.col('points')>0)

# COMMAND ----------

write_to_gold(
    fctsessionresultDF,
    targettable,
    't.season = s.season and t.round = s.round and t.driver_id = s.driver_id and t.session_type = s.session_type and t.constructor_id = s.constructor_id',
    [
        'grid_position',
        'completed_laps',
        'car_number',
        'points',
        'final_position',
        'final_position_text',
        'status',
        'is_win',
        'is_podium',
        'has_points'
    ]
)

# COMMAND ----------

#fctsessionresultDF.write.mode('overwrite').saveAsTable(targettable)

# COMMAND ----------

#display(spark.table(targettable))

# COMMAND ----------

