# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Results Data

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_RESULTS'
silvertable = f'{catalogname}.{silverschema}.FCT_RESULTS'

# COMMAND ----------

resultsDF = spark.read.table(bronzetable).filter(f"batch_id = '{v_batch_id}'").drop("url")

# COMMAND ----------

resultsrenamedDF = resultsDF.withColumnsRenamed(
    {'constructorId': 'constructor_id',
     'driverId': 'driver_id',
     'raceName': 'race_name',
     'date': 'race_date',
     'grid': 'grid_position',
     'laps': 'completed_laps',
     'number': 'car_number',
     'position': 'final_position',
     'positionText': 'final_position_text'
    }
)

# COMMAND ----------

from pyspark.sql import functions as F
resultsvalidDF = resultsrenamedDF.filter(
    F.col("season").isNotNull()&
    F.col("round").isNotNull()&
    F.col("constructor_id").isNotNull()&
    F.col("driver_id").isNotNull()
).dropDuplicates(['season','round','constructor_id','driver_id'])

resultsfinalDF = resultsvalidDF.withColumn('race_name',F.initcap(F.col('race_name')))

# COMMAND ----------

write_to_silver(
    resultsfinalDF,
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

#resultsfinalDF.write.mode('overwrite').saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table(silvertable))

# COMMAND ----------

