# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Cunstroctors Data

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_CONSTRUCTORS'
silvertable = f'{catalogname}.{silverschema}.DIM_CONSTRUCTORS'

# COMMAND ----------

constructorsDF = spark.read.table(bronzetable).filter(f"batch_id = '{v_batch_id}'")

# COMMAND ----------

from pyspark.sql import functions as F

constructordroppedDF = constructorsDF.drop('url')

constructorsrenamedDF = constructordroppedDF.withColumnsRenamed(
    {"constructorId": "constructor_id",
     "name": "constructor_name"}
)

constructordistinctDF = constructorsrenamedDF.dropDuplicates(['constructor_id'])

constructorfinalDF = constructordistinctDF.withColumn('nationality',F.initcap(F.col('nationality')))

# COMMAND ----------

write_to_silver(
    constructorfinalDF,
    silvertable,
    't.constructor_id = s.constructor_id',
    [
        'constructor_name',
        'nationality',
        'Timestamp',
        'Sourcefile',
        'batch_id'
    ]
)

# COMMAND ----------

#constructorfinalDF.write.mode('overwrite').saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table(silvertable))

# COMMAND ----------

