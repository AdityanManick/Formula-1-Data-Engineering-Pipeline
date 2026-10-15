# Databricks notebook source
# MAGIC %md
# MAGIC #Build Constructiors Dimension

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

targettable = f'{catalogname}.{goldschema}.dim_constructors'

# COMMAND ----------

constructorsDF = spark.table(f'{catalogname}.{silverschema}.dim_constructors').filter(F.col('batch_id') == v_batch_id)
regionDF = spark.table(f'{catalogname}.{goldschema}.ref_nationality_region')

# COMMAND ----------

dimconstructorsDF = (
    constructorsDF.join(regionDF, on="nationality", how="left").select(constructorsDF.constructor_id, constructorsDF.constructor_name, constructorsDF.nationality, regionDF.region.alias("nationality_region")))

# COMMAND ----------

write_to_gold(
    dimconstructorsDF,
    targettable,
    't.constructor_id = s.constructor_id',
    [
        'constructor_name',
        'nationality',
        'nationality_region'
    ]
)

# COMMAND ----------

#dimconstructorsDF.write.mode("overwrite").saveAsTable(targettable)

# COMMAND ----------

#display(spark.table(targettable))

# COMMAND ----------

