# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting Constructors.json File

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

Sourcefile = f'{landingfolderpath}/{v_batch_id}/constructors.json'
Tablename = f'{catalogname}.{bronzeschema}.RECP_CONSTRUCTORS'

# COMMAND ----------

ConstructorSchema = "constructorId STRING, name STRING, nationality STRING, url STRING"

ConstructorDF =(
    spark.read.format('json').schema(ConstructorSchema).option('mode', 'FASTFAIL').load(Sourcefile)
    )

ConstructorFinalDF = AddIngestionmetadata(ConstructorDF)
#ConstructorFinalDF.write.mode('overwrite').format('delta').saveAsTable(Tablename)
write_to_bronze(ConstructorFinalDF, Tablename, v_batch_id)

# COMMAND ----------

#display(spark.table(Tablename))

# COMMAND ----------

