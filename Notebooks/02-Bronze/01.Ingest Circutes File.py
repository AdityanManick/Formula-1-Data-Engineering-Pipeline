# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Reading the circute file

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

sourcefile = f'{landingfolderpath}/{v_batch_id}/circuits.csv'
tablename = f'{catalogname}.{bronzeschema}.RECP_CIRCUITS'

# COMMAND ----------

from pyspark.sql.types import *

circuteSchema = StructType([StructField("circuitId", StringType()), 
                           StructField("url", StringType()),
                           StructField("circuitName", StringType()),
                           StructField("lat", DoubleType()),
                           StructField("lng", DoubleType()),
                           StructField("locality", StringType()),
                           StructField("country", StringType()),
                           ])

# COMMAND ----------

CircuteDF = spark.read.csv(sourcefile, header=True, schema=circuteSchema, mode="FAILFAST")
#display(CircuteDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Add the metadata columns

# COMMAND ----------

CircutesFinalDF = AddIngestionmetadata(CircuteDF)

#display(CircutesFinalDF)


# COMMAND ----------

#CircutesFinalDF = CircutesFinalDF.withColumn("batch_id", F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC Write to bronze delta table

# COMMAND ----------

write_to_bronze(CircutesFinalDF, tablename, v_batch_id)


# COMMAND ----------

'''(
    CircutesFinalDF.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("batch_id")
    .option("replaceWhere", "batch_id = " + v_batch_id)
    .saveAsTable(tablename)
    )'''

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from formula1_incr.bronze.recp_circuits;

# COMMAND ----------

#display(spark.table("formula1_incr.bronze.RECP_CIRCUITS"))

# COMMAND ----------

