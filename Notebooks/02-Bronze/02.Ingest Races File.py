# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Race File

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Bronze-helper

# COMMAND ----------

sourcefile = f'{landingfolderpath}/{v_batch_id}/races.csv'
tablename = f'{catalogname}.{bronzeschema}.RECP_RACES'

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

RaceSchema = StructType([
    StructField("season", IntegerType()),
    StructField("round", IntegerType()),
    StructField("url", StringType()),
    StructField("raceName", StringType()),
    StructField("date", DateType()),
    StructField("circuitId", StringType())
])

RaceDF = spark.read.csv(sourcefile, header=True, schema=RaceSchema, mode="FAILFAST")
#display(RaceDF)

RaceFinalDF = AddIngestionmetadata(RaceDF)

#RaceFinalDF = RaceDF.withColumn("Timestamp", F.current_timestamp()).withColumn("Sourcefile", F.col("_metadata.file_path"))
#display(RaceFinalDF)
write_to_bronze(RaceFinalDF, tablename, v_batch_id)
#RaceFinalDF.write.format("delta").mode("overwrite").saveAsTable(tablename)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from formula1_incr.bronze.recp_races;

# COMMAND ----------

#display(spark.table(tablename))

# COMMAND ----------

