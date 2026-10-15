# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circutes data from bronze to silver

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

# MAGIC %run ../00-common/Silver-helper

# COMMAND ----------

bronzetable = f'{catalogname}.{bronzeschema}.RECP_circuits'
silvertable = f'{catalogname}.{silverschema}.FCT_circuits'

# COMMAND ----------

from pyspark.sql import functions as F
circutesDF = spark.read.table(bronzetable).filter(F.col('batch_id') == v_batch_id)

# COMMAND ----------

circutesselectedDF = circutesDF.select(
    F.col('circuitId'), 
    F.col('circuitName'), 
    F.col('lat'), 
    F.col('lng'), 
    F.col('locality'), 
    F.col('country'), 
    F.col('Timestamp'), 
    F.col('Sourcefile'),
    F.col('batch_id')
    )

# COMMAND ----------

circutesrenamedDF = circutesselectedDF.withColumnsRenamed(
    {"circuitId": "circuit_id", 
     "circuitName": "circuit_name", 
     "lat": "latitude", 
     "lng": "longitude"}
)

# COMMAND ----------

circutesvalidDF = circutesrenamedDF.filter("circuit_id IS NOT NULL")

#circuitesvalidDF = circutesrenamedDF.filter(F.col("circuit_id").isNotNull())

circutesdistinctDF = circutesvalidDF.dropDuplicates(["circuit_id"])

circutesfinalDF = circutesdistinctDF.withColumn("circuit_name", F.initcap(F.col("circuit_name"))).withColumn("locality", F.initcap(F.col("locality")))

# COMMAND ----------

#circutesfinalDF = circutesfinalDF.withColumn("created_timestamp",F.current_timestamp()).withColumn("updated_timestamp",F.current_timestamp())

# COMMAND ----------

'''if not spark.catalog.tableExists(silvertable):
  circutesfinalDF.write.format("delta").mode("overwrite").saveAsTable(silvertable)
else:
    from delta.tables import DeltaTable
    deltatable = DeltaTable.forName(spark, silvertable)
    (
        deltatable.alias("t")
        .merge(circutesfinalDF.alias("s"), "t.circuit_id = s.circuit_id")
        .whenMatchedUpdate(
            condition="s.batch_id >= t.batch_id",
            set={
                "circuit_name": "s.circuit_name",
                "latitude": "s.latitude",
                "longitude": "s.longitude",
                "locality": "s.locality",
                "country": "s.country",
                "timestamp": "s.timestamp",
                "sourcefile": "s.sourcefile",
                "batch_id": "s.batch_id",
                "updated_timestamp": "s.updated_timestamp"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )'''

# COMMAND ----------

write_to_silver(
    circutesfinalDF,
    silvertable,
    't.circuit_id = s.circuit_id', 
    ['circuit_id',
     'circuit_name',
     'latitude',
     'longitude',
     'locality',
     'country',
     'Timestamp',
     'Sourcefile',
     'batch_id']
    )

# COMMAND ----------

#circutesfinalDF.write.format("delta").mode("overwrite").saveAsTable(silvertable)

# COMMAND ----------

#display(spark.table('formula1_incr.bronze.recp_circuits'))

# COMMAND ----------

