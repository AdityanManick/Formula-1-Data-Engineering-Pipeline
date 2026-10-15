# Databricks notebook source
from pyspark.sql import functions as F

def AddIngestionmetadata(df):
    ingestdf = df.withColumn("Timestamp", F.current_timestamp()).withColumn("Sourcefile", F.col("_metadata.file_path"))
    return ingestdf


# COMMAND ----------

def write_to_bronze(input_df, target_table, batch_id):
    final_df = input_df.withColumn("batch_id", F.lit(batch_id))
    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("batch_id")
        .option("replaceWhere", f"batch_id = '{batch_id}'")
        .saveAsTable(target_table)
    )

# COMMAND ----------

