# Databricks notebook source
# MAGIC %md
# MAGIC #Create Control Table

# COMMAND ----------

# MAGIC %run ../00-common/environment-config

# COMMAND ----------

spark.sql(f'create schema if not exists {catalogname}.{controlschema}')

# COMMAND ----------

spark.sql(f'''
          create table if not exists {catalogname}.{controlschema}.batch_control
          (
            batch_id string,
            status string,
            created_timestamp timestamp,
            updated_timestamp timestamp
          )''')

# COMMAND ----------

display(spark.table(f'{catalogname}.{controlschema}.batch_control'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `formula1_incr`.`control`.`batch_control`;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from `formula1_incr`.`control`.`batch_control`;

# COMMAND ----------

