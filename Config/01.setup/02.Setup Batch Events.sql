-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Setup Batch Events

-- COMMAND ----------

create schema if not exists formula1.control
managed location 'abfss://formula1@databrickstrainingextdl3.dfs.core.windows.net/control'

-- COMMAND ----------

create table if not exists formula1.control.batch_events
(
    batch_id int,
    event_timestamp timestamp
)

-- COMMAND ----------

select * from formula1.control.batch_events

-- COMMAND ----------

insert into formula1.control.batch_events
values (1, current_timestamp())