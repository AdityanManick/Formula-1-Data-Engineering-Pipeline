-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup project environment
-- MAGIC
-- MAGIC Create external location

-- COMMAND ----------

create external location if not exists `databricks_training_ext_dl3_formula1-incr`
url 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/'
with (storage credential `databricks-training-sc`)
comment 'External loaction for Formula1 incremental project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Access the external location

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS `formula1_incr`
MANAGED LOCATION 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/'
COMMENT 'Catalog for Formula1 incremental project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze    
Managed LOCATION 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
Managed LOCATION 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
Managed LOCATION 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/gold';

-- COMMAND ----------

use catalog `formula1_incr`;

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Volume files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.sourcefiles
LOCATION 'abfss://formula1-incr@databrickstrainingextdl3.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/sourcefiles/

-- COMMAND ----------

