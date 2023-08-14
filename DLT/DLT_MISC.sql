-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("storage_account", "")
-- MAGIC dbutils.widgets.text("container_name", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC print (storage_account)
-- MAGIC print (container_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-08-12T13:14:59Z&se=2023-08-31T21:14:59Z&spr=https&sv=2022-11-02&sr=c&sig=4Pn3kXSIGKiWXN%2FGHAUx%2FCr5D6G0TtMKhr%2BESCaLRHc%3D")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/dlt_demo_bronze_raw"))

-- COMMAND ----------

select * from dlt_demo_v1.customers_bronze limit 10

-- COMMAND ----------

select count(*) from dlt_demo_v1.customers_bronze_clean

-- COMMAND ----------

select count(*) from dlt_demo_v1.customers_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_log = spark.read.format('delta').load(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/dlt_demo/tables/system/events")
-- MAGIC event_log.createOrReplaceTempView("event_log_raw")
-- MAGIC display(event_log)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC latest_update_id = spark.sql("""
-- MAGIC     SELECT origin.update_id
-- MAGIC     FROM event_log_raw
-- MAGIC     WHERE event_type = 'create_update'
-- MAGIC     ORDER BY timestamp DESC LIMIT 1""").first().update_id
-- MAGIC
-- MAGIC print(f"Latest Update ID: {latest_update_id}")
-- MAGIC
-- MAGIC # Push back into the spark config so that we can use it in a later query.
-- MAGIC spark.conf.set('latest_update.id', latest_update_id)

-- COMMAND ----------

select * from event_log_raw where origin.update_id="6d237a1b-cd25-4d31-81c8-816af4383126"

-- COMMAND ----------

describe history dlt_demo_v1.customers_bronze_clean

-- COMMAND ----------

describe history dlt_demo_v1.customers_bronze

-- COMMAND ----------

describe history dlt_demo_v1.customer_counts_state_gold

-- COMMAND ----------

describe history    dlt_demo_v1.__apply_changes_storage_customers_silver --dlt_demo_v1.customers_silver

-- COMMAND ----------

describe extended dlt_demo_v1.customers_silver

-- COMMAND ----------

describe extended dlt_demo_v1.__apply_changes_storage_customers_silver

-- COMMAND ----------

describe extended dlt_demo_v1.customers_bronze

-- COMMAND ----------

describe extended dlt_demo_v1.customer_counts_state_gold
