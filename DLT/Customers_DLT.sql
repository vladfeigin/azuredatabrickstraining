-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Delta Live Table demo (DLT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loading raw data into DLT. 
-- MAGIC
-- MAGIC Create **Streaming** Live Table by loading Customer data from Azure Blob Storage  
-- MAGIC
-- MAGIC Streaming Live tables only supports reading from "append-only" streaming sources (Kafka, Event Hub, Blob Storage)
-- MAGIC
-- MAGIC Only reads each input batch once, no matter what (even if joined dimensions change, or if the query definition changes, etc).
-- MAGIC
-- MAGIC Can perform operations on the table outside the managed DLT Pipeline (append data, perform GDPR, etc).

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "Raw data loaded by Autoloader"
AS SELECT current_timestamp() as proc_time, input_file_name() as source_file, * 
FROM cloud_files("abfss://${container_name}@${storage_account}.dfs.core.windows.net/dlt_demo/data/customers/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Streaming data from one streaming live table into other streaming live table
-- MAGIC
-- MAGIC In the stream we use **CONSTRAINT** to validate the data and define the action in case the dats is not valid

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT (
  (address IS NOT NULL and 
  city IS NOT NULL and 
  state IS NOT NULL and 
  zip_code IS NOT NULL) or
  operation = "DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW)
AS SELECT *
  FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Processing CDC Data with APPLY CHANGES INTO
-- MAGIC
-- MAGIC * Performs incremental/streaming ingestion of CDC data
-- MAGIC * You need to specify one or many fields as the primary key for a table
-- MAGIC * Default assumption is that rows will contain inserts and updates
-- MAGIC * Can optionally apply deletes
-- MAGIC * Automatically orders late-arriving records using user-provided sequencing key
-- MAGIC * Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
-- MAGIC * Will default to applying changes as Type 1 SCD
-- MAGIC
-- MAGIC
-- MAGIC Note: customers_silver is a view created on top of table __apply_changes_storage_customers_silver, which is created automatically by DLT

-- COMMAND ----------


CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Aggregation Live Table

-- COMMAND ----------

CREATE LIVE TABLE customer_counts_state_gold
  COMMENT "Total active customers per state"
AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
  FROM LIVE.customers_silver
  GROUP BY state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Live View
-- MAGIC
-- MAGIC Unlike views used elsewhere in Databricks, DLT views are not persisted to the metastore, meaning that they can only be referenced from within the DLT pipeline they are a part of.
-- MAGIC
-- MAGIC Note: DLT supports scheduling multiple notebooks as part of a single DLT Pipeline configuration. You can edit existing DLT pipelines to add additional notebooks.
-- MAGIC
-- MAGIC Within a DLT Pipeline, code in any notebook library can reference tables and views created in any other notebook library.
-- MAGIC
-- MAGIC Essentially, we can think of the scope of the schema reference by the LIVE keyword to be at the DLT Pipeline level, rather than the individual notebook.

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'
