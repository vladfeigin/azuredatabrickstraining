-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Delta Live Table demo (DLT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loading raw data into DLT. 
-- MAGIC
-- MAGIC Create **Streaming** Live Table by loading Status data from Azure Blob Storage  
-- MAGIC
-- MAGIC Streaming Live tables only supports reading from "append-only" streaming sources (Kafka, Event Hub, Blob Storage)
-- MAGIC
-- MAGIC Only reads each input batch once, no matter what (even if joined dimensions change, or if the query definition changes, etc).
-- MAGIC
-- MAGIC Can perform operations on the table outside the managed DLT Pipeline (append data, perform GDPR, etc).

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE status_bronze
COMMENT "Statuses raw data loaded by Autoloader"
AS SELECT current_timestamp() as proc_time, input_file_name() as source_file, * 
FROM cloud_files("abfss://${container_name}@${storage_account}.dfs.core.windows.net/dlt_demo/data/status/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE status_silver
(CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1691942211) ON VIOLATION DROP ROW)
AS SELECT * EXCEPT (source_file, _rescued_data)
FROM STREAM(LIVE.status_bronze)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE email_updates
AS SELECT a.*, b.email
FROM LIVE.status_silver a
INNER JOIN LIVE.subscribed_order_emails_v b
ON a.order_id = b.order_id;
