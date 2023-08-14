-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Delta Live Table demo (DLT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loading raw data into DLT. 
-- MAGIC
-- MAGIC Create **Streaming** Live Table by loading Orders data from Azure Blob Storage  
-- MAGIC
-- MAGIC Streaming Live tables only supports reading from "append-only" streaming sources (Kafka, Event Hub, Blob Storage)
-- MAGIC
-- MAGIC Only reads each input batch once, no matter what (even if joined dimensions change, or if the query definition changes, etc).
-- MAGIC
-- MAGIC Can perform operations on the table outside the managed DLT Pipeline (append data, perform GDPR, etc).

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
COMMENT "Orders raw data loaded by Autoloader"
AS SELECT current_timestamp() as proc_time, input_file_name() as source_file, * 
FROM cloud_files("abfss://${container_name}@${storage_account}.dfs.core.windows.net/dlt_demo/data/orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Validate data and create orders_silver table

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_silver
(CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
FROM STREAM(LIVE.orders_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create aggregattive table 

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE orders_by_date
AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
FROM LIVE.orders_silver
GROUP BY date(order_timestamp)
