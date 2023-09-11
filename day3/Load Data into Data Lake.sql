-- Databricks notebook source
-- MAGIC %md
-- MAGIC #In this notebook we explore techniques loading data into Data Lake
-- MAGIC

-- COMMAND ----------

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
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-09-07T14:17:14Z&se=2023-11-30T23:17:14Z&spr=https&sv=2022-11-02&sr=c&sig=jyWEvg%2FzLmK9J%2BOxIp%2B8QSCKYpVmNPfKNcNIo68Rh6E%3D")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **`CREATE OR REPLACE TABLE`** (CRAS) statements fully replace the contents of a table each time they execute.
-- MAGIC This operation automatically replaces all of the data in a table.
-- MAGIC - Overwriting a table is much faster because it doesn’t need to list the directory recursively or delete any files.
-- MAGIC - The old version of the table still exists; can easily retrieve the old data using Time Travel.
-- MAGIC - It’s an atomic operation. Concurrent queries can still read the table while you are deleting the table.
-- MAGIC - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC

-- COMMAND ----------

create or replace table raw_kafka_data as 
select * from  parquet.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka_2/`

-- COMMAND ----------

describe history raw_kafka_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`** Very similar to Create or Replace (CRAS)
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**:
-- MAGIC
-- MAGIC - Can only overwrite an existing table, not create a new one like our CRAS statement
-- MAGIC - Can overwrite only with new records that match the current table schema -- and thus can be a "safer" technique for overwriting an existing table without disrupting downstream consumers
-- MAGIC   
-- MAGIC   Whereas a CRAS statement will allow us to completely redefine the contents of our target table, INSERT OVERWRITE will fail if we try to change our schema (unless we provide optional settings).
-- MAGIC
-- MAGIC - Can overwrite individual partitions
-- MAGIC

-- COMMAND ----------

INSERT OVERWRITE raw_kafka_data
SELECT * FROM parquet.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka_2/`

-- COMMAND ----------

describe history raw_kafka_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Append Rows
-- MAGIC
-- MAGIC  **`INSERT INTO`** Atomically appends new rows to an existing Delta table. This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.
-- MAGIC
-- MAGIC  Note that **`INSERT INTO`** does not have any built-in guarantees to prevent inserting the same records multiple times. 
-- MAGIC
-- MAGIC

-- COMMAND ----------

create or replace table raw_kafka_data_dup as 
select * from  parquet.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka_2/`

-- COMMAND ----------

--duplicated records will be created
insert into raw_kafka_data_dup
select * from raw_kafka_data

-- COMMAND ----------

select count(*) from raw_kafka_data_dup

-- COMMAND ----------

select count(*) from raw_kafka_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Merge
-- MAGIC
-- MAGIC The main benefits of **`MERGE`**:
-- MAGIC * updates, inserts, and deletes are completed as a single transaction
-- MAGIC * multiple conditionals can be added in addition to matching fields
-- MAGIC * provides extensive options for implementing custom logic
-- MAGIC
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC With merge, you can avoid inserting the duplicate records by performing an insert-only merge.
-- MAGIC
-- MAGIC  **`WHEN NOT MATCHED THEN INSERT `**  
-- MAGIC

-- COMMAND ----------


merge into raw_kafka_data_dup a
using raw_kafka_data b
on a.key = b.key
when not matched then insert *


-- COMMAND ----------

select * from raw_kafka_data_dup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Load Incrementally
-- MAGIC
-- MAGIC **`COPY INTO`** provides an idempotent option to incrementally ingest data from external systems.
-- MAGIC
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC
-- MAGIC This operation is potentially much cheaper than full table scans for data that grows predictably.
-- MAGIC

-- COMMAND ----------

COPY INTO raw_kafka_data_dup
FROM "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka_2/"
FILEFORMAT = PARQUET

-- COMMAND ----------

select count(*) from raw_kafka_data_dup
