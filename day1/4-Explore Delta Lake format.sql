-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Explore Delta Lake format

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
-- MAGIC ### Delta Lake format
-- MAGIC
-- MAGIC Let's explore Delta Lake format. 
-- MAGIC
-- MAGIC Delta Lake provides ACID transactions, history, time travel, schema enforcement, performance optimizations  

-- COMMAND ----------

-- switch to our DB
use flights_demo


-- COMMAND ----------

show tables

-- COMMAND ----------

create or replace table flight_delay_delta
using delta options(
path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/'
)
as select * from flight_delay_bronze


-- COMMAND ----------

describe extended flight_delay_delta

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let's do update operation on the Delta Table. Great to see UPDATE on Big Data! 

-- COMMAND ----------

select count(*), carrier from flight_delay_delta group by carrier

-- COMMAND ----------

update flight_delay_delta
set carrier = 'OO-01'
where carrier = 'OO'

-- COMMAND ----------

-- let's check the history
describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let's do delete operation on the Delta Table. Great to see DELETE on Big Data!

-- COMMAND ----------

delete from flight_delay_delta
where Carrier = 'OO-01'

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Get more details about the table, run **`describe detail`** table_name
-- MAGIC
-- MAGIC Pay attention to `Location` field
-- MAGIC

-- COMMAND ----------

describe detail flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Exploring Delta Lake files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/")
-- MAGIC # You can see that data in delta lake format is kept in standard parquet files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/_delta_log/")
-- MAGIC #_delta_log folder contains transactions log files (.json)
-- MAGIC # Each transaction results in a new JSON file being written to the Delta Lake transaction log.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC delta_json_df = spark.read.json(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/_delta_log/00000000000000000001.json")
-- MAGIC display(delta_json_df)
-- MAGIC #The add column contains a list of all the new files written to our table; the remove column indicates those files that no longer should be included in our table.
-- MAGIC #When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Time travel in action

-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'OO-01'

-- COMMAND ----------

select count(*) from flight_delay_delta version as of 1
where Carrier = 'OO-01'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### The data has been deleted by mistake! What can we do??!

-- COMMAND ----------

-- No problem with Delta 
restore table flight_delay_delta version as of 1


-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'OO-01'

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Schema enforcement in Delta

-- COMMAND ----------

create or replace table flight_delay_delta_demo_table
as select *
from flight_delay_delta


-- COMMAND ----------

-- add new column to the new table
alter table flight_delay_delta_demo_table
add column new_column int

-- COMMAND ----------

select * from flight_delay_delta_demo_table

-- COMMAND ----------

-- trying insert into, error happens because a schema mismatch
-- Error: Error in SQL statement: AnalysisException: A schema mismatch detected when writing to the Delta table
insert into flight_delay_delta
select * from flight_delay_delta_demo_table

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Schema enforcement in action!
-- MAGIC
-- MAGIC In this specific case you can enforce merge, since adding new field doesn't break a schema (echema evolution)
-- MAGIC
-- MAGIC Set Spark parameter below, to enforce ***schema merge*** and run the `INSERT INTO` once again

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",  "true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run again `INSERT INTO`

-- COMMAND ----------

-- trying insert into again after we enforced schema merge. It should work now
insert into flight_delay_delta
select * from flight_delay_delta_demo_table

-- COMMAND ----------

select count(*) from flight_delay_delta 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### Compacting Small Files and Indexing
-- MAGIC
-- MAGIC Small files can occur for a variety of reasons; in our case, we performed a number of operations where only several records were inserted.
-- MAGIC
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. It speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.
-- MAGIC
-- MAGIC For small data volumes **`ZORDER`** doesn't provide benefits.

-- COMMAND ----------

--optimize flight_delay_delta

optimize flight_delay_delta
zorder by (Year, Month)

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Cleaning Up Stale Files
-- MAGIC
-- MAGIC Databricks will automatically clean up stale files in Delta Lake tables.
-- MAGIC
-- MAGIC While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive.
-- MAGIC
-- MAGIC If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation.
-- MAGIC
-- MAGIC Use **`DRY RUN`** to test the what will be deleted by Vacuum
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;
 

-- COMMAND ----------

--By default, VACUUM will prevent you from deleting files less than 7 days old.
--If you run VACUUM on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.
--If you want to keep the latest table version you can add "RETAIN 0 HOURS", but it's not recommended. The recommendation is to stick to default (7 days) or follow your use case

--VACUUM flight_delay_delta 
VACUUM flight_delay_delta RETAIN 0 HOURS --DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Exercise 
-- MAGIC
-- MAGIC Use time travel and restore _flight_delay_delta_ table to the version before Insert Into
