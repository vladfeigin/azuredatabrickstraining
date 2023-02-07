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
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Delta Lake format
-- MAGIC 
-- MAGIC Let's explore Delta Lake format. 
-- MAGIC 
-- MAGIC Delta Lake provides ACID transactions, history, time travel, schema enforcement, performance optimizations  

-- COMMAND ----------

-- switch to our DB
use flights


-- COMMAND ----------

show tables

-- COMMAND ----------

create or replace table flight_delay_delta
using delta options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/'
)
as select * from flights_delays_external_303474

-- COMMAND ----------

describe extended flight_delay_delta

-- COMMAND ----------



-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Let's do update operation on the Delta Table. Great to see UPDATE on Big Data! 

-- COMMAND ----------

update flight_delay_delta
set Carrier = 'DL01'
where Carrier = 'DL'

-- COMMAND ----------

-- let's check the history
describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Let's do delete operation on the Delta Table. Great to see DELETE on Big Data!

-- COMMAND ----------

delete from flight_delay_delta
where Carrier = 'DL01'

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Time travel in action

-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'DL01'

-- COMMAND ----------

select count(*) from flight_delay_delta version as of 1
where Carrier = 'DL01'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### The data has been deleted by mistake! What can we do??!

-- COMMAND ----------

-- No problem with Delta 
restore table flight_delay_delta version as of 1


-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'DL01'

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Schema enforcement in Delta

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
-- MAGIC Schema enforcement in action!
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
-- MAGIC ##### Exercise 
-- MAGIC 
-- MAGIC Use time travel and restore _flight_delay_delta_ table to the version before Insert Into

-- COMMAND ----------


