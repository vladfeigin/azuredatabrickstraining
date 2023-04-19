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
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-03-21T06:47:36Z&se=2023-06-04T13:47:36Z&spr=https&sv=2021-12-02&sr=c&sig=ioUnTbdgyKcGvCEUWOW875R32Vi8BinW%2BA8SasK7Nlo%3D")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Delta Lake format
-- MAGIC 
-- MAGIC Let's explore Delta Lake format. 
-- MAGIC 
-- MAGIC Delta Lake provides ACID transactions, history, time travel, schema enforcement, performance optimizations  

-- COMMAND ----------

-- switch to our DB
use flight_demo


-- COMMAND ----------

show tables

-- COMMAND ----------

create or replace table flight_delay_delta
using delta options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelayDelta/'
)
as select * from flight_delay_bronze


-- COMMAND ----------

describe extended flight_delay_delta

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Let's do update operation on the Delta Table. Great to see UPDATE on Big Data! 

-- COMMAND ----------

select count(*), carrier from flight_delay_delta group by carrier

-- COMMAND ----------

update flight_delay_delta
set Carrier = 'OO01'
where Carrier = 'OO'

-- COMMAND ----------

-- let's check the history
describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Let's do delete operation on the Delta Table. Great to see DELETE on Big Data!

-- COMMAND ----------

delete from flight_delay_delta
where Carrier = 'OO01'

-- COMMAND ----------

describe history flight_delay_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Time travel in action

-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'OO01'

-- COMMAND ----------

select count(*) from flight_delay_delta version as of 4
where Carrier = 'OO01'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### The data has been deleted by mistake! What can we do??!

-- COMMAND ----------

-- No problem with Delta 
restore table flight_delay_delta version as of 4


-- COMMAND ----------

select count(*) from flight_delay_delta where Carrier = 'OO01'

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
-- MAGIC Vacuum

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;
 

-- COMMAND ----------

VACUUM flight_delay_delta DRY RUN
--VACUUM flight_delay_delta RETAIN 1 HOURS 

-- COMMAND ----------

optimize flight_delay_delta

-- COMMAND ----------

optimize flight_delay_delta
zorder by (Year, Month, DayOfMonth)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Exercise 
-- MAGIC 
-- MAGIC Use time travel and restore _flight_delay_delta_ table to the version before Insert Into
