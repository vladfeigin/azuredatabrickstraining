-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("storage_account", "")
-- MAGIC dbutils.widgets.text("container_name", "")
-- MAGIC dbutils.widgets.text("table_name", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC table_name = getArgument("table_name")
-- MAGIC file_name = table_name + ".csv"
-- MAGIC print (storage_account)
-- MAGIC print (container_name)
-- MAGIC print (file_name)
-- MAGIC print (table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC table_name = getArgument("table_name")
-- MAGIC file_name = table_name + ".csv"
-- MAGIC print (storage_account)
-- MAGIC print (container_name)
-- MAGIC print (file_name)
-- MAGIC print (table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("file.name", file_name)

-- COMMAND ----------

create
or replace temp view flight_delay_bronze_view using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/${file.name}"
)

-- COMMAND ----------

select count(*) from flight_delay_bronze_view

-- COMMAND ----------

drop table if exists flight_delay_delta;

create table flight_delay_delta
using delta options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/performance_tests/flight_delay_delta/'
)
as 
select * from flight_delay_bronze_view

-- COMMAND ----------

select * from flight_delay_delta

-- COMMAND ----------

drop table if exists flight_delay_csv;

create table flight_delay_csv
using csv options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/performance_tests/flight_delay_csv/'
)
as 
select * from flight_delay_bronze_view

-- COMMAND ----------

select * from flight_delay_csv

-- COMMAND ----------

select count(*) from flight_delay_csv

-- COMMAND ----------

select count(*) from flight_delay_delta

-- COMMAND ----------

select max(DepDelay) as maxdelay, OriginAirportName as origin
from flight_delay_delta
group by OriginAirportName
order by maxdelay desc


-- COMMAND ----------

select max(DepDelay) as maxdelay, OriginAirportName as origin
from flight_delay_csv
group by OriginAirportName
order by maxdelay desc

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15
group by OriginAirportName
order by origin desc


-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_csv
where DepDelay >= 15
group by OriginAirportName
order by origin desc

-- COMMAND ----------

refresh table flight_delay_csv

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_csv
where DepDelay >= 15 and Year==2013 and Month=7 and DayOfMonth=7
group by OriginAirportName
order by origin desc

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15 and Year==2013 and Month=7 and DayOfMonth=7
group by OriginAirportName
order by origin desc

-- COMMAND ----------

update flight_delay_delta
set Carrier='WN_up'
where Carrier='WN'

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15
group by OriginAirportName
order by origin desc

-- COMMAND ----------

update flight_delay_delta
set Carrier='US_up'
where Carrier='US'

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15
group by OriginAirportName
order by origin desc

-- COMMAND ----------

refresh table flight_delay_delta

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15 and Year==2013 and Month=7 and DayOfMonth=7
group by OriginAirportName
order by origin desc

-- COMMAND ----------

optimize flight_delay_delta 

-- COMMAND ----------

optimize flight_delay_delta
zorder by (Year, Month, DayOfMonth);
optimize flight_delay_delta
zorder by  (Carrier)

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where DepDelay >= 15 and Year==2013 and Month=7 and DayOfMonth=7 
group by OriginAirportName
order by origin desc

-- COMMAND ----------

describe history  flight_delay_delta

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM flight_delay_delta RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

VACUUM flight_delay_delta RETAIN 0 HOURS

-- COMMAND ----------

drop table if exists flight_delay_delta_part;
create table flight_delay_delta_part
using delta 
options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/performance_tests/flight_delay_delta_part')
partitioned by (Year, Month, DayofMonth, Carrier)
as 
select * from flight_delay_delta

-- COMMAND ----------

refresh table flight_delay_delta_part;
refresh table flight_delay_delta

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta_part
where  Year==2013 and Month=7 and DayOfMonth=7 and DepDelay >= 15
group by OriginAirportName
order by origin desc

-- COMMAND ----------

select count(*) as total_delays, OriginAirportName as origin 
from flight_delay_delta
where  Year==2013 and (Month=6 or Month=7) and DepDelay >= 15
group by OriginAirportName
order by origin desc
