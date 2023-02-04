-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Exercise. Build Bronze Data Lake layer.
-- MAGIC 
-- MAGIC Bronze usually contains the raw data, converted into Delta Lake format. This data is infrequently accessed (cold data). Good for archiving to reduce storage cost.
-- MAGIC 
-- MAGIC Create 3 extrenal delta tables: _flight_delay_bronze_, _airport_code_location_bronze_, _flight_with_weather_bronze_.
-- MAGIC 
-- MAGIC Use temp view with inferSchema = "true"to load original CSV files from the FlightsDelays folder and then use CTAS to create Delta Lake tables in ./bronze folder.
-- MAGIC 
-- MAGIC Every table should be located in the dedicated folder. For example table flight_delay_bronze will be in abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay folder

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
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

-- COMMAND ----------

use flights

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### FlightDelaysWithAirportCodes 

-- COMMAND ----------

create
or replace temp view flight_delay_bronze_view using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv"
)

-- COMMAND ----------

create table flight_delay_bronze
using delta options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay/'
)
as 
select * from flight_delay_bronze_view


-- COMMAND ----------

select * from flight_delay_bronze limit 10

-- COMMAND ----------

describe extended  flight_delay_bronze

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay"))
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### AirportCodeLocationLookup 

-- COMMAND ----------

create or replace temp view airport_code_location_bronze_view
using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv"
)

-- COMMAND ----------

create table airport_code_location_bronze
options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/AirportCodeLocation/'
)
as 
select * from airport_code_location_bronze_view

-- COMMAND ----------

select * from airport_code_location_bronze limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### FlightWeatherWithAirportCode

-- COMMAND ----------

create or replace temp view flight_with_weather_bronze_view
using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
)

-- COMMAND ----------

create table flight_with_weather_bronze 
options(
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightWithWeather/"
)
as select * from flight_with_weather_bronze_view

-- COMMAND ----------

select * from flight_with_weather_bronze limit 10

-- COMMAND ----------

show tables 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create partitioned table

-- COMMAND ----------

create table flight_delay_bronze_part
using delta 
options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay_part/')
partitioned by (Year,Month,DayofMonth)
as 
select * from flight_delay_bronze_view

-- COMMAND ----------

describe extended flight_delay_bronze_part


-- COMMAND ----------


