-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### This notebook demonstrates advanced Spark SQL capabilities

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

-- MAGIC %md
-- MAGIC Prepare json data

-- COMMAND ----------

create or replace temp view raw_kafka_data_view
using delta
options
(inferSchema = "True", header = "True", path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka/")


-- COMMAND ----------

describe raw_kafka_data_view

-- COMMAND ----------

create or replace temp view raw_kafka_data_string_view 
as select string(key), string(value)
from raw_kafka_data_view 

-- COMMAND ----------

select * from raw_kafka_data_string_view

-- COMMAND ----------

select value:device from raw_kafka_data_string_view

-- COMMAND ----------

select value:geo:city from raw_kafka_data_string_view

-- COMMAND ----------

-- you can parse json into (typed) struct objects but for this you need json schema

-- COMMAND ----------

-- using schema_of_json and from_json Spark functions

create or replace temp view parsed_kafka_events as
select from_json(value,schema_of_json('{"device":"Android","ecommerce":{},"event_name":"add_item","event_previous_timestamp":1593880753875794,"event_timestamp":1593880826675403,"geo":{"city":"Chicago","state":"IL"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"facebook","user_first_touch_timestamp":1593877772975990,"user_id":"UA000000107369427"}')) as json
from raw_kafka_data_string_view


-- COMMAND ----------

select * from parsed_kafka_events limit 10

-- COMMAND ----------

describe parsed_kafka_events

-- COMMAND ----------

-- json unpacking. From struct you can flat (unpack) the json
create or replace temp view kafka_events_flat as
select json.*
from parsed_kafka_events

-- COMMAND ----------

select * from kafka_events_flat

-- COMMAND ----------

-- continue to unpack
select device,event_name, geo.* from kafka_events_flat

-- COMMAND ----------

-- dot notation to access nested fields
select * from kafka_events_flat where geo.city='Fargo'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Arrays

-- COMMAND ----------

select max(size(items)) as item_size, device  
from kafka_events_flat 
group by device 


-- COMMAND ----------

select device, geo.city, items from  kafka_events_flat
where size(items) > 1

-- COMMAND ----------

--explode arrays
select device, geo.city, explode(items) from  kafka_events_flat
where size(items) > 1


-- COMMAND ----------

create
or replace temp view transaction_flat as
select
  device,
  event_name,
  city,
  state,
  item.*
from
  (
    select
      device,
      event_name,
      geo.*,
      explode(items) as item
    from
      kafka_events_flat
  )

-- COMMAND ----------

select * from transaction_flat 

-- COMMAND ----------


