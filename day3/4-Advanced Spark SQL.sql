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
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-03-25T10:44:33Z&se=2023-05-31T18:44:33Z&spr=https&sv=2021-12-02&sr=c&sig=0cKmo1mfJvwXXAaViczNXGSXI60BMsPM5urqr9WBSNQ%3D")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Prepare json data

-- COMMAND ----------

create or replace temp view raw_kafka_data_view
using delta
options
(inferSchema = "True", header = "True", path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka/")


-- COMMAND ----------

select * from raw_kafka_data_view limit 10

-- COMMAND ----------

describe extended raw_kafka_data_view

-- COMMAND ----------

--Cast key and value to string
create or replace temp view raw_kafka_data_string_view 
as select string(key), string(value)
from raw_kafka_data_view 

-- COMMAND ----------

select * from raw_kafka_data_string_view limit 10

-- COMMAND ----------

-- for string type (not structured) we can use : notation to traverse json
select value:device, value:event_name, value:geo:city from raw_kafka_data_string_view

-- COMMAND ----------

-- you can parse json into (typed) struct objects (native Spark objects) but for this you need json schema
-- using schema_of_json and from_json built-in Spark functions
select value from raw_kafka_data_string_view where value:event_name != "" limit 1

-- COMMAND ----------

create or replace temp view typed_kafka_events as
select from_json(value,schema_of_json('{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}')) as event
from raw_kafka_data_string_view

-- COMMAND ----------

select * from typed_kafka_events limit 10

-- COMMAND ----------

describe typed_kafka_events

-- COMMAND ----------

-- json unpacking. From struct type you can flat (unpack) the json with *
create or replace temp view kafka_events_flat as
select event.*
from typed_kafka_events

-- COMMAND ----------

select * from kafka_events_flat

-- COMMAND ----------

-- continue to unpack with *
select device,event_name, geo.* from kafka_events_flat

-- COMMAND ----------

-- dot notation to access nested fields
select * from kafka_events_flat where geo.city='Fargo'

-- COMMAND ----------

select items.item_revenue_in_usd
from kafka_events_flat
where items.item_revenue_in_usd is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Arrays

-- COMMAND ----------

-- size function
select max(size(items)) as item_size, device  
from kafka_events_flat 
group by device 


-- COMMAND ----------

select device, geo.city, items from  kafka_events_flat
where size(items) > 1

-- COMMAND ----------

--explode arrays. It will put each element in an array in its own row
select device, geo.city, explode(items) from  kafka_events_flat
where size(items) > 1


-- COMMAND ----------

-- let's explode and unpack json to the entirely flat table
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

select * from transaction_flat limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Additonal array functions
-- MAGIC `array_distinct` removes duplicate elements from array
-- MAGIC 
-- MAGIC `flatten` combines multiple arrays into single array
-- MAGIC 
-- MAGIC `collect_set` create a set of unique values for a field. Aggregative function.

-- COMMAND ----------

select user_id,
collect_set(event_name) as event_collection
from kafka_events_flat
group by user_id

-- COMMAND ----------

select  collect_set(event_name) as items_list
from kafka_events_flat

-- COMMAND ----------

select  array_distinct(flatten(collect_set(items))) as items_list
from kafka_events_flat

-- COMMAND ----------

select user_id,
collect_set(event_name) as event_collection,
array_distinct(flatten(collect_set(items.item_id))) as cart_collection
from kafka_events_flat
group by user_id



-- COMMAND ----------

create or replace temp view item_lookup_price_view
using delta
options
(inferSchema = "True", header = "True", path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka/item_lookup")

-- COMMAND ----------

select * from item_lookup_price_view

-- COMMAND ----------

create or replace view sales_enriched as 
select * from (
select * from transaction_flat 
)

-- COMMAND ----------

create
or replace temp view sales_enriched_view as
select
  *
from
  (
    select
      device,
      event_name,
      city,
      state,
      item_name,
      item_revenue_in_usd,
      price_in_usd,
      quantity,
      price,
      a.item_id
    from
      transaction_flat a
      inner join item_lookup_price_view b on a.item_id = b.item_id
  )

-- COMMAND ----------

drop table sales_enriched;

create table sales_enriched 
as select * from sales_enriched_view

-- COMMAND ----------

select * from sales_enriched limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Set Operators

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Spark supports 
-- MAGIC 
-- MAGIC `union`
-- MAGIC 
-- MAGIC `minus`
-- MAGIC 
-- MAGIC `intersect`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Pivot Tables
-- MAGIC 
-- MAGIC Turns specific column values into columns.

-- COMMAND ----------

select * from (
select device,
      event_name,
      city,
      state,
      item_name,
      item_revenue_in_usd,
      price_in_usd,
      quantity,
      price,
      item_id
      from sales_enriched)
      pivot ( sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  ));
      

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####High Order Functions
-- MAGIC 
-- MAGIC `Filter`
-- MAGIC 
-- MAGIC `Exists`
-- MAGIC 
-- MAGIC `Transform`

-- COMMAND ----------

select * from kafka_events_flat limit 10

-- COMMAND ----------

-- first paramters must be array, i is an iterator 

create or replace temp view standard_iems_events_veiew as
select user_id, event_name, standard_items from(
  select user_id, event_name, items,
  filter (items, i -> i.item_id like '%STAN%') as standard_items
  from kafka_events_flat
  )
where size (standard_items) > 0

-- COMMAND ----------

select * from standard_iems_events_veiew limit 10

-- COMMAND ----------

select
  user_id,
  event_name,
  standard_items,
  transform (
    standard_items,
    i -> cast (i.item_revenue_in_usd * 3.4 as double)
  ) as nis_price
from
  standard_iems_events_veiew

-- COMMAND ----------


