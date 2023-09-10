-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This notebook demonstrates advanced Spark SQL capabilities

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
-- MAGIC ## Manipulate Json 
-- MAGIC

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

-- MAGIC %md
-- MAGIC
-- MAGIC #####NOTE: Spark SQL has built-in functionality to directly interact with nested data stored as JSON strings or struct types.
-- MAGIC
-- MAGIC Use **`:`** syntax in queries to access subfields in JSON strings
-- MAGIC
-- MAGIC Use **`.`** syntax in queries to access subfields in struct types
-- MAGIC

-- COMMAND ----------

-- for string type (not structured) we can use : notation to traverse json
select value:device, value:event_name, value:geo:city from raw_kafka_data_string_view

-- COMMAND ----------

-- you can parse json into (typed) struct objects (native Spark objects) but for this you need json schema
-- using schema_of_json and from_json built-in Spark functions
select value from raw_kafka_data_string_view where value:event_name != "" limit 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **schema_of_json()** returns the schema derived from an example JSON string.
-- MAGIC **from_json()** parses a column containing a JSON string into a struct type using the specified schema.

-- COMMAND ----------

select schema_of_json('{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}')

-- COMMAND ----------

create or replace temp view typed_kafka_events as
select from_json(value,schema_of_json('{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}')) as event
from raw_kafka_data_string_view

-- COMMAND ----------

select * from typed_kafka_events limit 10

-- COMMAND ----------

DESCRIBE EXTENDED typed_kafka_events

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
-- MAGIC ## Generated Columns
-- MAGIC
-- MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table. 
-- MAGIC

-- COMMAND ----------

create or replace table kafka_events_flat_time_formatted(
 user_id string,
 device string,
 event_name string,
 traffic_source string,
 event_timestamp bigint,
 event_date date GENERATED ALWAYS as (
    cast(cast(event_timestamp/1e6 as timestamp) as date)))


-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

merge into kafka_events_flat_time_formatted a
using kafka_events_flat b
on a.user_id = b.user_id and a.event_timestamp = b.event_timestamp
when not matched then
  insert *


-- COMMAND ----------

select * from kafka_events_flat_time_formatted limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Table Constraints
-- MAGIC
-- MAGIC
-- MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
-- MAGIC
-- MAGIC Databricks currently support two types of constraints:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
-- MAGIC
-- MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
-- MAGIC

-- COMMAND ----------

ALTER TABLE kafka_events_flat_time_formatted ADD CONSTRAINT date_validation CHECK (event_date >= '2020-01-01');

-- COMMAND ----------

ALTER TABLE kafka_events_flat_time_formatted ALTER COLUMN user_id SET NOT NULL;

-- COMMAND ----------

describe extended kafka_events_flat_time_formatted

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manipulate Arrays
-- MAGIC
-- MAGIC Spark SQL has a number of functions for manipulating array data, including the following:
-- MAGIC - **`explode()`** separates the elements of an array into multiple rows; this creates a new row for each element in an array.
-- MAGIC - **`size()`** provides a count for the number of elements in an array for each row. 
-- MAGIC
-- MAGIC The code below explodes the **`items`** field (an array of structs) into multiple rows and shows events containing arrays with 3 or more items.
-- MAGIC

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
-- MAGIC ##### Additonal array functions
-- MAGIC `array_distinct` removes duplicate elements from an array
-- MAGIC
-- MAGIC `flatten` combines multiple arrays into a single array
-- MAGIC
-- MAGIC `collect_set` create a set of unique values for a field including fields within arrays
-- MAGIC

-- COMMAND ----------

select * from kafka_events_flat limit 10

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

-- MAGIC %md
-- MAGIC
-- MAGIC ## Join Tables
-- MAGIC
-- MAGIC Spark SQL supports standard **`JOIN`** operations (inner, outer, left, right, anti, cross, semi).  
-- MAGIC Here we join the exploded events dataset with a lookup table to grab the standard printed item name.

-- COMMAND ----------

create or replace temp view item_lookup_price_view
using delta
options
(inferSchema = "True", header = "True", path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/kafka/item_lookup")

-- COMMAND ----------

select * from item_lookup_price_view

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

select * from sales_enriched_view limit 10

-- COMMAND ----------

create or replace table sales_enriched 
as select * from sales_enriched_view

-- COMMAND ----------

describe extended sales_enriched

-- COMMAND ----------

select * from sales_enriched limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Set Operators

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Spark supports set operations:
-- MAGIC
-- MAGIC `union`
-- MAGIC
-- MAGIC `minus`
-- MAGIC
-- MAGIC `intersect`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Pivot Tables
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
-- MAGIC ##High Order Functions
-- MAGIC
-- MAGIC `Filter`
-- MAGIC
-- MAGIC `Exists`
-- MAGIC
-- MAGIC `Transform`
-- MAGIC

-- COMMAND ----------

select * from kafka_events_flat limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####filter
-- MAGIC

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

-- MAGIC %md
-- MAGIC
-- MAGIC #### transform
-- MAGIC

-- COMMAND ----------

select
  user_id,
  event_name,
  standard_items,
  transform (
    standard_items,
    i ->  (i.item_revenue_in_usd * 3.4)
  ) as nis_price
from
  standard_iems_events_veiew

-- COMMAND ----------


